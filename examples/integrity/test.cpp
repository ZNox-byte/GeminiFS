#include <nvm_types.h>
#include <nvm_ctrl.h>
#include <nvm_dma.h>
#include <nvm_aq.h>
#include <nvm_error.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <thread>
#include <vector>
#include <chrono>
#include <algorithm>
#include <atomic>

#include "get-offset/get-offset.h"
#include "integrity.h"
#include "read.h"
#include "../../src/file.h"

#define snvme_control_path "/dev/snvm_control"
#define snvme_path "/dev/csnvme1"
#define nvme_dev_path "/dev/nvme0n1" 
#define snvme_helper_path "/dev/snvme_helper"
#define nvme_mount_path "/mnt/nvm_mount"
#define file_name "/mnt/nvm_mount/test.data"
#define nvme_pci_addr {0xc3, 0, 0}

std::atomic<bool> keep_running(true);


static void remove_queues(struct queue* queues, uint16_t n_queues)
{
    uint16_t i;

    if (queues != NULL)
    {

        for (i = 0; i < n_queues; i++)
        {
            remove_queue(&queues[i]);
        }

        free(queues);
    }
}



static int request_queues(nvm_ctrl_t* ctrl, struct queue** queues)
{
    struct queue* q;
    *queues = NULL;
    uint16_t i;
    int status;
    status = ioctl_set_qnum(ctrl, ctrl->cq_num+ctrl->sq_num);
    if (status != 0)
    {
    
        return status;
    }
    // Allocate queue descriptors
    q = (queue *)calloc(ctrl->cq_num+ctrl->sq_num, sizeof(struct queue));
    if (q == NULL)
    {
        fprintf(stderr, "Failed to allocate queues: %s\n", strerror(errno));
        return ENOMEM;
    }

    // Create completion queue
    for (i = 0; i < ctrl->cq_num; ++i)
    {
        status = create_queue(&q[i], ctrl, NULL, i);
        if (status != 0)
        {
            free(q);
            return status;
        }
    }


    // Create submission queues
    for (i = 0; i < ctrl->sq_num; ++i)
    {
        status = create_queue(&q[i + ctrl->cq_num], ctrl, &q[0], i);
        if (status != 0)
        {
            remove_queues(q, i);
            return status;
        }
    }
    printf("request_queues success\n");
    *queues = q;
    return status;
}

void hot_data(struct disk* disk, struct queue_pair* qp, nvm_dma_t* dma_buffer, struct file_info* info, std::vector<double>& latencies){
    printf("[Hot Thread] Started on CQ: %u, SQ: %u. IO size: 4KB\n", qp->cq->queue.no, qp->sq->queue.no);

    info->num_blocks = 4096 >> 9;

    while(keep_running)
    {
        auto start_time = std::chrono::high_resolution_clock::now();

        int status = pure_read(disk, qp, dma_buffer, info);

        auto end_time = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double, std::micro> elapsed = end_time - start_time;

        if(status == 0)
        {
            latencies.push_back(elapsed.count());
        }

        std::this_thread::sleep_for(std::chrono::microseconds(50));

    }
    printf("[Hot Thread] Finished.\n");
}

void cold_data(struct disk* disk, struct queue_pair* qp, nvm_dma_t* dma_buffer, struct file_info* info){
    printf("[Cold Thread] Started on CQ: %u, SQ: %u. IO size: 64KB\n", qp->cq->queue.no, qp->sq->queue.no);
    info->num_blocks = (1024 * 64) >> 9;
    while(keep_running)
    {
        pure_read(disk, qp, dma_buffer, info);
    }
    printf("[Cold Thread] Finished.\n");
}

int main()
{
    nvm_ctrl_t* ctrl;
    struct disk disk;
    struct buffer hot_buffer, cold_buffer;
    int snvme_c_fd, snvme_d_fd, snvme_helper_fd, fd;
    struct nds_mapping mapping;
    uint64_t nvme_ofst;
    int ret, status;
    
    // 1. 初始化控制节点与分配队列 (沿用你跑通的逻辑)
    snvme_c_fd = open(snvme_control_path, O_RDWR); 
    ioctl_set_cdev(snvme_c_fd, nvme_pci_addr, 1);
    snvme_d_fd = open(snvme_path, O_RDWR);
    
    nvm_ctrl_init(&ctrl, snvme_c_fd, snvme_d_fd);
    ctrl->device_addr = nvme_pci_addr;
    close(snvme_c_fd);
    close(snvme_d_fd);

    ctrl->cq_num = 16;
    ctrl->sq_num = 16;
    ctrl->qs = 1024;
    
    request_queues(ctrl, &ctrl->queues);
    ioctl_use_userioq(ctrl, 1);
    
    // 为热数据和冷数据分别申请独立的 DMA 接收内存
    create_buffer(&hot_buffer, ctrl, 4096, 0, -1);
    create_buffer(&cold_buffer, ctrl, 1024 * 64, 0, -1);

    // 重绑设备，接管控制权
    ioctl_rebind_nvme(ctrl, nvme_pci_addr, 1);

    disk.ns_id = 1;
    disk.page_size = ctrl->page_size;
    init_userioq(ctrl, &disk);

    // 2. 挂载 EXT4，建立测试文件，获取物理偏移
    Host_file_system_int(nvme_dev_path, nvme_mount_path);
    fd = open(file_name, O_RDWR | O_CREAT | O_DIRECT, S_IRUSR | S_IWUSR);
    ftruncate(fd, 1024 * 1024 * 10); // 预分配 10MB
    fsync(fd);

    mapping.file_fd = fd;
    mapping.offset = 4096;
    mapping.len = 4096;
    
    snvme_helper_fd = open(snvme_helper_path, O_RDWR);
    ioctl(snvme_helper_fd, SNVME_HELP_GET_NVME_OFFSET, &mapping);
    nvme_ofst = mapping.address;
    close(snvme_helper_fd);

    printf("Physical NVMe Offset: %lx\n", nvme_ofst);
    struct queue_pair hot_qp, cold_qp;

    hot_qp.cq = &ctrl->queues[0];
    hot_qp.sq = &ctrl->queues[ctrl->cq_num + 0];
    hot_qp.stop = false;
    hot_qp.num_cpls = 0;

    struct file_info hot_info, cold_info;
    hot_info.offset = nvme_ofst >> 9;
    cold_info.offset = (nvme_ofst + 1024 * 128) >> 9;

    std::vector<double> hot_latencies;
    hot_latencies.reserve(100000);

    printf("\n--- Starting Cold-Hot Isolation Test (Duration: 5 seconds) ---\n");

    std::thread t_cold(cold_data, &disk, &cold_qp, cold_buffer.dma, &cold_info);

    std::thread t_hot(hot_data, &disk, &hot_qp, hot_buffer.dma, &hot_info, std::ref(hot_latencies));

    std::this_thread::sleep_for(std::chrono::seconds(5));
    keep_running = false;

    t_hot.join();
    t_cold.join();

    if(!hot_latencies.empty())
    {
        std::sort(hot_latencies.begin(), hot_latencies.end());
        double avg = 0;
        for(double l : hot_latencies) avg += l;
        avg /= hot_latencies.size();

        double p95 = hot_latencies[hot_latencies.size() * 0.95];
        double p99 = hot_latencies[hot_latencies.size() * 0.99];

        printf("\n--- Hot Data Latency Report ---\n");
        printf("Total Hot Requests: %zu\n", hot_latencies.size());
        printf("Average Latency: %.2f us\n", avg);
        printf("P95 Tail Latency: %.2f us\n", p95);
        printf("P99 Tail Latency: %.2f us\n", p99);
    }

    close(fd);
    Host_file_system_exit(nvme_dev_path);
    nvm_ctrl_free(ctrl);
}