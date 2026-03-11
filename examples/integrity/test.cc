#include <iostream>
#include <vector>
#include <thread>
#include <mutex>
#include <random>
#include <chrono>
#include <fcntl.h>
#include <unistd.h>
#include <cassert>

extern "C" {
    #include <nvm_types.h>
    #include <nvm_ctrl.h>
    #include <nvm_dma.h>
    #include <nvm_aq.h>
    #include <nvm_error.h>
    
    // 你同目录及项目里的自定义头文件
    #include "get-offset/get-offset.h"
    #include "integrity.h"
    #include "read.h"
    #include "../../src/file.h"
    
    // 声明外部函数，确保链接正确
    int init_userioq(nvm_ctrl_t* ctrl, struct disk* d);
    int read_and_dump(const struct disk* disk, struct queue_pair* qp, const nvm_dma_t* buffer, const struct file_info* args);
    int create_buffer(struct buffer* b, nvm_ctrl_t* ctrl, size_t size, int is_cq, int ioq_idx);
}

#define snvme_control_path "/dev/snvm_control"
#define snvme_path "/dev/csnvme1"
#define nvme_dev_path "/dev/snvme0n1"
#define snvme_helper_path "/dev/snvme_helper"
#define nvme_mount_path "/mnt/nvm_mount"
#define file_name "/mnt/nvm_mount/test.data"
#define nvme_pci_addr {0xc3, 0, 0}

const int TOTAL_QUEUES = 16;
const int HOT_QUEUES = 4; // 0~3 为热数据 VIP 队列，4~15 为冷数据队列


std::mutex queue_mutexes[TOTAL_QUEUES];

struct IO_Task {
    bool is_hot;
    uint64_t offset;
    size_t size; 
};

void io_worker_thread(int thread_id, const std::vector<IO_Task>& tasks, nvm_ctrl_t* ctrl, const struct disk* d, const nvm_dma_t* buffer_dma) {
    thread_local std::mt19937 rng(std::random_device{}() + thread_id);

    for (const auto& task : tasks) {
        int target_q_idx;

        // QoS 冷热路由
        if (task.is_hot) {
            std::uniform_int_distribution<int> hot_dist(0, HOT_QUEUES - 1);
            target_q_idx = hot_dist(rng);
        } else {
            std::uniform_int_distribution<int> cold_dist(HOT_QUEUES, TOTAL_QUEUES - 1);
            target_q_idx = cold_dist(rng);
        }

        struct queue_pair qp;
        qp.cq = &ctrl->queues[target_q_idx]; 
        qp.sq = &ctrl->queues[ctrl->cq_num + target_q_idx]; // 注意偏移
        qp.stop = false;
        qp.num_cpls = 0;

        struct file_info read_info;
        read_info.offset = task.offset >> 9; // 字节 -> Block
        read_info.num_blocks = task.size >> 9;

        // 细粒度加锁并下发 NVMe 读请求
        {
            std::lock_guard<std::mutex> lock(queue_mutexes[target_q_idx]);
            
            // 真实触发硬件直通读取！
            read_and_dump(d, &qp, buffer_dma, &read_info);
            
            // 测试阶段可打印，跑极致性能数据时需注释掉
            // std::cout << "Thread " << thread_id << (task.is_hot ? " [HOT]" : " [COLD]") 
            //           << " -> Queue " << target_q_idx << std::endl;
        }
    }
}

int main(int argc, char** argv) {
    nvm_ctrl_t* ctrl;                   
    int status, ret;                    
    struct disk disk;                   
    struct buffer buffer;               
    int snvme_c_fd, snvme_d_fd;         
    
    int read_bytes = 1024 * 64;         
    void *buf__host = nullptr;            
    int *buf__host_int = nullptr;         
    int fd;                             
    int snvme_helper_fd;                
    struct nds_mapping mapping;         
    uint64_t nvme_ofst;                 

    // 1. 初始化硬件控制器 (复用你原本的代码)
    snvme_c_fd = open(snvme_control_path, O_RDWR); 
    if (snvme_c_fd < 0) {
        perror("Failed to open control device file");
        exit(1);
    }
    
    // Get controller reference
    //创建设备文件，绑定PCI设备地址到控制器设备文件，以便后续操作能够正确识别和访问NVMe设备
    ret = ioctl_set_cdev(snvme_c_fd, nvme_pci_addr, 1);
    if (ret < 0)
    {
        fprintf(stderr, "Failed to create device file: %s\n", strerror(errno));
        exit(1);
    }
    //打开设备文件，获取文件描述符
    snvme_d_fd = open(snvme_path, O_RDWR);
    if (snvme_d_fd < 0)
    {
        fprintf(stderr, "Failed to open device file: %s\n", strerror(errno));
        exit(1);
    }
    //初始化SNVMe
    status = nvm_ctrl_init(&ctrl, snvme_c_fd, snvme_d_fd);
    //在结构体里记录这块硬盘的 PCIe 地址
    ctrl->device_addr = nvme_pci_addr;
    if (status != 0)
    {
        close(snvme_c_fd);
        close(snvme_d_fd);
        
        fprintf(stderr, "Failed to get controller reference: %s\n", strerror(status));
    }
    
    close(snvme_c_fd);
    close(snvme_d_fd);

    ctrl->cq_num = 16;
    ctrl->sq_num = 16;
    ctrl->qs = 1024;
    // Create queues
    status = request_queues(ctrl, &ctrl->queues);
    if (status != 0)
    {
        goto out;
    }
    //这块硬盘的I/O现在属于用户态
    status =  ioctl_use_userioq(ctrl,1);
    if (status != 0)
    {
        goto out;
    }
    /*Prepare Buffer for read/write, need convert vaddt to io addr*/

    //申请DMA内存，用来接收直通读取的数据
    status = create_buffer(&buffer, ctrl, 4096,0,-1);
    if (status != 0)
    {
        goto out;
    }
    //重绑设备
    status = ioctl_rebind_nvme(ctrl, nvme_pci_addr, 1);
    if (status != 0)
    {
        goto out;
    }

    disk.ns_id = 1;                                 //命名空间ID，表示要操作的NVMe命名空间，通常为1
    disk.page_size = ctrl->page_size;               //页面大小，表示NVMe设备的最小数据传输单位，通常为4KB
    printf("page size is %lu\n",disk.page_size);
    sleep(5);

    // 初始化用户态队列的底层上下文状态。
    status =  init_userioq(ctrl,&disk);
    if (status != 0)
    {
        goto out;
    }
    printf("disk block size is %u, max data size is %u\n",disk.block_size,disk.max_data_size);
    //将文件系统挂载到指定路径，并创建一个测试文件，准备进行读写操作
    Host_file_system_int(nvme_dev_path,nvme_mount_path);
    //创建测试文件，并写入数据
    fd = open(file_name, O_RDWR| O_CREAT | O_DIRECT , S_IRUSR | S_IWUSR);
    if (fd < 0) {
        perror("Failed to open file");
        goto out;
    }
    //分配页对齐的内存，并且填充数据
    ret = posix_memalign(&buf__host, 4096, read_bytes);
    assert(ret==0);
    assert(0 == ftruncate(fd, read_bytes*16)); //预分配文件空间，避免写入时触发文件系统的动态分配机制，确保后续的读写操作能够直接映射到预分配的磁盘空间上，提高性能和稳定性。
    
    //把申请的内存当作整数数组来使用，填充测试数据
    buf__host_int = (int*)buf__host;
    for (size_t i = 0; i < read_bytes / sizeof(int); i++)
        buf__host_int[i] = i; //填充0，1，2，···等测试数据
    snvme_helper_fd = open(snvme_helper_path, O_RDWR);//打开用来查看物理地址的后门
    if (snvme_helper_fd < 0) {
        perror("Failed to open snvme_helper_fd");
        assert(0);
    }
    //写入
    assert(read_bytes == pwrite(fd, buf__host_int, read_bytes,read_bytes));
    //落盘
    fsync(fd);   
    
    mapping.file_fd = fd;               //查找文件的信息
    mapping.offset = read_bytes;        //offset
    mapping.len = read_bytes;           //查找长度
    // 发送极其特殊的 IOCTL 秘钥，强迫文件系统交出这部分文件对应的【硬盘真实物理寻址坐标】
    if (ioctl(snvme_helper_fd, SNVME_HELP_GET_NVME_OFFSET, &mapping) < 0) {
        perror("ioctl failed");
        assert(0);
    }
    // 从返回的结果中提取出 NVMe 偏移地址，这个地址就是文件在 NVMe 设备上的物理位置，可以用来直接访问文件数据
    nvme_ofst = mapping.address;
    
    std::cout << ">>> Initialized Hardware. NVMe Offset: 0x" << std::hex << nvme_ofst << std::dec << std::endl;

    // 3. 构建多线程并发混合负载
    int num_threads = 8;
    int tasks_per_thread = 20; 
    std::vector<std::thread> workers;

    std::cout << ">>> Launching " << num_threads << " threads for QoS routing test..." << std::endl;

    for (int i = 0; i < num_threads; ++i) {
        std::vector<IO_Task> thread_tasks;
        for (int j = 0; j < tasks_per_thread; ++j) {
            IO_Task t;
            t.is_hot = (rand() % 100) < 20; // 20% 热数据
            
            if (t.is_hot) {
                t.size = 4096;          
                t.offset = nvme_ofst;   
            } else {
                t.size = 64 * 1024; // 适度调小冷请求，避免测试时卡太久
                t.offset = nvme_ofst + 4096 + (rand() % 100) * 4096; 
            }
            thread_tasks.push_back(t);
        }
        
        // 拉起线程 (注意这里 buffer.dma 传入)
        workers.emplace_back(io_worker_thread, i, thread_tasks, ctrl, &disk, buffer.dma);
    }

    // 4. 等待收割
    for (auto& t : workers) {
        if (t.joinable()) {
            t.join();
        }
    }

    std::cout << ">>> All I/O tasks completed safely." << std::endl;
    
    // Host_file_system_exit(nvme_dev_path);
    // nvm_ctrl_free(ctrl);
    return 0;
}