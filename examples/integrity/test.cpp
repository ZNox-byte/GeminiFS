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
#include <atomic>

#include "get-offset/get-offset.h"
#include "integrity.h"
#include "read.h"
#include "../../src/file.h"

#define snvme_control_path "/dev/snvm_control"
#define snvme_path "/dev/csnvme1"
#define nvme_dev_path "/dev/snvme0n1"
#define snvme_helper_path "/dev/snvme_helper"
#define nvme_mount_path "/mnt/nvm_mount"
#define file_name "/mnt/nvm_mount/test.data"
#define nvme_pci_addr {0xc3, 0, 0}

std::atomic<bool> keep_running(true);

static int map_file_offset(int helper_fd, int file_fd, uint64_t file_offset, uint64_t length, uint64_t* nvme_offset)
{
    struct nds_mapping mapping = {};

    mapping.file_fd = file_fd;
    mapping.offset = file_offset;
    mapping.len = length;

    if (ioctl(helper_fd, SNVME_HELP_GET_NVME_OFFSET, &mapping) < 0)
    {
        return errno;
    }

    *nvme_offset = mapping.address;
    return 0;
}


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
        status = create_queue(&q[i + ctrl->cq_num], ctrl, &q[i], i);
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

struct latency_log
{
    double* values;
    size_t count;
    size_t capacity;
};

struct hot_thread_args
{
    struct disk* disk;
    struct queue_pair* qp;
    nvm_dma_t* dma_buffer;
    struct file_info* info;
    struct latency_log* latencies;
};

struct cold_thread_args
{
    struct disk* disk;
    struct queue_pair* qp;
    nvm_dma_t* dma_buffer;
    struct file_info* info;
};

static int latency_log_init(struct latency_log* log, size_t capacity)
{
    log->values = (double*)calloc(capacity, sizeof(double));
    if (log->values == NULL)
    {
        log->count = 0;
        log->capacity = 0;
        return ENOMEM;
    }

    log->count = 0;
    log->capacity = capacity;
    return 0;
}

static void latency_log_destroy(struct latency_log* log)
{
    free(log->values);
    log->values = NULL;
    log->count = 0;
    log->capacity = 0;
}

static void latency_log_push(struct latency_log* log, double latency)
{
    if (log->count < log->capacity)
    {
        log->values[log->count++] = latency;
    }
}

static int compare_double(const void* lhs, const void* rhs)
{
    double left = *(const double*)lhs;
    double right = *(const double*)rhs;

    if (left < right)
    {
        return -1;
    }
    if (left > right)
    {
        return 1;
    }
    return 0;
}

static uint64_t diff_us(const struct timespec* start, const struct timespec* end)
{
    return (uint64_t)(end->tv_sec - start->tv_sec) * 1000000ULL
        + (uint64_t)(end->tv_nsec - start->tv_nsec) / 1000ULL;
}

static double percentile_value(const struct latency_log* log, double percentile)
{
    size_t index;

    if (log->count == 0)
    {
        return 0.0;
    }

    index = (size_t)(log->count * percentile);
    if (index >= log->count)
    {
        index = log->count - 1;
    }

    return log->values[index];
}

static void* hot_data(void* arg){
    struct hot_thread_args* thread_args = (struct hot_thread_args*)arg;
    struct disk* disk = thread_args->disk;
    struct queue_pair* qp = thread_args->qp;
    nvm_dma_t* dma_buffer = thread_args->dma_buffer;
    struct file_info* info = thread_args->info;
    struct latency_log* latencies = thread_args->latencies;
    printf("[Hot Thread] Started on CQ: %u, SQ: %u. IO size: 4KB\n", qp->cq->queue.no, qp->sq->queue.no);

    info->num_blocks = 4096 >> 9;

    while(keep_running)
    {
        struct timespec start_time;
        struct timespec end_time;
        double elapsed_us;

        clock_gettime(CLOCK_MONOTONIC, &start_time);
        int status = pure_read(disk, qp, dma_buffer, info);
        clock_gettime(CLOCK_MONOTONIC, &end_time);
        elapsed_us = (double)diff_us(&start_time, &end_time);

        if(status == 0)
        {
            latency_log_push(latencies, elapsed_us);
        }
        else if (status != ECANCELED)
        {
            fprintf(stderr, "[Hot Thread] pure_read failed: %s\n", strerror(status));
            break;
        }

        usleep(50);

    }
    printf("[Hot Thread] Finished.\n");
    return NULL;
}

static void* cold_data(void* arg){
    struct cold_thread_args* thread_args = (struct cold_thread_args*)arg;
    struct disk* disk = thread_args->disk;
    struct queue_pair* qp = thread_args->qp;
    nvm_dma_t* dma_buffer = thread_args->dma_buffer;
    struct file_info* info = thread_args->info;
    printf("[Cold Thread] Started on CQ: %u, SQ: %u. IO size: 64KB\n", qp->cq->queue.no, qp->sq->queue.no);
    info->num_blocks = (1024 * 64) >> 9;
    while(keep_running)
    {
        int status = pure_read(disk, qp, dma_buffer, info);
        if (status != 0 && status != ECANCELED)
        {
            fprintf(stderr, "[Cold Thread] pure_read failed: %s\n", strerror(status));
            break;
        }
    }
    printf("[Cold Thread] Finished.\n");
    return NULL;
}

int main()
{
    nvm_ctrl_t* ctrl = NULL;
    struct disk disk;
    struct buffer hot_buffer = {};
    struct buffer cold_buffer = {};
    int snvme_c_fd = -1, snvme_d_fd = -1, snvme_helper_fd = -1, fd = -1;
    uint64_t hot_nvme_ofst, cold_nvme_ofst;
    int ret, status;
    bool mounted = false;
    bool hot_buffer_ready = false;
    bool cold_buffer_ready = false;
    char* dummy_buf = NULL;
    struct queue_pair hot_qp, cold_qp;
    struct file_info hot_info, cold_info;
    struct latency_log hot_latencies = {};
    struct hot_thread_args hot_args;
    struct cold_thread_args cold_args;
    pthread_t hot_thread = 0;
    pthread_t cold_thread = 0;
    bool hot_thread_started = false;
    bool cold_thread_started = false;
    double avg;
    double p95;
    double p99;
    size_t i;
    
    // 1. 初始化控制节点与分配队列 (沿用你跑通的逻辑)
    snvme_c_fd = open(snvme_control_path, O_RDWR); 
    if (snvme_c_fd < 0)
    {
        perror("Failed to open control device");
        return 1;
    }

    ret = ioctl_set_cdev(snvme_c_fd, nvme_pci_addr, 1);
    if (ret < 0)
    {
        perror("Failed to bind controller device");
        close(snvme_c_fd);
        return 1;
    }

    snvme_d_fd = open(snvme_path, O_RDWR);
    if (snvme_d_fd < 0)
    {
        perror("Failed to open data device");
        close(snvme_c_fd);
        return 1;
    }
    
    status = nvm_ctrl_init(&ctrl, snvme_c_fd, snvme_d_fd);
    if (status != 0)
    {
        fprintf(stderr, "Failed to initialize controller: %s\n", strerror(status));
        close(snvme_c_fd);
        close(snvme_d_fd);
        return 1;
    }

    ctrl->device_addr = nvme_pci_addr;
    close(snvme_c_fd);
    close(snvme_d_fd);
    snvme_c_fd = -1;
    snvme_d_fd = -1;

    ctrl->cq_num = 16;
    ctrl->sq_num = 16;
    ctrl->qs = 1024;
    
    status = request_queues(ctrl, &ctrl->queues);
    if (status != 0)
    {
        fprintf(stderr, "Failed to request queues: %s\n", strerror(status));
        goto out;
    }

    status = ioctl_use_userioq(ctrl, 1);
    if (status != 0)
    {
        fprintf(stderr, "Failed to enable user I/O queues: %s\n", strerror(status));
        goto out;
    }
    
    // 为热数据和冷数据分别申请独立的 DMA 接收内存
    status = create_buffer(&hot_buffer, ctrl, 4096, 0, -1);
    if (status != 0)
    {
        goto out;
    }
    hot_buffer_ready = true;

    status = create_buffer(&cold_buffer, ctrl, 1024 * 64, 0, -1);
    if (status != 0)
    {
        goto out;
    }
    cold_buffer_ready = true;

    // 重绑设备，接管控制权
    status = ioctl_rebind_nvme(ctrl, nvme_pci_addr, 1);
    if (status != 0)
    {
        fprintf(stderr, "Failed to rebind NVMe device: %s\n", strerror(status));
        goto out;
    }

    disk.ns_id = 1;
    disk.page_size = ctrl->page_size;
    
    sleep(5);
    
    status = init_userioq(ctrl, &disk);
    if (status != 0)
    {
        fprintf(stderr, "Failed to initialize user I/O queues: %s\n", strerror(status));
        goto out;
    }

    

    // 2. 挂载 EXT4，建立测试文件，获取物理偏移
    status = Host_file_system_int(nvme_dev_path, nvme_mount_path);
    if (status != 0)
    {
        fprintf(stderr, "Failed to mount host file system: %s\n", strerror(errno));
        goto out;
    }
    mounted = true;
    fd = open(file_name, O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
    if (fd < 0)
    {
        perror("Failed to open test file");
        goto out;
    }

    dummy_buf = (char*)calloc(1, 1024 * 1024 * 10);
    if (dummy_buf == NULL)
    {
        fprintf(stderr, "Failed to allocate test buffer\n");
        goto out;
    }

    ret = write(fd, dummy_buf, 1024 * 1024 * 10);
    free(dummy_buf);
    if (ret != 1024 * 1024 * 10)
    {
        perror("Failed to seed test file");
        goto out;
    }

    if (fsync(fd) != 0)
    {
        perror("fsync failed");
        goto out;
    }

    snvme_helper_fd = open(snvme_helper_path, O_RDWR);
    if (snvme_helper_fd < 0)
    {
        perror("Failed to open helper device");
        goto out;
    }

    status = map_file_offset(snvme_helper_fd, fd, 4096, 4096, &hot_nvme_ofst);
    if (status != 0)
    {
        fprintf(stderr, "Failed to map hot file offset: %s\n", strerror(status));
        goto out;
    }

    status = map_file_offset(snvme_helper_fd, fd, 1024 * 128, 1024 * 64, &cold_nvme_ofst);
    if (status != 0)
    {
        fprintf(stderr, "Failed to map cold file offset: %s\n", strerror(status));
        goto out;
    }

    close(snvme_helper_fd);
    snvme_helper_fd = -1;

    printf("Hot Physical NVMe Offset: %lx\n", hot_nvme_ofst);
    printf("Cold Physical NVMe Offset: %lx\n", cold_nvme_ofst);

    hot_qp.cq = &ctrl->queues[0];
    hot_qp.sq = &ctrl->queues[ctrl->cq_num + 0];
    hot_qp.stop = false;
    hot_qp.num_cpls = 0;

    cold_qp.cq = &ctrl->queues[1];
    cold_qp.sq = &ctrl->queues[ctrl->cq_num + 1];
    cold_qp.stop = false;
    cold_qp.num_cpls = 0;

    hot_info.offset = hot_nvme_ofst >> 9;
    hot_info.num_blocks = 4096 >> 9;
    cold_info.offset = cold_nvme_ofst >> 9;
    cold_info.num_blocks = (1024 * 64) >> 9;

    status = latency_log_init(&hot_latencies, 100000);
    if (status != 0)
    {
        fprintf(stderr, "Failed to allocate latency log: %s\n", strerror(status));
        goto out;
    }

    hot_args.disk = &disk;
    hot_args.qp = &hot_qp;
    hot_args.dma_buffer = hot_buffer.dma;
    hot_args.info = &hot_info;
    hot_args.latencies = &hot_latencies;

    cold_args.disk = &disk;
    cold_args.qp = &cold_qp;
    cold_args.dma_buffer = cold_buffer.dma;
    cold_args.info = &cold_info;

    printf("\n--- Starting Cold-Hot Isolation Test (Duration: 5 seconds) ---\n");

    status = pthread_create(&cold_thread, NULL, cold_data, &cold_args);
    if (status != 0)
    {
        fprintf(stderr, "Failed to start cold thread: %s\n", strerror(status));
        goto out;
    }
    cold_thread_started = true;

    status = pthread_create(&hot_thread, NULL, hot_data, &hot_args);
    if (status != 0)
    {
        fprintf(stderr, "Failed to start hot thread: %s\n", strerror(status));
        goto out;
    }
    hot_thread_started = true;

    sleep(5);
    keep_running = false;
    hot_qp.stop = true;
    cold_qp.stop = true;

    pthread_join(hot_thread, NULL);
    hot_thread_started = false;
    pthread_join(cold_thread, NULL);
    cold_thread_started = false;

    if(hot_latencies.count != 0)
    {
        qsort(hot_latencies.values, hot_latencies.count, sizeof(double), compare_double);
        avg = 0.0;
        for(i = 0; i < hot_latencies.count; ++i)
        {
            avg += hot_latencies.values[i];
        }
        avg /= hot_latencies.count;

        p95 = percentile_value(&hot_latencies, 0.95);
        p99 = percentile_value(&hot_latencies, 0.99);

        printf("\n--- Hot Data Latency Report ---\n");
        printf("Total Hot Requests: %zu\n", hot_latencies.count);
        printf("Average Latency: %.2f us\n", avg);
        printf("P95 Tail Latency: %.2f us\n", p95);
        printf("P99 Tail Latency: %.2f us\n", p99);
    }

    close(fd);
    fd = -1;
    Host_file_system_exit(nvme_mount_path);
    mounted = false;
    if (cold_buffer_ready)
    {
        remove_buffer(&cold_buffer);
    }
    if (hot_buffer_ready)
    {
        remove_buffer(&hot_buffer);
    }
    latency_log_destroy(&hot_latencies);
    nvm_ctrl_free(ctrl);
    return 0;

out:
    keep_running = false;
    if (hot_thread_started)
    {
        pthread_join(hot_thread, NULL);
    }
    if (cold_thread_started)
    {
        pthread_join(cold_thread, NULL);
    }
    if (snvme_d_fd >= 0)
    {
        close(snvme_d_fd);
    }
    if (snvme_c_fd >= 0)
    {
        close(snvme_c_fd);
    }
    if (snvme_helper_fd >= 0)
    {
        close(snvme_helper_fd);
    }
    if (fd >= 0)
    {
        close(fd);
    }
    free(dummy_buf);
    if (mounted)
    {
        Host_file_system_exit(nvme_mount_path);
    }
    if (cold_buffer_ready)
    {
        remove_buffer(&cold_buffer);
    }
    if (hot_buffer_ready)
    {
        remove_buffer(&hot_buffer);
    }
    latency_log_destroy(&hot_latencies);
    if (ctrl != NULL)
    {
        nvm_ctrl_free(ctrl);
    }
    return 1;
}
