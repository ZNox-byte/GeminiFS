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
#include <vector>
#include <algorithm>
#include <thread>
#include <system_error>
#include <nvm_util.h>
#include <nvm_queue.h>
#include <nvm_cmd.h>
#include <sched.h>

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

enum workload_type
{
    WORKLOAD_HOT = 0,
    WORKLOAD_COLD = 1
};

enum experiment_mode
{
    MODE_ISOLATED = 0,
    MODE_MIXED = 1,
    MODE_WEAK_MIXED = 2,
    MODE_STRONG_ISOLATED = 3,
};

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
            remove_queues(q, i + ctrl->cq_num);
            return status;
        }
    }
    printf("request_queues success\n");
    *queues = q;
    return status;
}


static constexpr size_t TotalThread = 16;
static constexpr size_t HotThread = 4;
static constexpr size_t ColdThread = TotalThread - HotThread;
static constexpr size_t hot_piece_size = 4096;
static constexpr size_t cold_piece_size = 128 * 1024;
static constexpr uint64_t Hot_ofst = 4096;
static constexpr uint64_t Cold_ofst = 2ULL * 1024ULL * 1024ULL;
static constexpr size_t SeedFilseSize  = 16 * 1024 * 1024;
static constexpr useconds_t HotThinkTime = 50;
static constexpr useconds_t ColdThinkTime = 1000;
static constexpr size_t LatencyCapacility = 100000;
static constexpr unsigned int TestDurationSeconds = 5;
static constexpr size_t MixedInflightDepth = 8;
static constexpr uint16_t RequiredQueueDepth = 64;

struct latency_log
{
    double* values;
    size_t count;
    size_t capacity;
};

struct inflight_request
{
    bool valid;
    bool is_hot;
    struct timespec submit_time;
    uint8_t opcode;
    uint32_t nsid;
    uint64_t slba;
    uint16_t nblocks;
};


struct thread_stats
{
    uint64_t hot_io_count;
    uint64_t cold_io_count;
    int error_status;
};

struct work_args
{
    size_t thread_index;
    workload_type workload;
    struct disk* disk;
    struct queue_pair* qp;
    nvm_dma_t* dma_buffer;
    nvm_dma_t* dma_buffer_cold;
    struct file_info* info;
    struct file_info* info_cold;
    struct latency_log* latencies;
    struct thread_stats* stats;
    
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

static const char* workload_name(workload_type workload)
{
    return workload == WORKLOAD_HOT ? "Hot" : "Cold";
}

static uint64_t diff_us(const struct timespec* start, const struct timespec* end)
{
    int64_t diff_sec = end->tv_sec - start->tv_sec;
    int64_t diff_nsec = end->tv_nsec - start->tv_nsec;
    
    if (diff_nsec < 0)
    {
        diff_sec -= 1;
        diff_nsec += 1000000000LL;
    }
    
    return (uint64_t)(diff_sec * 1000000ULL + diff_nsec / 1000ULL);
}

static double percentile_value(const std::vector<double>& log, double percentile)
{
    size_t index;

    if(log.empty())
    {
        return 0.0;
    }
    index = (size_t)(log.size() * percentile);
    if (index >= log.size())
    {
        index = log.size() - 1;
    }
    return log[index];
}

static experiment_mode parse_mode(int argc, char** argv)
{
    if(argc < 2)
    {
        return MODE_ISOLATED;
    }

    if (strcmp(argv[1], "isolated") == 0)
    {
        return MODE_ISOLATED;
    }

    if (strcmp(argv[1], "mixed") == 0)
    {
        return MODE_MIXED;
    }

    if (strcmp(argv[1], "weak-mixed") == 0)
    {
        return MODE_WEAK_MIXED;
    }

    if (strcmp(argv[1], "strong-i") == 0)
    {
        return MODE_STRONG_ISOLATED;
    }

    fprintf(stderr, "Unknown mode '%s', fallback to isolated\n", argv[1]);
    return MODE_ISOLATED;
}

static useconds_t think_time_us(workload_type workload)
{
    return workload == WORKLOAD_HOT ? HotThinkTime : ColdThinkTime;
}

static int read_module_io_queue_depth(unsigned int* depth_out)
{
    FILE* fp = fopen("/sys/module/snvme/parameters/io_queue_depth", "r");
    unsigned int depth = 0;

    if(fp == NULL)
    {
        return errno;
    }

    if(fscanf(fp, "%u", &depth) != 1)
    {
        fclose(fp);
        return EIO;
    }

    fclose(fp);

    if(depth < 2 || depth > 4095)
    {
        return ERANGE;
    }

    *depth_out = depth;
}

static int submit_direct_read(const struct disk* disk, struct queue_pair* qp, const nvm_dma_t* buffer, const struct file_info* info, uint16_t* cid_out, struct timespec* submit_time_out)
{
    nvm_queue_t* sq = &qp->sq->queue;
    size_t bytes = info->num_blocks * disk->block_size;
    size_t pages = NVM_PAGE_ALIGN(bytes, disk->page_size) / disk->page_size; //字节数换算成页数，第一个函数是对齐用的（假设page_size是4096，如果要读4097的话，就会向上对齐成4096*2）
    size_t max_pages = disk->max_data_size / disk->page_size;

    if(pages == 0 || pages > buffer->n_ioaddrs || pages > max_pages)
    {
        return EINVAL;
    }

    nvm_cmd_t* cmd = nvm_sq_enqueue(sq);
    if(cmd == NULL)
    {
        nvm_sq_submit(sq);
        return EAGAIN;
    }

    memset(cmd, 0, sizeof(nvm_cmd_t));

    uint16_t cmd_slot = (uint16_t)(((uintptr_t)cmd - (uintptr_t)sq->vaddr) / sq->es); //cmd这个指针落在第几个槽位，cmd_slot = (cmd 地址 - 队列起始地址) / 每个命令槽大小
    uint16_t cid = NVM_DEFAULT_CID(sq); //获取cid

    size_t sq_pages = NVM_SQ_PAGES(qp->sq->qmem.dma, sq->qs);
    size_t available_prp_pages = (qp->sq->qmem.dma->n_ioaddrs > sq_pages) ? (qp->sq->qmem.dma->n_ioaddrs - sq_pages) : 0;
    nvm_prp_list_t list = {};
    size_t n_lists = 0;

    if(pages > 2)
    {
        if(available_prp_pages == 0 || cmd_slot >= available_prp_pages)//这个地方得再搞明白一点
        {
            return ENOMEM;
        }
        list = NVM_PRP_LIST(qp->sq->qmem.dma, sq_pages + cmd_slot);
        n_lists = 1;
    }

    size_t num_blocks = NVM_PAGE_TO_BLOCK(disk->page_size, disk->block_size, pages);

    nvm_cmd_header(cmd, cid, NVM_IO_READ, disk->ns_id);
    nvm_cmd_data(cmd, n_lists, n_lists == 0 ? NULL : &list, pages, &buffer->ioaddrs[0]);
    nvm_cmd_rw_blks(cmd, info->offset, num_blocks);

    nvm_sq_submit(sq);

    if (submit_time_out != NULL)
    {
        // Start latency timing after the command is actually submitted.
        clock_gettime(CLOCK_MONOTONIC_RAW, submit_time_out);
    }

    if (cid_out != NULL)
    {
        *cid_out = cid;
    }

    return 0;
}

static uint64_t monotonic_time_ns()
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static int read_one_completion(struct queue_pair* qp, uint16_t* cid_out, uint64_t timeout_ms)
{
    nvm_queue_t* cq = &qp->cq->queue;
    nvm_queue_t* sq = &qp->sq->queue;
    nvm_cpl_t* cpl = nvm_cq_dequeue(cq); //cpl是完成项指针，里面有cid、status等信息
    uint64_t deadline_ns = monotonic_time_ns() + timeout_ms * 1000000ULL;//这是个什么算法？
    size_t spins = 0;
    int status = 0;
    uint16_t cid = 0;

    while(cpl == NULL)
    {
        if ((++spins & 0x3ff) == 0)
        {
            if (monotonic_time_ns() >= deadline_ns)
            {
                return ETIMEDOUT;
            }
            sched_yield();
        }
        cpl = nvm_cq_dequeue(cq);
    }

    //获取cid和status
    cid = *NVM_CPL_CID(cpl);
    status = NVM_ERR_STATUS(cpl);

    nvm_sq_update(sq);
    nvm_cq_update(cq);

    if (cid_out != NULL)
    {
        *cid_out = cid;
    }

    if (status != 0)
    {
        return status;
    }

    return 0;
}


void work_thread(struct work_args* kthread, experiment_mode mode)
{
    struct disk* disk = kthread->disk;
    struct queue_pair* qp = kthread->qp;
    nvm_dma_t* dma_buffer = kthread->dma_buffer;
    nvm_dma_t* dma_buffer_cold = kthread->dma_buffer_cold;
    struct file_info* info = kthread->info;
    struct file_info* info_cold = kthread->info_cold;
    struct latency_log* latency = kthread->latencies;
    struct thread_stats* stats = kthread->stats;

    if(mode == MODE_ISOLATED)
    {
        while(keep_running.load())
        {
            struct timespec start_time;
            struct timespec end_time;
            double elapsed_us;
            int status;

            clock_gettime(CLOCK_MONOTONIC, &start_time);
            status = pure_read(disk, qp, dma_buffer, info);
            clock_gettime(CLOCK_MONOTONIC, &end_time);
            elapsed_us = (double)diff_us(&start_time, &end_time);

            if(status == 0)
            {
                if(kthread->workload == WORKLOAD_HOT)
                {
                    stats->hot_io_count++;
                }
                else
                {
                    stats->cold_io_count++;
                }
                if (kthread->workload == WORKLOAD_HOT && latency != NULL)
                {
                    latency_log_push(latency, elapsed_us);
                }
            }
            else if(status == ECANCELED)
            {
                break;
            }
            else
            {
                stats->error_status = status;
                fprintf(stderr,
                        "[%s Thread %zu] pure_read failed: %s\n",
                        workload_name(kthread->workload),
                        kthread->thread_index,
                        strerror(status));
                break;
            }
            // usleep(think_time_us(kthread->workload));
        }
    }
    else if(mode == MODE_WEAK_MIXED)
    {
        size_t count = 0;
        while(keep_running.load())
        {
            struct timespec start_time;
            struct timespec end_time;
            double elapsed_us;
            int status;
            bool is_hot;

            is_hot = count % 4 == 0;
            if(is_hot)
            {
                clock_gettime(CLOCK_MONOTONIC, &start_time);
                status = pure_read(disk, qp, dma_buffer, info);
                clock_gettime(CLOCK_MONOTONIC, &end_time);
                elapsed_us = (double)diff_us(&start_time, &end_time);
            }
            else
            {
                clock_gettime(CLOCK_MONOTONIC, &start_time);
                status = pure_read(disk, qp, dma_buffer_cold, info_cold);
                clock_gettime(CLOCK_MONOTONIC, &end_time);
                elapsed_us = (double)diff_us(&start_time, &end_time);
            }

            if(status == 0)
            {
                if(is_hot)
                {
                    stats->hot_io_count++;
                    if (latency != NULL)
                    {
                        latency_log_push(latency, elapsed_us);
                    }
                }
                else
                {
                    stats->cold_io_count++;
                }
            }
            else if(status == ECANCELED)
            {
                break;
            }
            else
            {
                stats->error_status = status;
                fprintf(stderr,
                        "[%s Thread %zu] pure_read failed: %s\n",
                        is_hot ? "Hot" : "Cold",
                        kthread->thread_index,
                        strerror(status));
                break;
            }
            count++;
        }
    }
    else
    {
        size_t cid_slots = qp->sq->queue.qs * 2;
        size_t inflight_target = MixedInflightDepth;
        std::vector<struct inflight_request> inflight(cid_slots);
        size_t inflight_count = 0;
        size_t issue_count = 0;
        int status = 0;
        size_t sq_pages = NVM_SQ_PAGES(qp->sq->qmem.dma, qp->sq->queue.qs);//当前这个sq要占用多少个page

        if(cid_slots == 0)
        {
            stats->error_status = EINVAL;
            return;
        }

        if(inflight_target >= qp->sq->queue.qs)
        {
            inflight_target = qp->sq->queue.qs > 1 ? (qp->sq->queue.qs - 1) : 1;
        }

        if(qp->sq->qmem.dma->n_ioaddrs > sq_pages)
        {
            memset(NVM_DMA_OFFSET(qp->sq->qmem.dma, sq_pages), 0,
                        qp->sq->qmem.dma->page_size * (qp->sq->qmem.dma->n_ioaddrs - sq_pages));
            //第一个参数是起始点，跳过sq的原文，第三个参数是长度
        }

        while(keep_running.load())
        {
            while(keep_running.load() && inflight_count < inflight_target)
            {
                const nvm_dma_t* target_buffer = {};
                const struct file_info* target_info = {};
                bool is_hot = (issue_count % 4) == 0;
                if(mode == MODE_MIXED)
                {
                    
                    target_buffer = is_hot ? dma_buffer : dma_buffer_cold;
                    target_info = is_hot ? info : info_cold;
                }
                else if(mode == MODE_STRONG_ISOLATED)
                {
                    is_hot = kthread->workload == WORKLOAD_HOT ? 1 : 0;
                    target_buffer = dma_buffer;
                    target_info = info;
                }
                uint16_t cid = 0;
                struct timespec submit_time = {};

                status = submit_direct_read(disk, qp, target_buffer, target_info, &cid, &submit_time);

                if(status == EAGAIN)
                {
                    break;
                }

                if(status != 0)
                {
                    stats->error_status = status;
                    fprintf(stderr,
                            "[Mixed Thread %zu] submit failed: %s\n",
                            kthread->thread_index,
                            nvm_strerror(status));
                    return;
                }
                if(cid >= cid_slots || inflight[cid].valid)
                {
                    stats->error_status = EOVERFLOW;
                    fprintf(stderr,
                            "[Mixed Thread %zu] invalid cid state: cid=%u slots=%zu valid=%d\n",
                            kthread->thread_index,
                            cid,
                            cid_slots,
                            cid < cid_slots ? (int)inflight[cid].valid : -1);
                    return;
                }

                inflight[cid].submit_time = submit_time;
                inflight[cid].is_hot = is_hot;
                inflight[cid].valid = true;
                inflight[cid].opcode = NVM_IO_READ;
                inflight[cid].nsid = disk->ns_id;
                inflight[cid].slba = target_info->offset;
                inflight[cid].nblocks = (uint16_t)target_info->num_blocks;
                inflight_count++;
                issue_count++;

            }

            if(inflight_count == 0)
            {
                usleep(1);
                continue;
            }

            uint16_t done_cid = 0;
            status = read_one_completion(qp, &done_cid, 100);
            
            if(status == ETIMEDOUT)
            {
                if(qp->stop || !keep_running.load())
                {
                    break;
                }
                continue;
            }

            if(status != 0)
            {
                stats->error_status = status;
                fprintf(stderr,
                        "[Mixed Thread %zu] completion failed: %s (SQ=%u CQ=%u inflight=%zu)\n",
                        kthread->thread_index,
                        nvm_strerror(status),
                        qp->sq->queue.no,
                        qp->cq->queue.no,
                        inflight_count);
                if (done_cid < cid_slots && inflight[done_cid].valid)
                {
                    fprintf(stderr,
                            "[Mixed Thread %zu] failed cid=%u op=0x%x nsid=%u slba=%lu nblk=%u\n",
                            kthread->thread_index,
                            done_cid,
                            inflight[done_cid].opcode,
                            inflight[done_cid].nsid,
                            inflight[done_cid].slba,
                            inflight[done_cid].nblocks);
                }
                break;

            }

            if(done_cid >= cid_slots || !inflight[done_cid].valid)
            {
                stats->error_status = EIO;
                fprintf(stderr,
                        "[Mixed Thread %zu] completion cid mismatch: cid=%u slots=%zu\n",
                        kthread->thread_index,
                        done_cid,
                        cid_slots);
                break;
            }

            struct timespec done_time;
            clock_gettime(CLOCK_MONOTONIC_RAW, &done_time);

            if(inflight[done_cid].is_hot)
            {
                stats->hot_io_count++;
                if(latency != NULL)
                {
                    double elapsed_us = (double)diff_us(&inflight[done_cid].submit_time, &done_time);
                    latency_log_push(latency, elapsed_us);
                }
            }
            else
            {
                stats->cold_io_count++;
            }

            inflight[done_cid].valid = false;
            inflight_count--;
        }

        while(inflight_count > 0)
        {
            uint16_t done_cid = 0;
            status = read_one_completion(qp, &done_cid, 10);
            if(status == ETIMEDOUT)
            {
                break;
            }
            if(status != 0)
            {
                if(stats->error_status == 0)
                {
                    stats->error_status = status;
                }
                break;
            }

            if (done_cid >= cid_slots || !inflight[done_cid].valid)
            {
                continue;
            }

            struct timespec done_time;
            clock_gettime(CLOCK_MONOTONIC_RAW, &done_time);

            if (inflight[done_cid].is_hot)
            {
                stats->hot_io_count++;
                if (latency != NULL)
                {
                    double elapsed_us = (double)diff_us(&inflight[done_cid].submit_time, &done_time);
                    latency_log_push(latency, elapsed_us);
                }
            }
            else
            {
                stats->cold_io_count++;
            }

            inflight[done_cid].valid = false;
            inflight_count--;
        }
    }
}

int main(int argc, char** argv)
{
    experiment_mode mode = parse_mode(argc, argv);
    nvm_ctrl_t* ctrl = NULL;
    struct disk disk;
    struct buffer buffers[TotalThread] = {};
    struct buffer buffers_cold[TotalThread] = {};
    bool buffer_ready[TotalThread] = {};
    bool buffer_ready_cold[TotalThread] = {};
    int snvme_c_fd = -1, snvme_d_fd = -1, snvme_helper_fd = -1, fd = -1;
    uint64_t nvme_ofst[TotalThread] = {};
    uint64_t nvme_ofst_cold[TotalThread] = {};
    int ret, status;
    bool mounted = false;
    char* dummy_buf = NULL;
    struct queue_pair qps[TotalThread] = {};
    struct file_info infos[TotalThread] = {};
    struct file_info infos_cold[TotalThread] = {};
    struct latency_log hot_latencies[TotalThread] = {};
    struct work_args work_threads[TotalThread] = {};
    struct thread_stats stats_for_thread[TotalThread] = {};

    std::thread workers[TotalThread];

    double avg;
    double p95;
    double p99;
    size_t i;
    bool any_thread_started = false;
    std::vector<double> all_hot_latencies;
    size_t total_hot_ios = 0;
    size_t total_cold_ios = 0;
    int hot_error_count = 0;
    int cold_error_count = 0;
    
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

    ctrl->cq_num = TotalThread;
    ctrl->sq_num = TotalThread;
    ctrl->qs = RequiredQueueDepth;

    unsigned int module_qs = 0;
    status = read_module_io_queue_depth(&module_qs);
    if(status == 0)
    {
        if(module_qs != RequiredQueueDepth)
        {
            fprintf(stderr,
                    "Queue depth mismatch: module io_queue_depth=%u, benchmark expects %u.\n"
                    "Please reload module with: sudo insmod snvme.ko io_queue_depth=%u\n",
                    module_qs,
                    RequiredQueueDepth,
                    RequiredQueueDepth);
            goto out;
        }
    }
    else
    {
        fprintf(stderr,
                "Warning: failed to read /sys/module/snvme/parameters/io_queue_depth: %s\n",
                strerror(status));
    }
    
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
    if(mode == MODE_ISOLATED || mode == MODE_STRONG_ISOLATED)
    {
        for(i = 0; i < TotalThread; i++)
        {
            if(i < HotThread)
            {
                status = create_buffer(&buffers[i], ctrl, hot_piece_size, 0, -1);
            }
            else
            {
                status = create_buffer(&buffers[i], ctrl, cold_piece_size, 0, -1);
            }
            if (status != 0)
            {
                fprintf(stderr, "Failed to allocate DMA buffer for thread %zu: %s\n", i, strerror(status));
                goto out;
            }
            buffer_ready[i] = true;
        }
    }
    else
    {
        for(i = 0; i < TotalThread; i++)
        {
            status = create_buffer(&buffers[i], ctrl, hot_piece_size, 0, -1);
            if (status != 0)
            {
                fprintf(stderr, "Failed to allocate DMA buffer for thread %zu: %s\n", i, strerror(status));
                goto out;
            }
            buffer_ready[i] = true;

            status = create_buffer(&buffers_cold[i], ctrl, cold_piece_size, 0, -1);
            if (status != 0)
            {
                fprintf(stderr, "Failed to allocate cold DMA buffer for thread %zu: %s\n", i, strerror(status));
                goto out;
            }
            buffer_ready_cold[i] = true;
        }
    }
    

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

    dummy_buf = (char*)calloc(1, SeedFilseSize);
    if (dummy_buf == NULL)
    {
        fprintf(stderr, "Failed to allocate test buffer\n");
        goto out;
    }

    ret = write(fd, dummy_buf, SeedFilseSize);
    free(dummy_buf);
    dummy_buf = NULL; //避免第二次free
    if (ret != (int)SeedFilseSize)
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
    if(mode == MODE_ISOLATED || mode == MODE_STRONG_ISOLATED)
    {
        for(int i = 0; i < TotalThread; i++)
        {
            if(i < HotThread)
            {
                status = map_file_offset(snvme_helper_fd, fd, Hot_ofst + i * hot_piece_size, hot_piece_size, &nvme_ofst[i]);
                if (status != 0)
                {
                    fprintf(stderr, "Failed to map Hot file offset for thread %d: %s\n", i, strerror(status));
                    goto out;
                }
            }
            else
            {
                status = map_file_offset(snvme_helper_fd, fd, Cold_ofst + (i - HotThread) * cold_piece_size, cold_piece_size, &nvme_ofst[i]);
                if (status != 0)
                {
                    fprintf(stderr, "Failed to map Cold file offset for thread %d: %s\n", i, strerror(status));
                    goto out;
                }
            }
        }
    }
    else
    {
        for(int i = 0; i < TotalThread; i++)
        {

            status = map_file_offset(snvme_helper_fd, fd, Hot_ofst + i * hot_piece_size, hot_piece_size, &nvme_ofst[i]);
            if (status != 0)
            {
                fprintf(stderr, "Failed to map Hot file offset for thread %d: %s\n", i, strerror(status));
                goto out;
            }
            status = map_file_offset(snvme_helper_fd, fd, Cold_ofst + i * cold_piece_size, cold_piece_size, &nvme_ofst_cold[i]);
            if (status != 0)
            {
                fprintf(stderr, "Failed to map Cold file offset for thread %d: %s\n", i, strerror(status));
                goto out;
            }
        }
    }

    close(snvme_helper_fd);
    snvme_helper_fd = -1;

    for(i = 0; i < TotalThread; i++)
    {
        size_t queue_index;
        if(mode == MODE_ISOLATED || mode == MODE_STRONG_ISOLATED)
        {
            if(i < HotThread)
            {
                queue_index = i * 4;
            }
            else
            {
                queue_index = ((i - HotThread) % 3) + ((i - HotThread) / 3) * 4 + 1;
            }
        }
        else
        {
            queue_index = i;
        }
        qps[i].cq = &ctrl->queues[queue_index];
        qps[i].sq = &ctrl->queues[queue_index + ctrl->cq_num];
        qps[i].stop = false;
        qps[i].num_cpls = 0;

        infos[i].offset = nvme_ofst[i] >> 9;
        infos[i].namespace_id = disk.ns_id;
        infos[i].queue_size = ctrl->qs;
        if(mode == MODE_ISOLATED || mode == MODE_STRONG_ISOLATED)
        {
            if(i < HotThread)
            {
                infos[i].num_blocks = hot_piece_size >> 9;
                infos[i].chunk_size = hot_piece_size;
            }
            else
            {
                infos[i].num_blocks = cold_piece_size >> 9;
                infos[i].chunk_size = cold_piece_size;
            }
            if(i < HotThread)
            {
                status = latency_log_init(&hot_latencies[i], LatencyCapacility);
                if (status != 0)
                {
                    fprintf(stderr, "Failed to allocate latency log for hot thread %zu: %s\n", i, strerror(status));
                    goto out;
                }
            }
        }
        else
        {
            
            infos_cold[i].offset = nvme_ofst_cold[i] >> 9;
            infos_cold[i].namespace_id = disk.ns_id;
            infos_cold[i].queue_size = ctrl->qs;
            infos[i].num_blocks = hot_piece_size >> 9;
            infos[i].chunk_size = hot_piece_size;
            infos_cold[i].num_blocks = cold_piece_size >> 9;
            infos_cold[i].chunk_size = cold_piece_size;
            status = latency_log_init(&hot_latencies[i], LatencyCapacility);
            if (status != 0)
            {
                fprintf(stderr, "Failed to allocate latency log for hot thread %zu: %s\n", i, strerror(status));
                goto out;
            }
        }
        
        work_threads[i].thread_index = i;
        work_threads[i].disk = &disk;
        work_threads[i].dma_buffer = buffers[i].dma;
        work_threads[i].info = &infos[i];
        
        work_threads[i].qp = &qps[i];
        work_threads[i].stats = &stats_for_thread[i];
        if(mode == MODE_MIXED)
        {
            work_threads[i].dma_buffer_cold = buffers_cold[i].dma;
            work_threads[i].info_cold = &infos_cold[i];
            work_threads[i].latencies = &hot_latencies[i];
        }
        else
        {
            if(i < HotThread)
            {
                work_threads[i].latencies = &hot_latencies[i];
                work_threads[i].workload = WORKLOAD_HOT;
            }
            else
            {
                work_threads[i].latencies = NULL;
                work_threads[i].workload = WORKLOAD_COLD;
            }
        }
        
    }

    if (mode == MODE_MIXED)
    {
        printf("Mixed mode keeps hot+cold requests in-flight on each queue pair (pattern 1 hot : 3 cold, depth=%zu).\n",
               MixedInflightDepth);
    }
    else if (mode == MODE_WEAK_MIXED)
    {
        printf("Weak-mixed mode alternates hot/cold requests in the single-issue path.\n");
    }
    else if (mode == MODE_STRONG_ISOLATED)
    {
        printf("Strong-isolated mode reserves queue 0-3 for hot data and queue 4-15 for cold data, while keeping up to %zu requests in flight per queue pair.\n",
               MixedInflightDepth);
    }
    else
    {
        printf("Isolated mode reserves queue 0-3 for hot data and queue 4-15 for cold data.\n");
    }

    keep_running = true;

    for(int i = 0; i < TotalThread; i++)
    {
        try
        {
            workers[i] = std::thread(work_thread, &work_threads[i], mode);
            any_thread_started = true;
        }
        catch (const std::system_error& e)
        {
            fprintf(stderr, "Failed to start worker thread %zu: %s\n", i, e.what());
            goto out;
        }
        
    }
    sleep(TestDurationSeconds);
    keep_running = false;
    for(i = 0; i < TotalThread; i++)
    {
        qps[i].stop = true;
    }

    for(int i = 0; i < TotalThread; i++)
    {
        if(workers[i].joinable())
            workers[i].join();
    }

    any_thread_started = false;

    for(int i = 0; i < TotalThread; i++)
    {
        total_hot_ios += stats_for_thread[i].hot_io_count;
        total_cold_ios += stats_for_thread[i].cold_io_count;

        if(mode == MODE_ISOLATED)
        {
            if(i < HotThread)
            {
                if (stats_for_thread[i].error_status != 0)
                {
                    hot_error_count++;
                }
                all_hot_latencies.insert(all_hot_latencies.end(),
                                         hot_latencies[i].values,
                                         hot_latencies[i].values + hot_latencies[i].count);
            }
            else
            {
                if (stats_for_thread[i].error_status != 0)
                {
                    cold_error_count++;
                }
            }
        }
        else
        {
            if (stats_for_thread[i].error_status != 0)
            {
                hot_error_count++;
            }
            all_hot_latencies.insert(all_hot_latencies.end(),
                                     hot_latencies[i].values,
                                     hot_latencies[i].values + hot_latencies[i].count);
        }
    }


    if (!all_hot_latencies.empty())
    {
        bool run_valid = (hot_error_count == 0 && cold_error_count == 0);

        std::sort(all_hot_latencies.begin(), all_hot_latencies.end());
        avg = 0.0;
        for (double latency : all_hot_latencies)
        {
            avg += latency;
        }
        avg /= (double)all_hot_latencies.size();

        p95 = percentile_value(all_hot_latencies, 0.95);
        p99 = percentile_value(all_hot_latencies, 0.99);

        printf("\n--- Hot Data Latency Report ---\n");
        if (!run_valid)
        {
            printf("WARNING: completion errors detected, this latency sample is invalid for comparison.\n");
        }
        // printf("Mode: %s\n", mode_name(mode));
        printf("Hot Threads: %zu, Cold Threads: %zu\n", HotThread, ColdThread);
        printf("Total Hot Requests: %zu\n", all_hot_latencies.size());
        printf("Average Latency: %.2f us\n", avg);
        printf("P95 Tail Latency: %.2f us\n", p95);
        printf("P99 Tail Latency: %.2f us\n", p99);
        printf("Hot Throughput: %.2f MB/s\n",
               (double)(total_hot_ios * hot_piece_size) / (1024.0 * 1024.0 * TestDurationSeconds));
        printf("Cold Throughput: %.2f MB/s\n",
               (double)(total_cold_ios * cold_piece_size) / (1024.0 * 1024.0 * TestDurationSeconds));
        printf("Hot Thread Errors: %d\n", hot_error_count);
        printf("Cold Thread Errors: %d\n", cold_error_count);
    }

    close(fd);
    fd = -1;
    Host_file_system_exit(nvme_mount_path);
    mounted = false;
    // printf("cleanup: start remove_buffer on normal path\n");
    for (i = 0; i < TotalThread; ++i)
    {
        if (buffer_ready[i])
        {
            remove_buffer(&buffers[i]);
        }
        if (buffer_ready_cold[i])
        {
            remove_buffer(&buffers_cold[i]);
        }
    }
    for (i = 0; i < TotalThread; ++i)
    {
        latency_log_destroy(&hot_latencies[i]);
    }
    if (ctrl != NULL)
    {
        nvm_ctrl_free(ctrl);
    }
    return 0;
out:
    keep_running = false;
    for (i = 0; i < TotalThread; ++i)
    {
        qps[i].stop = true;
    }
    if (any_thread_started)
    {
        for (i = 0; i < TotalThread; ++i)
        {
            if (workers[i].joinable())
            {
                workers[i].join();
            }
        }
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
    // printf("cleanup: start remove_buffer on error path\n");
    for (i = 0; i < TotalThread; ++i)
    {
        if (buffer_ready[i])
        {
            remove_buffer(&buffers[i]);
        }
        if (buffer_ready_cold[i])
        {
            remove_buffer(&buffers_cold[i]);
        }
    }
    for (i = 0; i < TotalThread; ++i)
    {
        latency_log_destroy(&hot_latencies[i]);
    }
    if (ctrl != NULL)
    {
        nvm_ctrl_free(ctrl);
    }
    
    return 1;
}
