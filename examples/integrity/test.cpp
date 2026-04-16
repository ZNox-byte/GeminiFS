#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <nvm_types.h>
#include <nvm_ctrl.h>
#include <nvm_dma.h>
#include <nvm_aq.h>
#include <nvm_error.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <limits.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <atomic>
#include <vector>
#include <algorithm>
#include <thread>
#include <chrono>
#include <mutex>
#include <condition_variable>
#include <deque>
#include <system_error>
#include <nvm_util.h>
#include <nvm_queue.h>
#include <nvm_cmd.h>

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
#define nvme_pci_addr {0xc1, 0, 0}

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
    MODE_HOT_ONLY = 4,
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


static constexpr size_t MaxThreadCount = 16;
static constexpr size_t DefaultHotThreadCount = 4;
static constexpr size_t DefaultColdThreadCount = MaxThreadCount - DefaultHotThreadCount;
static constexpr size_t hot_piece_size = 4096;
static constexpr size_t cold_piece_size = 128 * 1024;
static constexpr uint64_t Hot_ofst = 4096;
static constexpr uint64_t Cold_ofst = 2ULL * 1024ULL * 1024ULL;
static constexpr size_t SeedFilseSize  = 16 * 1024 * 1024;
static constexpr useconds_t HotThinkTime = 50;
static constexpr useconds_t ColdThinkTime = 1000;
static constexpr size_t LatencyCapacility = 100000;
static constexpr unsigned int DefaultTestDurationSeconds = 5;
static constexpr size_t DefaultMixedInflightDepth = 16;
static constexpr size_t MixedPendingDepthMultiplier = 8;
static constexpr size_t DefaultStrongIsolatedHotInflightDepth = 16;
static constexpr size_t DefaultStrongIsolatedColdInflightDepth = 8;
static constexpr uint16_t DefaultQueueDepth = 128;
static constexpr size_t DispatchCidCount = 1u << 16;

struct run_config
{
    bool use_budget;
    size_t hot_budget;
    size_t cold_budget;
    uint16_t queue_depth;
    size_t mixed_inflight_depth;
    size_t strong_hot_inflight_depth;
    size_t strong_cold_inflight_depth;
    unsigned int duration_seconds;
    size_t hot_threads;
    size_t cold_threads;
    bool hot_threads_overridden;
    bool cold_threads_overridden;
};

static struct run_config g_run_config = {false,
                                         0,
                                         0,
                                         DefaultQueueDepth,
                                         DefaultMixedInflightDepth,
                                         DefaultStrongIsolatedHotInflightDepth,
                                         DefaultStrongIsolatedColdInflightDepth,
                                         DefaultTestDurationSeconds,
                                         DefaultHotThreadCount,
                                         DefaultColdThreadCount,
                                         false,
                                         false};
static std::atomic<size_t> g_hot_generated(0);
static std::atomic<size_t> g_cold_generated(0);

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
    struct timespec issue_time;
    struct timespec submit_time;
    struct thread_stats* owner_stats;
    struct latency_log* owner_latency;
    size_t owner_thread_index;
    uint8_t opcode;
    uint32_t nsid;
    uint64_t slba;
    uint16_t nblocks;
};

struct mixed_dispatch_request
{
    bool valid;
    bool is_hot;
    struct timespec issue_time;
    const nvm_dma_t* buffer;
    const struct file_info* info;
    struct thread_stats* stats;
    struct latency_log* latency;
    size_t thread_index;
};

enum dispatch_policy
{
    DISPATCH_FIFO = 0,
    DISPATCH_MIXED_RATIO = 1
};

struct dispatch_group_state
{
    struct queue_pair* qp;
    dispatch_policy policy;
    size_t inflight_target;
    std::mutex mutex;
    std::condition_variable can_push;
    std::condition_variable can_pop;
    std::deque<struct mixed_dispatch_request> hot_pending;
    std::deque<struct mixed_dispatch_request> cold_pending;
    size_t issue_count;
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
    struct dispatch_group_state* dispatch_groups;
    size_t dispatch_group_base;
    size_t dispatch_group_count;
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

static size_t pending_count_locked(const struct dispatch_group_state* group)
{
    return group->hot_pending.size() + group->cold_pending.size();
}

static bool try_pop_dispatch_request_locked(struct dispatch_group_state* group,
                                           struct mixed_dispatch_request* request_out)
{
    if (group->policy == DISPATCH_MIXED_RATIO)
    {
        bool preferred_hot = (group->issue_count % 4) == 0;
        std::deque<struct mixed_dispatch_request>* preferred =
            preferred_hot ? &group->hot_pending : &group->cold_pending;
        std::deque<struct mixed_dispatch_request>* alternate =
            preferred_hot ? &group->cold_pending : &group->hot_pending;

        if (!preferred->empty())
        {
            *request_out = preferred->front();
            preferred->pop_front();
            group->issue_count++;
            return true;
        }

        if (!alternate->empty())
        {
            *request_out = alternate->front();
            alternate->pop_front();
            group->issue_count++;
            return true;
        }

        return false;
    }

    if (!group->hot_pending.empty())
    {
        *request_out = group->hot_pending.front();
        group->hot_pending.pop_front();
        group->issue_count++;
        return true;
    }

    if (!group->cold_pending.empty())
    {
        *request_out = group->cold_pending.front();
        group->cold_pending.pop_front();
        group->issue_count++;
        return true;
    }

    return false;
}

static int parse_size_option(const char* value, size_t* out)
{
    char* endptr = NULL;
    unsigned long long parsed = strtoull(value, &endptr, 10);

    if (value[0] == '\0' || endptr == value || *endptr != '\0')
    {
        return EINVAL;
    }

    *out = (size_t)parsed;
    return 0;
}

static int parse_u16_option(const char* value, uint16_t* out)
{
    char* endptr = NULL;
    unsigned long parsed = strtoul(value, &endptr, 10);

    if (value[0] == '\0' || endptr == value || *endptr != '\0' || parsed == 0 || parsed > UINT16_MAX)
    {
        return EINVAL;
    }

    *out = (uint16_t)parsed;
    return 0;
}

static int parse_u32_option(const char* value, unsigned int* out)
{
    char* endptr = NULL;
    unsigned long parsed = strtoul(value, &endptr, 10);

    if (value[0] == '\0' || endptr == value || *endptr != '\0' || parsed == 0 || parsed > UINT_MAX)
    {
        return EINVAL;
    }

    *out = (unsigned int)parsed;
    return 0;
}

static int parse_run_config(int argc, char** argv, struct run_config* config_out)
{
    *config_out = {false,
                   0,
                   0,
                   DefaultQueueDepth,
                   DefaultMixedInflightDepth,
                   DefaultStrongIsolatedHotInflightDepth,
                   DefaultStrongIsolatedColdInflightDepth,
                   DefaultTestDurationSeconds,
                   DefaultHotThreadCount,
                   DefaultColdThreadCount,
                   false,
                   false};

    for (int i = 2; i < argc; ++i)
    {
        if (strcmp(argv[i], "--hot-budget") == 0)
        {
            if (i + 1 >= argc || parse_size_option(argv[++i], &config_out->hot_budget) != 0)
            {
                fprintf(stderr, "Invalid value for --hot-budget.\n");
                return EINVAL;
            }
            config_out->use_budget = true;
        }
        else if (strcmp(argv[i], "--cold-budget") == 0)
        {
            if (i + 1 >= argc || parse_size_option(argv[++i], &config_out->cold_budget) != 0)
            {
                fprintf(stderr, "Invalid value for --cold-budget.\n");
                return EINVAL;
            }
            config_out->use_budget = true;
        }
        else if (strcmp(argv[i], "--queue-depth") == 0 || strcmp(argv[i], "--queue_depth") == 0)
        {
            if (i + 1 >= argc || parse_u16_option(argv[++i], &config_out->queue_depth) != 0)
            {
                fprintf(stderr, "Invalid value for --queue-depth.\n");
                return EINVAL;
            }
        }
        else if (strcmp(argv[i], "--duration") == 0)
        {
            if (i + 1 >= argc || parse_u32_option(argv[++i], &config_out->duration_seconds) != 0)
            {
                fprintf(stderr, "Invalid value for --duration.\n");
                return EINVAL;
            }
        }
        else if (strcmp(argv[i], "--hot-threads") == 0 || strcmp(argv[i], "--hot_threads") == 0)
        {
            if (i + 1 >= argc || parse_size_option(argv[++i], &config_out->hot_threads) != 0
                || config_out->hot_threads > MaxThreadCount)
            {
                fprintf(stderr, "Invalid value for --hot-threads.\n");
                return EINVAL;
            }
            config_out->hot_threads_overridden = true;
        }
        else if (strcmp(argv[i], "--cold-threads") == 0 || strcmp(argv[i], "--cold_threads") == 0)
        {
            if (i + 1 >= argc || parse_size_option(argv[++i], &config_out->cold_threads) != 0
                || config_out->cold_threads > MaxThreadCount)
            {
                fprintf(stderr, "Invalid value for --cold-threads.\n");
                return EINVAL;
            }
            config_out->cold_threads_overridden = true;
        }
        else if (strcmp(argv[i], "--mixed-inflight") == 0 || strcmp(argv[i], "--mixed_inflight") == 0)
        {
            if (i + 1 >= argc || parse_size_option(argv[++i], &config_out->mixed_inflight_depth) != 0
                || config_out->mixed_inflight_depth == 0)
            {
                fprintf(stderr, "Invalid value for --mixed-inflight.\n");
                return EINVAL;
            }
        }
        else if (strcmp(argv[i], "--strong-hot-inflight") == 0 || strcmp(argv[i], "--strong_hot_inflight") == 0)
        {
            if (i + 1 >= argc || parse_size_option(argv[++i], &config_out->strong_hot_inflight_depth) != 0
                || config_out->strong_hot_inflight_depth == 0)
            {
                fprintf(stderr, "Invalid value for --strong-hot-inflight.\n");
                return EINVAL;
            }
        }
        else if (strcmp(argv[i], "--strong-cold-inflight") == 0 || strcmp(argv[i], "--strong_cold_inflight") == 0)
        {
            if (i + 1 >= argc || parse_size_option(argv[++i], &config_out->strong_cold_inflight_depth) != 0
                || config_out->strong_cold_inflight_depth == 0)
            {
                fprintf(stderr, "Invalid value for --strong-cold-inflight.\n");
                return EINVAL;
            }
        }
        else
        {
            fprintf(stderr,
                    "Unknown option '%s'. Supported options: --hot-budget, --cold-budget, --queue-depth, --duration, --hot-threads, --cold-threads, --mixed-inflight, --strong-hot-inflight, --strong-cold-inflight\n",
                    argv[i]);
            return EINVAL;
        }
    }

    return 0;
}

static int resolve_thread_layout(experiment_mode mode,
                                 const struct run_config* config,
                                 size_t* hot_threads_out,
                                 size_t* cold_threads_out,
                                 size_t* active_threads_out)
{
    size_t hot_threads = config->hot_threads_overridden ? config->hot_threads
                                                        : (mode == MODE_HOT_ONLY ? MaxThreadCount
                                                                                 : DefaultHotThreadCount);
    size_t cold_threads = config->cold_threads_overridden ? config->cold_threads
                                                          : (mode == MODE_HOT_ONLY ? 0
                                                                                   : DefaultColdThreadCount);

    if (mode == MODE_HOT_ONLY && config->cold_threads_overridden && cold_threads != 0)
    {
        fprintf(stderr, "--cold-threads is not supported in hot-only mode.\n");
        return EINVAL;
    }

    if (hot_threads == 0)
    {
        fprintf(stderr, "--hot-threads must be at least 1.\n");
        return EINVAL;
    }

    size_t active_threads = mode == MODE_HOT_ONLY ? hot_threads : (hot_threads + cold_threads);
    if (active_threads == 0 || active_threads > MaxThreadCount)
    {
        fprintf(stderr,
                "Invalid thread layout: hot=%zu cold=%zu exceeds max supported producers=%zu.\n",
                hot_threads,
                cold_threads,
                MaxThreadCount);
        return EINVAL;
    }

    *hot_threads_out = hot_threads;
    *cold_threads_out = mode == MODE_HOT_ONLY ? 0 : cold_threads;
    *active_threads_out = active_threads;
    return 0;
}

static size_t mixed_pending_depth_limit()
{
    return g_run_config.mixed_inflight_depth * MixedPendingDepthMultiplier;
}

static bool reserve_budget_slot(workload_type workload)
{
    if (!g_run_config.use_budget)
    {
        return true;
    }

    std::atomic<size_t>* counter = workload == WORKLOAD_HOT ? &g_hot_generated : &g_cold_generated;
    size_t limit = workload == WORKLOAD_HOT ? g_run_config.hot_budget : g_run_config.cold_budget;
    size_t current = counter->load(std::memory_order_relaxed);

    while (current < limit)
    {
        if (counter->compare_exchange_weak(current,
                                           current + 1,
                                           std::memory_order_relaxed,
                                           std::memory_order_relaxed))
        {
            return true;
        }
    }

    return false;
}

static bool budgets_exhausted(experiment_mode mode)
{
    if (!g_run_config.use_budget)
    {
        return false;
    }

    bool hot_done = g_hot_generated.load(std::memory_order_relaxed) >= g_run_config.hot_budget;
    bool cold_done = g_cold_generated.load(std::memory_order_relaxed) >= g_run_config.cold_budget;

    if (mode == MODE_HOT_ONLY)
    {
        return hot_done;
    }

    return hot_done && cold_done;
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

    if (strcmp(argv[1], "hot-only") == 0)
    {
        return MODE_HOT_ONLY;
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
    return 0;
}

static int submit_direct_read(const struct disk* disk,
                              struct queue_pair* qp,
                              const nvm_dma_t* buffer,
                              const struct file_info* info,
                              uint16_t cid,
                              struct timespec* submit_time_out)
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

    return 0;
}

static uint16_t reserve_dispatch_cid(std::vector<struct inflight_request>* inflight,
                                     uint16_t* next_cid_io)
{
    uint16_t cid = *next_cid_io;

    for (size_t attempt = 0; attempt < DispatchCidCount; ++attempt)
    {
        if (!(*inflight)[cid].valid)
        {
            *next_cid_io = (uint16_t)(cid + 1);
            return cid;
        }
        cid = (uint16_t)(cid + 1);
    }

    return UINT16_MAX;
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

static void dispatch_thread(struct dispatch_group_state* group,
                            const struct disk* disk)
{
    size_t inflight_target = group->inflight_target;
    std::vector<struct inflight_request> inflight(DispatchCidCount);
    struct mixed_dispatch_request pending = {};
    size_t inflight_count = 0;
    int status = 0;
    uint16_t next_cid = 1;
    size_t sq_pages = NVM_SQ_PAGES(group->qp->sq->qmem.dma, group->qp->sq->queue.qs);

    if (inflight_target >= group->qp->sq->queue.qs)
    {
        inflight_target = group->qp->sq->queue.qs > 1 ? (group->qp->sq->queue.qs - 1) : 1;
    }

    if (group->qp->sq->qmem.dma->n_ioaddrs > sq_pages)
    {
        memset(NVM_DMA_OFFSET(group->qp->sq->qmem.dma, sq_pages), 0,
               group->qp->sq->qmem.dma->page_size * (group->qp->sq->qmem.dma->n_ioaddrs - sq_pages));
    }

    while (keep_running.load() || inflight_count > 0 || pending.valid)
    {
        while (inflight_count < inflight_target)
        {
            if (!pending.valid)
            {
                std::unique_lock<std::mutex> lock(group->mutex);
                if (!try_pop_dispatch_request_locked(group, &pending))
                {
                    group->can_push.notify_all();
                    break;
                }
                group->can_push.notify_all();
            }

            uint16_t cid = 0;
            struct timespec submit_time = {};
            cid = reserve_dispatch_cid(&inflight, &next_cid);
            if (cid == UINT16_MAX)
            {
                if (pending.stats != NULL && pending.stats->error_status == 0)
                {
                    pending.stats->error_status = ENOSPC;
                }
                fprintf(stderr, "[Dispatcher] exhausted CID space while requests are still inflight.\n");
                return;
            }

            status = submit_direct_read(disk, group->qp, pending.buffer, pending.info, cid, &submit_time);

            if (status == EAGAIN)
            {
                break;
            }

            if (status != 0)
            {
                if (pending.stats != NULL && pending.stats->error_status == 0)
                {
                    pending.stats->error_status = status;
                }
                fprintf(stderr,
                        "[Dispatcher] submit failed for thread %zu: %s\n",
                        pending.thread_index,
                        nvm_strerror(status));
                return;
            }

            if (cid >= inflight.size())
            {
                if (pending.stats != NULL && pending.stats->error_status == 0)
                {
                    pending.stats->error_status = ERANGE;
                }
                fprintf(stderr,
                        "[Dispatcher] returned cid=%u exceeds inflight slots=%zu\n",
                        cid,
                        inflight.size());
                return;
            }

            if (inflight[cid].valid)
            {
                if (pending.stats != NULL && pending.stats->error_status == 0)
                {
                    pending.stats->error_status = EOVERFLOW;
                }
                fprintf(stderr,
                        "[Dispatcher] invalid cid state: cid=%u slots=%zu valid=%d\n",
                        cid,
                        inflight.size(),
                        (int)inflight[cid].valid);
                return;
            }

            inflight[cid].issue_time = pending.issue_time;
            inflight[cid].submit_time = submit_time;
            inflight[cid].is_hot = pending.is_hot;
            inflight[cid].valid = true;
            inflight[cid].owner_stats = pending.stats;
            inflight[cid].owner_latency = pending.latency;
            inflight[cid].owner_thread_index = pending.thread_index;
            inflight[cid].opcode = NVM_IO_READ;
            inflight[cid].nsid = disk->ns_id;
            inflight[cid].slba = pending.info->offset;
            inflight[cid].nblocks = (uint16_t)pending.info->num_blocks;
            inflight_count++;
            pending.valid = false;
        }

        if (inflight_count == 0)
        {
            std::unique_lock<std::mutex> lock(group->mutex);
            if (!keep_running.load() && pending_count_locked(group) == 0)
            {
                break;
            }
            group->can_pop.wait_for(lock, std::chrono::milliseconds(1));
            continue;
        }

        uint16_t done_cid = 0;
        status = read_one_completion(group->qp, &done_cid, 100);

        if (status == ETIMEDOUT)
        {
            continue;
        }

        if (status != 0)
        {
            fprintf(stderr,
                    "[Dispatcher] completion failed: %s (SQ=%u CQ=%u inflight=%zu)\n",
                    nvm_strerror(status),
                    group->qp->sq->queue.no,
                    group->qp->cq->queue.no,
                    inflight_count);
            break;
        }

        if (!inflight[done_cid].valid)
        {
            fprintf(stderr,
                    "[Dispatcher] completion cid mismatch: cid=%u slots=%zu\n",
                    done_cid,
                    inflight.size());
            break;
        }

        struct timespec done_time;
        clock_gettime(CLOCK_MONOTONIC_RAW, &done_time);

        if (inflight[done_cid].is_hot)
        {
            inflight[done_cid].owner_stats->hot_io_count++;
            if (inflight[done_cid].owner_latency != NULL)
            {
                double elapsed_us = (double)diff_us(&inflight[done_cid].issue_time, &done_time);
                latency_log_push(inflight[done_cid].owner_latency, elapsed_us);
            }
        }
        else
        {
            inflight[done_cid].owner_stats->cold_io_count++;
        }

        inflight[done_cid].valid = false;
        inflight_count--;
    }
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

    if (mode == MODE_MIXED || mode == MODE_STRONG_ISOLATED || mode == MODE_HOT_ONLY)
    {
        size_t route_count = 0;

        while (keep_running.load())
        {
            struct mixed_dispatch_request request = {};
            struct dispatch_group_state* group = &kthread->dispatch_groups[
                kthread->dispatch_group_base + (route_count % kthread->dispatch_group_count)];
            workload_type target_workload = (mode == MODE_HOT_ONLY)
                ? WORKLOAD_HOT
                : kthread->workload;

            if (!reserve_budget_slot(target_workload))
            {
                break;
            }

            request.valid = true;
            request.is_hot = (target_workload == WORKLOAD_HOT);
            request.buffer = request.is_hot ? dma_buffer : (dma_buffer_cold != NULL ? dma_buffer_cold : dma_buffer);
            request.info = request.is_hot ? info : (info_cold != NULL ? info_cold : info);
            request.stats = stats;
            request.latency = request.is_hot ? latency : NULL;
            request.thread_index = kthread->thread_index;
            clock_gettime(CLOCK_MONOTONIC_RAW, &request.issue_time);

            std::unique_lock<std::mutex> lock(group->mutex);
            group->can_push.wait(lock, [group]() {
                return !keep_running.load() ||
                       pending_count_locked(group) < mixed_pending_depth_limit();
            });

            if (!keep_running.load())
            {
                break;
            }

            if (request.is_hot)
            {
                group->hot_pending.push_back(request);
            }
            else
            {
                group->cold_pending.push_back(request);
            }
            lock.unlock();
            group->can_pop.notify_one();
            route_count++;
        }
        return;
    }

    if(mode == MODE_ISOLATED)
    {
        while(keep_running.load())
        {
            struct timespec start_time;
            struct timespec end_time;
            double elapsed_us;
            int status;

            if (!reserve_budget_slot(kthread->workload))
            {
                break;
            }

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
            if (!reserve_budget_slot(is_hot ? WORKLOAD_HOT : WORKLOAD_COLD))
            {
                break;
            }
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
}

int main(int argc, char** argv)
{
    experiment_mode mode = parse_mode(argc, argv);
    nvm_ctrl_t* ctrl = NULL;
    struct disk disk;
    struct buffer buffers[MaxThreadCount] = {};
    struct buffer buffers_cold[MaxThreadCount] = {};
    bool buffer_ready[MaxThreadCount] = {};
    bool buffer_ready_cold[MaxThreadCount] = {};
    int snvme_c_fd = -1, snvme_d_fd = -1, snvme_helper_fd = -1, fd = -1;
    uint64_t nvme_ofst[MaxThreadCount] = {};
    uint64_t nvme_ofst_cold[MaxThreadCount] = {};
    int ret, status;
    bool mounted = false;
    char* dummy_buf = NULL;
    struct queue_pair qps[MaxThreadCount] = {};
    struct file_info infos[MaxThreadCount] = {};
    struct file_info infos_cold[MaxThreadCount] = {};
    struct latency_log hot_latencies[MaxThreadCount] = {};
    struct work_args work_threads[MaxThreadCount] = {};
    struct thread_stats stats_for_thread[MaxThreadCount] = {};
    struct dispatch_group_state dispatch_groups[MaxThreadCount] = {};

    std::thread workers[MaxThreadCount];
    std::thread dispatchers[MaxThreadCount];

    double avg;
    double p95;
    double p99;
    double elapsed_seconds = 0.0;
    size_t i;
    bool any_thread_started = false;
    std::vector<double> all_hot_latencies;
    size_t total_hot_ios = 0;
    size_t total_cold_ios = 0;
    int hot_error_count = 0;
    int cold_error_count = 0;
    uint64_t run_start_ns = 0;
    uint64_t run_end_ns = 0;
    size_t hot_thread_count = 0;
    size_t cold_thread_count = 0;
    size_t active_thread_count = 0;
    bool use_legacy_isolated_layout = false;

    status = parse_run_config(argc, argv, &g_run_config);
    if (status != 0)
    {
        return 1;
    }
    status = resolve_thread_layout(mode, &g_run_config, &hot_thread_count, &cold_thread_count, &active_thread_count);
    if (status != 0)
    {
        return 1;
    }
    use_legacy_isolated_layout = (hot_thread_count == DefaultHotThreadCount
                               && cold_thread_count == DefaultColdThreadCount
                               && active_thread_count == MaxThreadCount);
    g_hot_generated.store(0, std::memory_order_relaxed);
    g_cold_generated.store(0, std::memory_order_relaxed);
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

    ctrl->cq_num = active_thread_count;
    ctrl->sq_num = active_thread_count;
    ctrl->qs = g_run_config.queue_depth;

    unsigned int module_qs = 0;
    status = read_module_io_queue_depth(&module_qs);
    if(status == 0)
    {
        if(module_qs != g_run_config.queue_depth)
        {
            fprintf(stderr,
                    "Queue depth mismatch: module io_queue_depth=%u, benchmark expects %u.\n"
                    "Please reload module with: sudo insmod snvme.ko io_queue_depth=%u\n",
                    module_qs,
                    g_run_config.queue_depth,
                    g_run_config.queue_depth);
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
    if(mode == MODE_ISOLATED || mode == MODE_STRONG_ISOLATED || mode == MODE_HOT_ONLY)
    {
        for(i = 0; i < active_thread_count; i++)
        {
            if(mode == MODE_HOT_ONLY || i < hot_thread_count)
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
        for(i = 0; i < active_thread_count; i++)
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
    if(mode == MODE_ISOLATED || mode == MODE_STRONG_ISOLATED || mode == MODE_HOT_ONLY)
    {
        for(int i = 0; i < (int)active_thread_count; i++)
        {
            if(mode == MODE_HOT_ONLY || (size_t)i < hot_thread_count)
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
                status = map_file_offset(snvme_helper_fd, fd, Cold_ofst + (i - (int)hot_thread_count) * cold_piece_size, cold_piece_size, &nvme_ofst[i]);
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
        for(int i = 0; i < (int)active_thread_count; i++)
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

    for(i = 0; i < active_thread_count; i++)
    {
        size_t queue_index;
        if(mode == MODE_ISOLATED || mode == MODE_STRONG_ISOLATED)
        {
            if(mode == MODE_HOT_ONLY || i < hot_thread_count)
            {
                queue_index = use_legacy_isolated_layout ? (i * 4) : i;
            }
            else
            {
                queue_index = use_legacy_isolated_layout
                    ? (((i - hot_thread_count) % 3) + ((i - hot_thread_count) / 3) * 4 + 1)
                    : i;
            }
        }
        else
        {
            if (mode == MODE_MIXED || mode == MODE_HOT_ONLY)
            {
                queue_index = i;
            }
            else
            {
                queue_index = i;
            }
        }
        qps[i].cq = &ctrl->queues[queue_index];
        qps[i].sq = &ctrl->queues[queue_index + ctrl->cq_num];
        qps[i].stop = false;
        qps[i].num_cpls = 0;

        infos[i].offset = nvme_ofst[i] >> 9;
        infos[i].namespace_id = disk.ns_id;
        infos[i].queue_size = ctrl->qs;
        if(mode == MODE_ISOLATED || mode == MODE_STRONG_ISOLATED || mode == MODE_HOT_ONLY)
        {
            if(mode == MODE_HOT_ONLY || i < hot_thread_count)
            {
                infos[i].num_blocks = hot_piece_size >> 9;
                infos[i].chunk_size = hot_piece_size;
            }
            else
            {
                infos[i].num_blocks = cold_piece_size >> 9;
                infos[i].chunk_size = cold_piece_size;
            }
            if(mode == MODE_HOT_ONLY || i < hot_thread_count)
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
        work_threads[i].dispatch_groups = dispatch_groups;
        work_threads[i].dispatch_group_base = 0;
        work_threads[i].dispatch_group_count = 0;
        work_threads[i].qp = &qps[i];
        work_threads[i].stats = &stats_for_thread[i];
        if(mode == MODE_MIXED)
        {
            work_threads[i].dma_buffer_cold = buffers_cold[i].dma;
            work_threads[i].info_cold = &infos_cold[i];
            work_threads[i].latencies = (i < hot_thread_count) ? &hot_latencies[i] : NULL;
            work_threads[i].workload = (i < hot_thread_count) ? WORKLOAD_HOT : WORKLOAD_COLD;
            work_threads[i].dispatch_group_base = 0;
            work_threads[i].dispatch_group_count = active_thread_count;
        }
        else if(mode == MODE_HOT_ONLY)
        {
            work_threads[i].dma_buffer_cold = NULL;
            work_threads[i].info_cold = NULL;
            work_threads[i].latencies = &hot_latencies[i];
            work_threads[i].workload = WORKLOAD_HOT;
            work_threads[i].dispatch_group_base = 0;
            work_threads[i].dispatch_group_count = active_thread_count;
        }
        else if(mode == MODE_WEAK_MIXED)
        {
            work_threads[i].dma_buffer_cold = buffers_cold[i].dma;
            work_threads[i].info_cold = &infos_cold[i];
            work_threads[i].latencies = &hot_latencies[i];
        }
        else
        {
            if(i < hot_thread_count)
            {
                work_threads[i].dma_buffer_cold = NULL;
                work_threads[i].info_cold = NULL;
                work_threads[i].latencies = &hot_latencies[i];
                work_threads[i].workload = WORKLOAD_HOT;
                work_threads[i].dispatch_group_base = 0;
                work_threads[i].dispatch_group_count = hot_thread_count;
            }
            else
            {
                work_threads[i].dma_buffer_cold = NULL;
                work_threads[i].info_cold = NULL;
                work_threads[i].latencies = NULL;
                work_threads[i].workload = WORKLOAD_COLD;
                if (mode == MODE_STRONG_ISOLATED)
                {
                    work_threads[i].dispatch_group_base = hot_thread_count;
                    work_threads[i].dispatch_group_count = cold_thread_count;
                }
            }
        }
        
    }

    if (mode == MODE_MIXED)
    {
        for (i = 0; i < active_thread_count; ++i)
        {
            dispatch_groups[i].qp = &qps[i];
            dispatch_groups[i].policy = DISPATCH_MIXED_RATIO;
            dispatch_groups[i].inflight_target = g_run_config.mixed_inflight_depth;
            dispatch_groups[i].issue_count = 0;
        }
        printf("Mixed mode runs %zu hot producer threads and %zu cold producer threads over %zu shared queue pairs; each shared queue schedules requests with a 1 hot : 3 cold preference while preserving independent hot/cold arrivals. Mixed inflight=%zu per queue pair.\n",
               hot_thread_count,
               cold_thread_count,
               active_thread_count,
               g_run_config.mixed_inflight_depth);
    }
    else if (mode == MODE_HOT_ONLY)
    {
        for (i = 0; i < active_thread_count; ++i)
        {
            dispatch_groups[i].qp = &qps[i];
            dispatch_groups[i].policy = DISPATCH_FIFO;
            dispatch_groups[i].inflight_target = g_run_config.mixed_inflight_depth;
            dispatch_groups[i].issue_count = 0;
        }
        printf("Hot-only mode runs %zu hot producer threads over %zu shared queue pairs with the same dispatcher structure as mixed, but generates only hot requests. Mixed inflight=%zu per queue pair.\n",
               hot_thread_count,
               active_thread_count,
               g_run_config.mixed_inflight_depth);
    }
    else if (mode == MODE_WEAK_MIXED)
    {
        printf("Weak-mixed mode alternates hot/cold requests in the single-issue path.\n");
    }
    else if (mode == MODE_STRONG_ISOLATED)
    {
        for (i = 0; i < hot_thread_count; ++i)
        {
            dispatch_groups[i].qp = &qps[i];
            dispatch_groups[i].policy = DISPATCH_FIFO;
            dispatch_groups[i].inflight_target = g_run_config.strong_hot_inflight_depth;
            dispatch_groups[i].issue_count = 0;
        }
        for (i = hot_thread_count; i < active_thread_count; ++i)
        {
            dispatch_groups[i].qp = &qps[i];
            dispatch_groups[i].policy = DISPATCH_FIFO;
            dispatch_groups[i].inflight_target = g_run_config.strong_cold_inflight_depth;
            dispatch_groups[i].issue_count = 0;
        }
        printf("Strong-isolated mode keeps %zu hot producer threads and %zu cold producer threads, dispatching them through isolated queue pairs with hot depth=%zu and cold depth=%zu per queue pair.\n",
               hot_thread_count,
               cold_thread_count,
               g_run_config.strong_hot_inflight_depth,
               g_run_config.strong_cold_inflight_depth);
    }
    else
    {
        printf("Isolated mode reserves queue 0-3 for hot data and queue 4-15 for cold data.\n");
    }
    if (g_run_config.use_budget)
    {
        printf("Budget run: hot=%zu, cold=%zu\n", g_run_config.hot_budget, g_run_config.cold_budget);
    }
    else
    {
        printf("Timed run: %u s\n", g_run_config.duration_seconds);
    }
    printf("Queue depth: %u\n", g_run_config.queue_depth);
    printf("Hot threads: %zu | Cold threads: %zu | Active producers: %zu\n",
           hot_thread_count,
           cold_thread_count,
           active_thread_count);
    printf("Mixed inflight: %zu | Strong hot inflight: %zu | Strong cold inflight: %zu\n",
           g_run_config.mixed_inflight_depth,
           g_run_config.strong_hot_inflight_depth,
           g_run_config.strong_cold_inflight_depth);

    keep_running = true;
    run_start_ns = monotonic_time_ns();

    if (mode == MODE_MIXED || mode == MODE_STRONG_ISOLATED || mode == MODE_HOT_ONLY)
    {
        for (i = 0; i < active_thread_count; ++i)
        {
            try
            {
                dispatchers[i] = std::thread(dispatch_thread, &dispatch_groups[i], &disk);
            }
            catch (const std::system_error& e)
            {
                fprintf(stderr, "Failed to start dispatcher thread %zu: %s\n", i, e.what());
                goto out;
            }
        }
    }

    for(int i = 0; i < (int)active_thread_count; i++)
    {
        try
        {
            workers[i] = std::thread(work_thread, &work_threads[i], mode);
            any_thread_started = true;
        }
        catch (const std::system_error& e)
        {
            fprintf(stderr, "Failed to start worker thread %d: %s\n", i, e.what());
            goto out;
        }
        
    }

    if (g_run_config.use_budget)
    {
        while (!budgets_exhausted(mode))
        {
            usleep(1000);
        }
        keep_running = false;
    }
    else
    {
        sleep(g_run_config.duration_seconds);
        keep_running = false;
    }

    if (mode == MODE_MIXED || mode == MODE_STRONG_ISOLATED || mode == MODE_HOT_ONLY)
    {
        for (i = 0; i < active_thread_count; ++i)
        {
            dispatch_groups[i].can_push.notify_all();
            dispatch_groups[i].can_pop.notify_all();
        }
    }

    for(int i = 0; i < (int)active_thread_count; i++)
    {
        if(workers[i].joinable())
            workers[i].join();
    }
    if (mode == MODE_MIXED || mode == MODE_STRONG_ISOLATED || mode == MODE_HOT_ONLY)
    {
        for (i = 0; i < active_thread_count; ++i)
        {
            if (dispatchers[i].joinable())
            {
                dispatchers[i].join();
            }
        }
    }
    run_end_ns = monotonic_time_ns();
    for(i = 0; i < active_thread_count; i++)
    {
        qps[i].stop = true;
    }

    any_thread_started = false;
    elapsed_seconds = (double)(run_end_ns - run_start_ns) / 1000000000.0;
    if (elapsed_seconds <= 0.0)
    {
        elapsed_seconds = 1e-9;
    }

    for(int i = 0; i < (int)active_thread_count; i++)
    {
        total_hot_ios += stats_for_thread[i].hot_io_count;
        total_cold_ios += stats_for_thread[i].cold_io_count;

        if(mode == MODE_ISOLATED || mode == MODE_MIXED || mode == MODE_STRONG_ISOLATED)
        {
            if((size_t)i < hot_thread_count)
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
        else if (mode == MODE_HOT_ONLY)
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
        size_t report_hot_threads = (mode == MODE_HOT_ONLY) ? hot_thread_count : hot_thread_count;
        size_t report_cold_threads = (mode == MODE_HOT_ONLY) ? 0 : cold_thread_count;
        printf("Hot Threads: %zu, Cold Threads: %zu\n", report_hot_threads, report_cold_threads);
        printf("Elapsed Time: %.3f s\n", elapsed_seconds);
        printf("Total Hot Requests: %zu\n", all_hot_latencies.size());
        printf("Average Latency: %.2f us\n", avg);
        printf("P95 Tail Latency: %.2f us\n", p95);
        printf("P99 Tail Latency: %.2f us\n", p99);
        printf("Hot Throughput: %.2f MB/s\n",
               (double)(total_hot_ios * hot_piece_size) / (1024.0 * 1024.0 * elapsed_seconds));
        printf("Cold Throughput: %.2f MB/s\n",
               (double)(total_cold_ios * cold_piece_size) / (1024.0 * 1024.0 * elapsed_seconds));
        printf("Hot Thread Errors: %d\n", hot_error_count);
        printf("Cold Thread Errors: %d\n", cold_error_count);
    }

    close(fd);
    fd = -1;
    Host_file_system_exit(nvme_mount_path);
    mounted = false;
    // printf("cleanup: start remove_buffer on normal path\n");
    for (i = 0; i < MaxThreadCount; ++i)
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
    for (i = 0; i < MaxThreadCount; ++i)
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
    if (mode == MODE_MIXED || mode == MODE_STRONG_ISOLATED || mode == MODE_HOT_ONLY)
    {
        for (i = 0; i < active_thread_count; ++i)
        {
            dispatch_groups[i].can_push.notify_all();
            dispatch_groups[i].can_pop.notify_all();
        }
    }
    for (i = 0; i < active_thread_count; ++i)
    {
        qps[i].stop = true;
    }
    if (any_thread_started)
    {
        for (i = 0; i < active_thread_count; ++i)
        {
            if (workers[i].joinable())
            {
                workers[i].join();
            }
        }
    }
    if (mode == MODE_MIXED || mode == MODE_STRONG_ISOLATED || mode == MODE_HOT_ONLY)
    {
        for (i = 0; i < active_thread_count; ++i)
        {
            if (dispatchers[i].joinable())
            {
                dispatchers[i].join();
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
    for (i = 0; i < MaxThreadCount; ++i)
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
    for (i = 0; i < MaxThreadCount; ++i)
    {
        latency_log_destroy(&hot_latencies[i]);
    }
    if (ctrl != NULL)
    {
        nvm_ctrl_free(ctrl);
    }
    
    return 1;
}
