#include "read.h"
#include <nvm_types.h>
#include <nvm_util.h>
#include <nvm_queue.h>
#include <nvm_cmd.h>
#include <nvm_error.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <time.h>

#define MIN(a, b) ((a) <= (b) ? (a) : (b))

//统计时间
uint64_t timediff_us(struct timespec* start, struct timespec* end) {
    return (uint64_t)(end->tv_sec - start->tv_sec) * 1000000 + (end->tv_nsec-start->tv_nsec) / 1000;
}

//打印I/O统计信息
void print_stats(struct timespec* start, struct timespec* end, size_t bytes) {
        uint64_t diff = timediff_us(start, end);
        fprintf(stderr, "Done in %lldus, %fMB/s\n", (unsigned long long) diff, (double)bytes/(double)diff);
}


//轮询NVMe完成队列
static void* consume_completions(struct queue_pair* qp)
{
    nvm_cpl_t* cpl;

    qp->stop = false;
    qp->num_cpls = 0;
    nvm_queue_t* cq;
    nvm_queue_t* sq;
    cq = &qp->cq->queue;
    sq = &qp->sq->queue;


    while (!qp->stop)
    {
        if ((cpl = nvm_cq_dequeue_block(cq, 100)) == NULL)
        {
            usleep(1);
            continue;
        }
        nvm_sq_update(sq);
        if (!NVM_ERR_OK(cpl))
        {
            fprintf(stderr, "%s\n", nvm_strerror(NVM_ERR_STATUS(cpl)));
        }
        usleep(1);
        nvm_cq_update(cq);
        qp->num_cpls++;
    }

    return NULL;
}

//实际构建 I/O 请求， 计算本次 I/O 需要处理的页面数量和数据块
static size_t rw_bytes(const struct disk* disk, struct queue_pair* qp, const nvm_dma_t* buffer, uint64_t* blk_offset, size_t* size_remaining, uint8_t op)
{
    // Read blocks
    size_t page = 0;
    size_t num_cmds = 0;
    size_t num_pages = disk->max_data_size / disk->page_size;
    size_t chunk_pages = MIN(buffer->n_ioaddrs, NVM_PAGE_ALIGN(*size_remaining, disk->page_size) / disk->page_size);
    size_t offset = *blk_offset;
    nvm_prp_list_t list;
    nvm_queue_t* sq;
    sq = &qp->sq->queue;
    
    while (page < chunk_pages)
    {
        num_pages = MIN(buffer->n_ioaddrs - page, num_pages);

        nvm_cmd_t* cmd;
        //入队请求
        while ((cmd = nvm_sq_enqueue(sq)) == NULL)
        {
            printf("rw_bytes submit 0");
            nvm_sq_submit(sq);
            usleep(1);
        }
        //NVMe请求封装
        uint16_t prp_list = num_cmds % sq->qs;
        size_t num_blocks = NVM_PAGE_TO_BLOCK(disk->page_size, disk->block_size, num_pages);
        size_t start_block = offset + NVM_PAGE_TO_BLOCK(disk->page_size, disk->block_size, page);
        nvm_cmd_header(cmd, NVM_DEFAULT_CID(sq), op, disk->ns_id);
        list = NVM_PRP_LIST(qp->sq->qmem.dma, NVM_SQ_PAGES(qp->sq->qmem.dma, sq->qs) + prp_list);
        page += nvm_cmd_data(cmd, 1, &list, num_pages, &buffer->ioaddrs[page]);
        nvm_cmd_rw_blks(cmd, start_block, num_blocks);
        ++num_cmds;
        usleep(1);
    }
   //提交入队命令
    nvm_sq_submit(sq);
    *blk_offset = offset + NVM_PAGE_TO_BLOCK(disk->page_size, disk->block_size, page);
    *size_remaining -= MIN(*size_remaining, chunk_pages * disk->page_size);
    return num_cmds;
}

//协调前面所有底层组件，将一个巨大的读取请求切分成一块块的 NVMe 命令，并发射给固态硬盘，同时通过多线程来监听结果，最后统计读写速度。
int read_and_dump(const struct disk* disk, struct queue_pair* qp, const nvm_dma_t* buffer, const struct file_info* args)
{
    int status;
    pthread_t completer;
    struct timespec start, end;

    // Start consuming
    //这里是消费者，轮询完成队列，是另一个线程。
    status = pthread_create(&completer, NULL, (void *(*)(void*)) consume_completions, qp);
    if (status != 0)
    {
        fprintf(stderr, "Could not start completer thread\n");
        return status;
    }
    
    // Clear all PRP lists
    size_t sq_pages = NVM_SQ_PAGES(qp->sq->qmem.dma, qp->sq->queue.qs);
    memset(NVM_DMA_OFFSET(qp->sq->qmem.dma, sq_pages), 0, qp->sq->qmem.dma->page_size * (qp->sq->qmem.dma->n_ioaddrs - sq_pages));

    size_t num_cmds = 0;
    uint64_t start_block = args->offset ;
    size_t size_remaining = args->num_blocks * disk->block_size;
    while (size_remaining != 0)
    {
        fprintf(stderr, "Reading %zu bytes [%zu MB] (total=%zu)\n", 
                buffer->n_ioaddrs * disk->page_size, 
                (buffer->n_ioaddrs * disk->page_size) >> 20,
                args->num_blocks * disk->block_size - size_remaining);
        size_t remaining = size_remaining;

        clock_gettime(CLOCK_MONOTONIC, &start);

        num_cmds += rw_bytes(disk, qp, buffer, &start_block, &size_remaining, NVM_IO_READ);
        while (qp->num_cpls < num_cmds)
        {
            usleep(1);
        }
        clock_gettime(CLOCK_MONOTONIC, &end);

        print_stats(&start, &end, remaining - size_remaining);

    }

    // Wait for completions
    qp->stop = true;
    pthread_join(completer, NULL);

    return 0;
}



int pure_read(const struct disk* disk, struct queue_pair* qp, const nvm_dma_t* buffer, const struct file_info* args)
{
    uint64_t start_block = args->offset;
    size_t size_remaining = args->num_blocks * disk->block_size;
    size_t num_cmds = 0;
    struct timespec wait_start;
    struct timespec wait_now;
    uint64_t waited_us;
    size_t empty_polls = 0;

    size_t sq_pages = NVM_SQ_PAGES(qp->sq->qmem.dma, qp->sq->queue.qs);
    memset(NVM_DMA_OFFSET(qp->sq->qmem.dma, sq_pages), 0, qp->sq->qmem.dma->page_size * (qp->sq->qmem.dma->n_ioaddrs - sq_pages));

    num_cmds += rw_bytes(disk, qp, buffer, &start_block, &size_remaining, NVM_IO_READ);

    nvm_queue_t* cq = &qp->cq->queue;
    nvm_queue_t* sq = &qp->sq->queue;
    size_t completed = 0;
    nvm_cpl_t* cpl;

    clock_gettime(CLOCK_MONOTONIC, &wait_start);

    while(completed < num_cmds)
    {
        if (qp->stop)
        {
            return ECANCELED;
        }

        if((cpl = nvm_cq_dequeue_block(cq, 100)) == NULL)
        {
            ++empty_polls;
            if ((empty_polls % 10000) == 0)
            {
                clock_gettime(CLOCK_MONOTONIC, &wait_now);
                waited_us = timediff_us(&wait_start, &wait_now);
                fprintf(stderr,
                        "pure_read waiting: CQ=%u SQ=%u start_block=%lu num_cmds=%zu completed=%zu waited=%lluus\n",
                        qp->cq->queue.no,
                        qp->sq->queue.no,
                        start_block,
                        num_cmds,
                        completed,
                        (unsigned long long)waited_us);
            }

            clock_gettime(CLOCK_MONOTONIC, &wait_now);
            waited_us = timediff_us(&wait_start, &wait_now);
            if (waited_us > 5000000ULL)
            {
                fprintf(stderr,
                        "pure_read timeout: CQ=%u SQ=%u start_block=%lu num_cmds=%zu completed=%zu waited=%lluus\n",
                        qp->cq->queue.no,
                        qp->sq->queue.no,
                        start_block,
                        num_cmds,
                        completed,
                        (unsigned long long)waited_us);
                return ETIMEDOUT;
            }
            continue;
        }

        nvm_sq_update(sq);

        nvm_cq_update(cq);
        completed++;
    }

    return 0;
}
