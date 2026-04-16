#include <nvm_ctrl.h>
#include <nvm_dma.h>
#include <nvm_error.h>
#include <stdio.h>
#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <nvm_util.h>
#include <sys/mman.h>
#include <stdint.h>
#include "integrity.h"

static constexpr size_t QueueHugePageSize = 2 * 1024 * 1024;

static bool dma_prefix_is_contiguous(const nvm_dma_t* dma, size_t page_count)
{
    if (dma == NULL || page_count == 0 || dma->n_ioaddrs < page_count)
    {
        return false;
    }

    for (size_t i = 1; i < page_count; ++i)
    {
        if (dma->ioaddrs[i - 1] + dma->page_size != dma->ioaddrs[i])
        {
            return false;
        }
    }

    return true;
}

static void* map_aligned_region(size_t size, size_t alignment)
{
    size_t reserve_size = size + alignment;
    void* reserve = mmap(NULL,
                         reserve_size,
                         PROT_READ | PROT_WRITE,
                         MAP_PRIVATE | MAP_ANONYMOUS,
                         -1,
                         0);
    if (reserve == MAP_FAILED)
    {
        return MAP_FAILED;
    }

    uintptr_t base = (uintptr_t)reserve;
    uintptr_t aligned = (base + alignment - 1) & ~(uintptr_t)(alignment - 1);
    size_t prefix = aligned - base;
    size_t suffix = reserve_size - prefix - size;

    if (prefix > 0)
    {
        munmap((void*)base, prefix);
    }
    if (suffix > 0)
    {
        munmap((void*)(aligned + size), suffix);
    }

    return (void*)aligned;
}


int create_buffer(struct buffer* b, nvm_ctrl_t* ctrl, size_t size,int is_cq, int ioq_idx)
{
    int status;
    size_t alloc_size = NVM_PAGE_ALIGN(size, ctrl->page_size);
    size_t map_size = alloc_size;

    if (ioq_idx >= 0 && alloc_size < QueueHugePageSize)
    {
        map_size = QueueHugePageSize;
        b->buffer = map_aligned_region(map_size, QueueHugePageSize);
    }
    else
    {
        b->buffer = mmap(NULL, map_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    }

    if (b->buffer == MAP_FAILED)
    {
        b->buffer = NULL;
        return errno;
    }

#ifdef MADV_HUGEPAGE
    if (ioq_idx >= 0)
    {
        madvise(b->buffer, map_size, MADV_HUGEPAGE);
    }
#endif

    // Fault the pages in before DMA mapping so multi-page queue memory has a
    // better chance of landing in a contiguous large page.
    memset(b->buffer, 0, map_size);

    //DMA映射，获取物理地址
    status = nvm_dma_map_host(&b->dma, ctrl, b->buffer, map_size, is_cq, ioq_idx);

    if (!nvm_ok(status))
    {
        munmap(b->buffer, map_size);
        b->buffer = NULL;
        fprintf(stderr, "Failed to create local segment: %s\n", nvm_strerror(status));
        return status;
    }
    //初始化内存
    memset(b->dma->vaddr, 0, b->dma->page_size * b->dma->n_ioaddrs);

    return 0;
}


void remove_buffer(struct buffer* b)
{
    // fprintf(stderr, "remove_buffer: dma=%p buffer=%p\n", (void*)b->dma, b->buffer);
    // fprintf(stderr, "remove_buffer: start nvm_dma_unmap\n");
    size_t alloc_size = 0;
    if(b->dma != NULL)
    {
        alloc_size = b->dma->page_size * b->dma->n_ioaddrs;
    }
    nvm_dma_unmap(b->dma);
    // fprintf(stderr, "remove_buffer: done nvm_dma_unmap\n");
    // fprintf(stderr, "remove_buffer: start free host buffer\n");
    if(b->buffer != NULL && alloc_size > 0)
    {
        munmap(b->buffer, alloc_size);
    }
    b->buffer = NULL;
    b->dma = NULL;
    // fprintf(stderr, "remove_buffer: done free host buffer\n");
}


int create_queue(struct queue* q, nvm_ctrl_t* ctrl, const struct queue* cq, uint16_t qno)
{
    int status;

    int is_cq;
    size_t qmem_size;
    size_t sq_pages = 0;
    
    is_cq = 1;
    if (cq != NULL)
    {
        is_cq = 0;
        sq_pages = NVM_SQ_PAGES(ctrl, ctrl->qs);
        qmem_size = (sq_pages + ctrl->qs) * ctrl->page_size;
    }
    else
        qmem_size =  ctrl->qs * sizeof(nvm_cpl_t);

    // Multi-page SQs require the queue pages themselves to be physically
    // contiguous. Retry a few times so larger depths like 128 have a chance
    // to land inside a single huge page instead of failing later with
    // malformed-command completions.
    for (int attempt = 0; attempt < 8; ++attempt)
    {
        status = create_buffer(&q->qmem, ctrl, qmem_size, is_cq, qno);
        if (!nvm_ok(status))
        {
            return status;
        }

        if (is_cq || sq_pages <= 1 || dma_prefix_is_contiguous(q->qmem.dma, sq_pages))
        {
            break;
        }

        remove_buffer(&q->qmem);
        status = EFAULT;
    }

    if (!nvm_ok(status))
    {
        fprintf(stderr,
                "Failed to create queue %u: SQ queue pages are not physically contiguous for depth=%u (sq_pages=%zu).\n",
                qno,
                ctrl->qs,
                sq_pages);
        return status;
    }

    if (!nvm_ok(status))
    {
        remove_buffer(&q->qmem);
        fprintf(stderr, "Failed to create queue: %s\n", nvm_strerror(status));
        return status;
    }

    q->counter = 0;
    return 0;
}


void remove_queue(struct queue* q)
{
    remove_buffer(&q->qmem);
}
