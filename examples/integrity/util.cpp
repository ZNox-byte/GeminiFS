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
#include "integrity.h"


int create_buffer(struct buffer* b, nvm_ctrl_t* ctrl, size_t size,int is_cq, int ioq_idx)
{
    int status;
    //页对齐分配内存
    status = posix_memalign(&b->buffer, ctrl->page_size, size);
    if (status != 0)
    {
        fprintf(stderr, "Failed to allocate memory: %s\n", strerror(status));
        return status;
    }
    //DMA映射，获取物理地址
    status = nvm_dma_map_host(&b->dma, ctrl, b->buffer, size, is_cq, ioq_idx);

    if (!nvm_ok(status))
    {
        free(b->buffer);
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
    nvm_dma_unmap(b->dma);
    // fprintf(stderr, "remove_buffer: done nvm_dma_unmap\n");
    // fprintf(stderr, "remove_buffer: start free host buffer\n");
    free(b->buffer);
    // fprintf(stderr, "remove_buffer: done free host buffer\n");
}


int create_queue(struct queue* q, nvm_ctrl_t* ctrl, const struct queue* cq, uint16_t qno)
{
    int status;

    int is_cq;
    size_t qmem_size;
    
    is_cq = 1;
    if (cq != NULL)
    {
        size_t sq_pages;

        is_cq = 0;
        sq_pages = NVM_SQ_PAGES(ctrl, ctrl->qs);
        qmem_size = (sq_pages + ctrl->qs) * ctrl->page_size;
    }
    else
        qmem_size =  ctrl->qs * sizeof(nvm_cpl_t);
    
    status = create_buffer(&q->qmem, ctrl, qmem_size,is_cq,qno);
    if (!nvm_ok(status))
    {
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
