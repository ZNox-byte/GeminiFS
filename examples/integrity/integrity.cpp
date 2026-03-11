#include <nvm_types.h>
#include <nvm_ctrl.h>
#include <nvm_dma.h>
#include <nvm_aq.h>
#include <nvm_error.h>
#include <stdio.h>
#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>
#include <stdbool.h>
#include <getopt.h>
#include <string.h>
#include <errno.h>


#include <fcntl.h>
#include <unistd.h>

#include "get-offset/get-offset.h"
#include "integrity.h"
#include "read.h"
#include "../../src/file.h"

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

#define snvme_control_path "/dev/snvm_control"
#define snvme_path "/dev/csnvme1"
#define nvme_dev_path "/dev/snvme0n1"
#define snvme_helper_path "/dev/snvme_helper"
#define nvme_mount_path "/mnt/nvm_mount"
#define file_name "/mnt/nvm_mount/test.data"
#define nvme_pci_addr {0xc3, 0, 0}

int main(int argc, char** argv)
{
    nvm_ctrl_t* ctrl;                   //NVMe控制器指针
    int status,ret;                     //报错变量
    struct disk disk;                   //磁盘信息结构体
    struct buffer buffer;               //缓冲区结构体，包含虚拟地址和DMA映射信息
    int snvme_c_fd,snvme_d_fd;          //文件描述符，分别用于控制器和设备的操作
    // Parse command line arguments
    int *buffer2;                       //用于打印读取结果的缓冲区指针
    int read_bytes;                     //读取字节数，设置为64KB
    read_bytes = 1024*64;               //读取字节数，设置为64KB
    void *buf__host = NULL;             //主机内存缓冲区指针，用于存储要写入文件的数据
    int *buf__host_int = NULL;          //主机内存缓冲区指针，类型为整数指针，用于存储要写入文件的数据
    int fd;                             //文件描述符，用于操作文件
    struct queue_pair qp;               //队列对结构体，包含一个提交队列和一个完成队列，以及一些状态信息
    struct file_info read_info;         //文件信息结构体，包含读取操作的参数，如队列大小、块大小、命名空间ID、块数量、偏移量等
    int snvme_helper_fd;                //文件描述符，用于与snvme_helper设备进行通信，获取NVMe偏移地址等信息
    struct nds_mapping mapping;         //NDS映射结构体，包含文件描述符、偏移量、长度、分配长度、地址、版本、块位和存在标志等信息，用于获取文件在磁盘上的物理地址等信息
    uint64_t nvme_ofst;                 //NVMe偏移地址，表示文件在NVMe设备上的物理地址，用于直接访问文件数据

    //打开控制器设备文件，获取文件描述符
    snvme_c_fd = open(snvme_control_path, O_RDWR); 

    if (snvme_c_fd < 0)
    {

        fprintf(stderr, "Failed to open device file: %s\n", strerror(errno));
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
    close(snvme_helper_fd);
    printf("nvme_ofst is %lx,block size is %u\n",nvme_ofst,read_bytes);

    qp.cq = &ctrl->queues[0];
    qp.sq = &ctrl->queues[ctrl->cq_num];
    qp.stop = false;                        //标志位，表示是否停止队列对的操作，初始值为false，表示继续操作
    qp.num_cpls = 0;                        //完成的命令数量，初始值为0，用于统计已经完成的NVMe命令数量
    printf("using cq is %u, sq is %u\n",qp.cq->queue.no,qp.sq->queue.no);
    read_info.offset = nvme_ofst >> 9 ;     //读取偏移，单位是块（block），通过将字节偏移右移9位（相当于除以512）来转换为块偏移，因为NVMe设备通常以512字节为一个块进行寻址
    read_info.num_blocks = 4096 >> 9;       //读取块数量，表示要读取的块数量，通过将字节数右移9位（相当于除以512）来转换为块数量，因为NVMe设备通常以512字节为一个块进行寻址
    printf("offset is %lx, block num is %u\n",read_info.offset,read_info.num_blocks);
    for(int  i = 0; i < 1; i++)
        status = read_and_dump(&disk,&qp,buffer.dma,&read_info); //调用read_and_dump函数，执行读取操作，并将结果打印出来
    
    printf("disk_read ret is %d\n",status);
    buffer2 = (int *)buffer.buffer;
    for (int i = 0; i < 256; i++) {  
        printf("%02X ", buffer2[i]); 
        if ((i + 1) % 16 == 0) {  
            printf("\n"); 
        }  
    } 
    qp.stop = true;
    close(fd);
out:
    ret = Host_file_system_exit(nvme_dev_path);
    
    if(ret < 0)
        exit(-1);
    nvm_ctrl_free(ctrl);
    exit(status);
}
