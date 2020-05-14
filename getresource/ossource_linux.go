package getresource

/*
#include <stdio.h>
#include <fcntl.h>
#include <linux/fs.h>

long long getdiskcap(char *path)
{
    int fd;
    //off_t size
    unsigned long long size;
    int len;
    int r;

    if ((fd = open(path, O_RDONLY)) < 0)
    {
        printf("open error %d\n");
        return -1;
    }

    if ((r = ioctl(fd, BLKGETSIZE64, &size)) < 0)
    {
        printf("ioctl error \n");
        return -1;
    }

    len = (size>>30);
    printf("size of sda = %d G, size=%ld\n", len, size);
    return size;
}
*/
import "C"

func GetDiskCap(path string) int64 {
	diskSize_byte := C.getdiskcap(C.string(path))
	return diskSize_byte
}