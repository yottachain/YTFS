package getresource

import (
	"syscall"
)

type DiskStatus struct {
	All  uint64 `json:"all"`
	Used uint64 `json:"used"`
	Free uint64 `json:"free"`
}

func GetDiskCap(path string) uint64 {
	fs := syscall.Statfs_t{}
	err := syscall.Statfs(path, &fs)
	if err != nil {
		return uint64(0)
	}
	diskAllCap := fs.Blocks * uint64(fs.Bsize)
	//	disk.Free = fs.Bfree * uint64(fs.Bsize)
	//	disk.Used = disk.All - disk.Free
	return diskAllCap
}
