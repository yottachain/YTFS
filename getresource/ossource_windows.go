package getresource

import "unsafe"
import (
	"fmt"
	"github.com/yottachain/YTFS/storage"
	"golang.org/x/sys/windows"
)

type DiskStatus struct {
	All  uint64
	Used uint64
	Free uint64
}

func GetDiskCap(stor storage.Storage) uint64 {
	path := stor.GetFd().Path

	disk := &DiskStatus{}
	h := windows.MustLoadDLL("kernel32.dll")
	c := h.MustFindProc("GetDiskFreeSpaceExW")
	lpFreeBytesAvailable := uint64(0)
	lpTotalNumberOfBytes := uint64(0)
	lpTotalNumberOfFreeBytes := uint64(0)
	c.Call(uintptr(unsafe.Pointer(windows.StringToUTF16Ptr(path))),
		uintptr(unsafe.Pointer(&lpFreeBytesAvailable)),
		uintptr(unsafe.Pointer(&lpTotalNumberOfBytes)),
		uintptr(unsafe.Pointer(&lpTotalNumberOfFreeBytes)))
	disk.All = lpTotalNumberOfBytes
	disk.Free = lpTotalNumberOfFreeBytes
	disk.Used = lpFreeBytesAvailable
	fmt.Println("")
	return uint64(0)
}
