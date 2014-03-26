// +build !windows

package stats

import (
	"syscall"
)

type DiskStatus struct {
	Dir  string
	All  uint64
	Used uint64
	Free uint64
}

func NewDiskStatus(path string) (disk *DiskStatus) {
	disk = &DiskStatus{Dir: path}
	fs := syscall.Statfs_t{}
	err := syscall.Statfs(path, &fs)
	if err != nil {
		return
	}
	disk.All = fs.Blocks * uint64(fs.Bsize)
	disk.Free = fs.Bfree * uint64(fs.Bsize)
	disk.Used = disk.All - disk.Free
	return
}
