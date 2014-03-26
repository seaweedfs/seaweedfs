// +build windows

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
	return
}
