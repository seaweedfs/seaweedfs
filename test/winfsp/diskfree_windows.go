package winfsp

import (
	"golang.org/x/sys/windows"
)

func getDiskFreeSpace(path string, free, total, totalFree *uint64) error {
	p, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return err
	}
	return windows.GetDiskFreeSpaceEx(p, free, total, totalFree)
}
