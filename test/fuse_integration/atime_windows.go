//go:build windows

package fuse

import (
	"syscall"
)

// getAtimeNano returns the access time in nanoseconds for Windows systems
func getAtimeNano(stat *syscall.Win32FileAttributeData) int64 {
	return stat.LastAccessTime.Nanoseconds()
}
