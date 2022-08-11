//go:build linux
// +build linux

package backend

import (
	"syscall"
)

// Using Fdatasync to optimize file sync operation
func (df *DiskFile) Sync() error {
	fd := df.File.Fd()
	return syscall.Fdatasync(int(fd))
}
