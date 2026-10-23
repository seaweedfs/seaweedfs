//go:build unix

package fuse

import (
	"syscall"

	"golang.org/x/sys/unix"
)

// Memory mapping support for Unix-like systems

func mmapFile(fd int, offset int64, length int, prot int, flags int) ([]byte, error) {
	return unix.Mmap(fd, offset, length, prot, flags)
}

func munmapFile(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	return unix.Munmap(data)
}

func msyncFile(data []byte, flags int) error {
	if len(data) == 0 {
		return nil
	}
	return unix.Msync(data, flags)
}

func isMmapSupported() bool {
	return true
}

// Memory protection flags
const (
	PROT_READ  = syscall.PROT_READ
	PROT_WRITE = syscall.PROT_WRITE
	PROT_EXEC  = syscall.PROT_EXEC
	PROT_NONE  = syscall.PROT_NONE
)

// Memory mapping flags
const (
	MAP_SHARED    = syscall.MAP_SHARED
	MAP_PRIVATE   = syscall.MAP_PRIVATE
	MAP_ANONYMOUS = syscall.MAP_ANON
)

// Memory sync flags
const (
	MS_ASYNC      = syscall.MS_ASYNC
	MS_SYNC       = syscall.MS_SYNC
	MS_INVALIDATE = syscall.MS_INVALIDATE
)
