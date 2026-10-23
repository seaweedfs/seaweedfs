//go:build linux

package fuse

import (
	"syscall"
)

// File allocation support for Linux

const (
	// Fallocate flags
	FALLOC_FL_KEEP_SIZE      = 0x01
	FALLOC_FL_PUNCH_HOLE     = 0x02
	FALLOC_FL_NO_HIDE_STALE  = 0x04
	FALLOC_FL_COLLAPSE_RANGE = 0x08
	FALLOC_FL_ZERO_RANGE     = 0x10
	FALLOC_FL_INSERT_RANGE   = 0x20
	FALLOC_FL_UNSHARE_RANGE  = 0x40
)

func fallocateFile(fd int, mode int, offset int64, length int64) error {
	_, _, errno := syscall.Syscall6(syscall.SYS_FALLOCATE,
		uintptr(fd),
		uintptr(mode),
		uintptr(offset),
		uintptr(length),
		0, 0)

	if errno != 0 {
		return errno
	}
	return nil
}

func isFallocateSupported() bool {
	return true
}
