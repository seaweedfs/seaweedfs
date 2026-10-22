//go:build linux

package fuse

import (
	"syscall"

	"golang.org/x/sys/unix"
)

// Direct I/O support for Linux

const (
	O_DIRECT = unix.O_DIRECT
)

func openDirectIO(path string, flags int, mode uint32) (int, error) {
	return syscall.Open(path, flags|O_DIRECT, mode)
}

func isDirectIOSupported() bool {
	return true
}
