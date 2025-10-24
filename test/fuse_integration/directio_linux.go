//go:build linux

package fuse

import (
	"syscall"
)

// Direct I/O support for Linux

const (
	O_DIRECT = 0x4000 // Direct I/O flag for Linux
)

func openDirectIO(path string, flags int, mode uint32) (int, error) {
	return syscall.Open(path, flags|O_DIRECT, mode)
}

func isDirectIOSupported() bool {
	return true
}
