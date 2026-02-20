//go:build !linux && !darwin

package fuse

import (
	"errors"
	"syscall"
)

// Direct I/O support for unsupported platforms

var ErrDirectIONotSupported = errors.New("direct I/O not supported on this platform")

const (
	O_DIRECT = 0x4000 // Dummy flag for compatibility
)

func openDirectIO(path string, flags int, mode uint32) (int, error) {
	// Fall back to regular open
	fd, err := syscall.Open(path, flags, mode)
	if err != nil {
		return -1, err
	}
	return int(fd), nil
}

func isDirectIOSupported() bool {
	return false
}
