//go:build darwin

package fuse

import (
	"syscall"
)

// Direct I/O support for macOS

const (
	// macOS doesn't have O_DIRECT, but we can use fcntl with F_NOCACHE
	F_NOCACHE = 48
)

func openDirectIO(path string, flags int, mode uint32) (int, error) {
	// Open file normally first
	fd, err := syscall.Open(path, flags, mode)
	if err != nil {
		return -1, err
	}

	// Set F_NOCACHE to bypass buffer cache (similar to O_DIRECT)
	_, _, errno := syscall.Syscall(syscall.SYS_FCNTL,
		uintptr(fd),
		F_NOCACHE,
		1) // enable

	if errno != 0 {
		syscall.Close(fd)
		return -1, errno
	}

	return fd, nil
}

func isDirectIOSupported() bool {
	return true
}
