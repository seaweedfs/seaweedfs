//go:build linux

package fuse

import (
	"syscall"
	"unsafe"
)

// Sendfile support for Linux

func sendfileTransfer(outFd int, inFd int, offset *int64, count int) (int, error) {
	var offsetPtr uintptr
	if offset != nil {
		offsetPtr = uintptr(unsafe.Pointer(offset))
	}

	n, _, errno := syscall.Syscall6(syscall.SYS_SENDFILE,
		uintptr(outFd),
		uintptr(inFd),
		offsetPtr,
		uintptr(count),
		0, 0)

	if errno != 0 {
		return 0, errno
	}
	return int(n), nil
}

func isSendfileSupported() bool {
	return true
}
