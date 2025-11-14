//go:build darwin

package fuse

import (
	"syscall"
	"unsafe"
)

// Sendfile support for macOS

func sendfileTransfer(outFd int, inFd int, offset *int64, count int) (int, error) {
	// macOS sendfile has different signature: sendfile(in_fd, out_fd, offset, len, hdtr, flags)
	var off int64
	if offset != nil {
		off = *offset
	}

	length := int64(count)

	_, _, errno := syscall.Syscall6(syscall.SYS_SENDFILE,
		uintptr(inFd),                    // input fd
		uintptr(outFd),                   // output fd
		uintptr(off),                     // offset
		uintptr(unsafe.Pointer(&length)), // length (in/out parameter)
		0,                                // hdtr (headers/trailers)
		0)                                // flags

	if errno != 0 {
		return 0, errno
	}

	// Update offset if provided
	if offset != nil {
		*offset += length
	}

	return int(length), nil
}

func isSendfileSupported() bool {
	return true
}
