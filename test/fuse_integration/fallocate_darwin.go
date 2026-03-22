//go:build darwin

package fuse

import (
	"syscall"
	"unsafe"
)

// File allocation support for macOS using fcntl

const (
	// macOS doesn't have fallocate, but we can use fcntl with F_PREALLOCATE
	F_ALLOCATECONTIG = 0x02
	F_ALLOCATEALL    = 0x04

	// Dummy flags for compatibility
	FALLOC_FL_KEEP_SIZE      = 0x01
	FALLOC_FL_PUNCH_HOLE     = 0x02
	FALLOC_FL_NO_HIDE_STALE  = 0x04
	FALLOC_FL_COLLAPSE_RANGE = 0x08
	FALLOC_FL_ZERO_RANGE     = 0x10
	FALLOC_FL_INSERT_RANGE   = 0x20
	FALLOC_FL_UNSHARE_RANGE  = 0x40
)

// fstore_t structure for F_PREALLOCATE
type fstore struct {
	flags      uint32
	posmode    int16
	offset     int64
	length     int64
	bytesalloc int64
}

func fallocateFile(fd int, mode int, offset int64, length int64) error {
	// Check for unsupported modes on macOS
	unsupportedModes := FALLOC_FL_PUNCH_HOLE | FALLOC_FL_NO_HIDE_STALE | FALLOC_FL_COLLAPSE_RANGE | FALLOC_FL_ZERO_RANGE | FALLOC_FL_INSERT_RANGE | FALLOC_FL_UNSHARE_RANGE
	if mode&unsupportedModes != 0 {
		return syscall.ENOTSUP // Operation not supported
	}

	// On macOS, we use fcntl with F_PREALLOCATE
	store := fstore{
		flags:   F_ALLOCATECONTIG,
		posmode: syscall.F_PEOFPOSMODE, // Allocate from EOF
		offset:  0,
		length:  length,
	}

	_, _, errno := syscall.Syscall(syscall.SYS_FCNTL,
		uintptr(fd),
		syscall.F_PREALLOCATE,
		uintptr(unsafe.Pointer(&store)))

	if errno != 0 {
		// If contiguous allocation fails, try non-contiguous
		store.flags = F_ALLOCATEALL
		_, _, errno = syscall.Syscall(syscall.SYS_FCNTL,
			uintptr(fd),
			syscall.F_PREALLOCATE,
			uintptr(unsafe.Pointer(&store)))

		if errno != 0 {
			return errno
		}
	}

	// Set file size if not keeping size
	if mode&FALLOC_FL_KEEP_SIZE == 0 {
		return syscall.Ftruncate(fd, offset+length)
	}

	return nil
}

func isFallocateSupported() bool {
	return true
}
