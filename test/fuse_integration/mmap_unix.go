//go:build unix

package fuse

import (
	"syscall"
	"unsafe"
)

// Memory mapping support for Unix-like systems

func mmapFile(fd int, offset int64, length int, prot int, flags int) ([]byte, error) {
	addr, _, errno := syscall.Syscall6(syscall.SYS_MMAP,
		0, // addr (let kernel choose)
		uintptr(length),
		uintptr(prot),
		uintptr(flags),
		uintptr(fd),
		uintptr(offset))

	if errno != 0 {
		return nil, errno
	}

	// Convert the address to a byte slice
	return (*[1 << 30]byte)(unsafe.Pointer(addr))[:length:length], nil
}

func munmapFile(data []byte) error {
	if len(data) == 0 {
		return nil
	}

	_, _, errno := syscall.Syscall(syscall.SYS_MUNMAP,
		uintptr(unsafe.Pointer(&data[0])),
		uintptr(len(data)),
		0)

	if errno != 0 {
		return errno
	}
	return nil
}

func msyncFile(data []byte, flags int) error {
	if len(data) == 0 {
		return nil
	}

	_, _, errno := syscall.Syscall(syscall.SYS_MSYNC,
		uintptr(unsafe.Pointer(&data[0])),
		uintptr(len(data)),
		uintptr(flags))

	if errno != 0 {
		return errno
	}
	return nil
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
