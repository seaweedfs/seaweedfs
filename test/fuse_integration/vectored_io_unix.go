//go:build unix

package fuse

import (
	"syscall"
	"unsafe"
)

// Vectored I/O support for Unix-like systems

// IOVec represents an I/O vector for readv/writev operations
type IOVec struct {
	Base *byte
	Len  uint64
}

func readvFile(fd int, iovs []IOVec) (int, error) {
	if len(iovs) == 0 {
		return 0, nil
	}

	n, _, errno := syscall.Syscall(syscall.SYS_READV,
		uintptr(fd),
		uintptr(unsafe.Pointer(&iovs[0])),
		uintptr(len(iovs)))

	if errno != 0 {
		return 0, errno
	}
	return int(n), nil
}

func writevFile(fd int, iovs []IOVec) (int, error) {
	if len(iovs) == 0 {
		return 0, nil
	}

	n, _, errno := syscall.Syscall(syscall.SYS_WRITEV,
		uintptr(fd),
		uintptr(unsafe.Pointer(&iovs[0])),
		uintptr(len(iovs)))

	if errno != 0 {
		return 0, errno
	}
	return int(n), nil
}

func preadvFile(fd int, iovs []IOVec, offset int64) (int, error) {
	if len(iovs) == 0 {
		return 0, nil
	}

	// preadv/pwritev may not be available on all Unix systems
	// Fall back to individual pread calls
	totalRead := 0
	currentOffset := offset

	for _, iov := range iovs {
		if iov.Len == 0 {
			continue
		}

		buf := (*[1 << 30]byte)(unsafe.Pointer(iov.Base))[:iov.Len:iov.Len]
		n, err := syscall.Pread(fd, buf, currentOffset)
		if err != nil {
			return totalRead, err
		}
		totalRead += n
		currentOffset += int64(n)

		if n < int(iov.Len) {
			break // EOF or partial read
		}
	}

	return totalRead, nil
}

func pwritevFile(fd int, iovs []IOVec, offset int64) (int, error) {
	if len(iovs) == 0 {
		return 0, nil
	}

	// preadv/pwritev may not be available on all Unix systems
	// Fall back to individual pwrite calls
	totalWritten := 0
	currentOffset := offset

	for _, iov := range iovs {
		if iov.Len == 0 {
			continue
		}

		buf := (*[1 << 30]byte)(unsafe.Pointer(iov.Base))[:iov.Len:iov.Len]
		n, err := syscall.Pwrite(fd, buf, currentOffset)
		if err != nil {
			return totalWritten, err
		}
		totalWritten += n
		currentOffset += int64(n)

		if n < int(iov.Len) {
			break // Partial write
		}
	}

	return totalWritten, nil
}

// Helper function to create IOVec from byte slices
func makeIOVecs(buffers [][]byte) []IOVec {
	iovs := make([]IOVec, len(buffers))
	for i, buf := range buffers {
		if len(buf) > 0 {
			iovs[i].Base = &buf[0]
			iovs[i].Len = uint64(len(buf))
		}
	}
	return iovs
}

func isVectoredIOSupported() bool {
	return true
}
