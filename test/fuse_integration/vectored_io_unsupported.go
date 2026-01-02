//go:build !unix

package fuse

import (
	"errors"
)

// Vectored I/O support for unsupported platforms

var ErrVectoredIONotSupported = errors.New("vectored I/O not supported on this platform")

// IOVec represents an I/O vector for readv/writev operations
type IOVec struct {
	Base *byte
	Len  uint64
}

func readvFile(fd int, iovs []IOVec) (int, error) {
	return 0, ErrVectoredIONotSupported
}

func writevFile(fd int, iovs []IOVec) (int, error) {
	return 0, ErrVectoredIONotSupported
}

func preadvFile(fd int, iovs []IOVec, offset int64) (int, error) {
	return 0, ErrVectoredIONotSupported
}

func pwritevFile(fd int, iovs []IOVec, offset int64) (int, error) {
	return 0, ErrVectoredIONotSupported
}

func makeIOVecs(buffers [][]byte) []IOVec {
	return nil
}

func isVectoredIOSupported() bool {
	return false
}
