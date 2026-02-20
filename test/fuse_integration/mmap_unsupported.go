//go:build !unix

package fuse

import (
	"errors"
)

// Memory mapping support for unsupported platforms

var ErrMmapNotSupported = errors.New("memory mapping not supported on this platform")

func mmapFile(fd int, offset int64, length int, prot int, flags int) ([]byte, error) {
	return nil, ErrMmapNotSupported
}

func munmapFile(data []byte) error {
	return ErrMmapNotSupported
}

func msyncFile(data []byte, flags int) error {
	return ErrMmapNotSupported
}

func isMmapSupported() bool {
	return false
}

// Dummy constants for unsupported platforms
const (
	PROT_READ  = 0x1
	PROT_WRITE = 0x2
	PROT_EXEC  = 0x4
	PROT_NONE  = 0x0
)

const (
	MAP_SHARED    = 0x01
	MAP_PRIVATE   = 0x02
	MAP_ANONYMOUS = 0x20
)

const (
	MS_ASYNC      = 0x1
	MS_SYNC       = 0x4
	MS_INVALIDATE = 0x2
)
