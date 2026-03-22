//go:build !linux && !darwin

package fuse

import (
	"errors"
)

// File allocation support for unsupported platforms

var ErrFallocateNotSupported = errors.New("fallocate not supported on this platform")

const (
	// Dummy flags for compatibility
	FALLOC_FL_KEEP_SIZE      = 0x01
	FALLOC_FL_PUNCH_HOLE     = 0x02
	FALLOC_FL_NO_HIDE_STALE  = 0x04
	FALLOC_FL_COLLAPSE_RANGE = 0x08
	FALLOC_FL_ZERO_RANGE     = 0x10
	FALLOC_FL_INSERT_RANGE   = 0x20
	FALLOC_FL_UNSHARE_RANGE  = 0x40
)

func fallocateFile(fd int, mode int, offset int64, length int64) error {
	return ErrFallocateNotSupported
}

func isFallocateSupported() bool {
	return false
}
