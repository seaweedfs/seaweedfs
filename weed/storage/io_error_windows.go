//go:build windows

package storage

import (
	"errors"
	"syscall"
)

const (
	errERROR_CRC       = syscall.Errno(23)
	errERROR_IO_DEVICE = syscall.Errno(1117)
)

func isWindowsStorageIoError(err error) bool {
	if errors.Is(err, errERROR_CRC) {
		return true
	}
	if errors.Is(err, errERROR_IO_DEVICE) {
		return true
	}
	return false
}
