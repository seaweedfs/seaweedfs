//go:build !linux && !darwin

package fuse

import (
	"errors"
)

// Extended attributes support for unsupported platforms

var ErrXattrNotSupported = errors.New("extended attributes not supported on this platform")

func setXattr(path, name string, value []byte, flags int) error {
	return ErrXattrNotSupported
}

func getXattr(path, name string, value []byte) (int, error) {
	return 0, ErrXattrNotSupported
}

func listXattr(path string, list []byte) (int, error) {
	return 0, ErrXattrNotSupported
}

func removeXattr(path, name string) error {
	return ErrXattrNotSupported
}

func isXattrSupported() bool {
	return false
}
