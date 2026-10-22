//go:build unix

package fuse

import (
	"path/filepath"
	"syscall"
)

// deviceID returns the device ID of the filesystem containing path.
func deviceID(path string) (uint64, error) {
	var st syscall.Stat_t
	if err := syscall.Stat(path, &st); err != nil {
		return 0, err
	}
	return uint64(st.Dev), nil
}

// mountedAt reports whether something is actually mounted at path: a plain
// directory shares its parent's device, a mount point does not. The path is
// cleaned first so a trailing slash does not make Dir return the mount itself.
func mountedAt(path string) bool {
	path = filepath.Clean(path)
	dev, err := deviceID(path)
	if err != nil {
		return false
	}
	parentDev, err := deviceID(filepath.Dir(path))
	if err != nil {
		return false
	}
	return dev != parentDev
}
