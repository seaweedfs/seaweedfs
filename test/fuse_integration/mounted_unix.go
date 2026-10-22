//go:build unix

package fuse

import "syscall"

// deviceID returns the device ID of the filesystem containing path.
func deviceID(path string) (uint64, error) {
	var st syscall.Stat_t
	if err := syscall.Stat(path, &st); err != nil {
		return 0, err
	}
	return uint64(st.Dev), nil
}
