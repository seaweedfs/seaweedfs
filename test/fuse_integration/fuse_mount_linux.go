//go:build linux

package fuse

import "syscall"

// fuseSuperMagic is statfs f_type for fuse and fuseblk mounts.
const fuseSuperMagic = 0x65735546

// fuseFilesystem reports whether path sits on a FUSE mount.
func fuseFilesystem(path string) bool {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return false
	}
	return st.Type == fuseSuperMagic
}
