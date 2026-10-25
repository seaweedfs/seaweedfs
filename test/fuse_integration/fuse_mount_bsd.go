//go:build darwin || freebsd

package fuse

import (
	"strings"
	"syscall"
)

// fuseFilesystem reports whether path sits on a FUSE mount; macFUSE, osxfuse
// and fusefs all carry "fuse" in the filesystem type name.
func fuseFilesystem(path string) bool {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return false
	}
	name := make([]byte, 0, len(st.Fstypename))
	for _, c := range st.Fstypename {
		if c == 0 {
			break
		}
		name = append(name, byte(c))
	}
	return strings.Contains(string(name), "fuse")
}
