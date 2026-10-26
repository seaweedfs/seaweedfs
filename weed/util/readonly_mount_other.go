//go:build !(linux || darwin || freebsd || netbsd || openbsd || solaris)

package util

import "os"

// IsReadOnlyMount reports whether path is on a filesystem mounted read-only.
// Not detected on this platform.
func IsReadOnlyMount(path string) bool {
	return false
}

// DirOwnedBySelf reports whether the directory info belongs to the current
// user. Unreachable here: read-only mounts are not detected on this platform.
func DirOwnedBySelf(fi os.FileInfo) bool {
	return false
}

// FileDeviceId returns zero on this platform; read-only mounts are not
// detected, so no derived uuid consumes it.
func FileDeviceId(fi os.FileInfo) uint64 {
	return 0
}
