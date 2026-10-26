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
