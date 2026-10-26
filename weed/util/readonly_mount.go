//go:build linux || darwin || freebsd || netbsd || openbsd || solaris

package util

import "golang.org/x/sys/unix"

// IsReadOnlyMount reports whether path is on a filesystem mounted read-only.
// access(2) with W_OK answers EROFS for that case regardless of the permission
// bits, and also for root, unlike TestFolderWritable and CheckFile.
func IsReadOnlyMount(path string) bool {
	return unix.Access(path, unix.W_OK) == unix.EROFS
}
