//go:build linux || darwin || freebsd || netbsd || openbsd || solaris

package util

import (
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

// IsReadOnlyMount reports whether path is on a filesystem mounted read-only.
// access(2) with W_OK answers EROFS for that case regardless of the permission
// bits, and also for root, unlike TestFolderWritable and CheckFile.
func IsReadOnlyMount(path string) bool {
	return unix.Access(path, unix.W_OK) == unix.EROFS
}

// DirOwnedBySelf reports whether the directory info belongs to the current
// user. Content under os.TempDir is only trusted when it does.
func DirOwnedBySelf(fi os.FileInfo) bool {
	st, ok := fi.Sys().(*syscall.Stat_t)
	return ok && int(st.Uid) == os.Getuid()
}

// FileDeviceId returns the device the file lives on, for a stable disk
// identity across processes.
func FileDeviceId(fi os.FileInfo) uint64 {
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return 0
	}
	return uint64(st.Dev)
}
