//go:build windows

package fuse

import (
	"os"
	"path/filepath"
	"syscall"
)

// deviceID has no portable meaning here: reporting one device keeps
// waitForMount from mistaking the plain pre-mount directory for a live mount.
func deviceID(path string) (uint64, error) {
	return 0, nil
}

// mountedAt reports whether path is a WinFsp mount: a directory mount is a
// reparse point and a drive mount is a volume root, while a plain directory
// is neither.
func mountedAt(path string) bool {
	abs, err := filepath.Abs(path)
	if err != nil {
		return false
	}
	if filepath.Dir(abs) == abs {
		return true
	}
	info, err := os.Lstat(abs)
	if err != nil {
		return false
	}
	data, ok := info.Sys().(*syscall.Win32FileAttributeData)
	return ok && data.FileAttributes&syscall.FILE_ATTRIBUTE_REPARSE_POINT != 0
}

// fuseFilesystem is covered by the WinFsp checks in mountedAt: WinFsp is the
// only FUSE provider on the platform.
func fuseFilesystem(path string) bool {
	return true
}
