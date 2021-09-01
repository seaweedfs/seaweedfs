//go:build linux || darwin || freebsd || netbsd || openbsd || plan9 || solaris || zos
// +build linux darwin freebsd netbsd openbsd plan9 solaris zos

package util

import (
	"os"
	"syscall"
)

func GetFileUidGid(fi os.FileInfo) (uid, gid uint32) {
	return fi.Sys().(*syscall.Stat_t).Uid, fi.Sys().(*syscall.Stat_t).Gid
}
