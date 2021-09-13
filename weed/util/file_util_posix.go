//go:build windows
// +build windows

package util

import (
	"os"
)

func GetFileUidGid(fi os.FileInfo) (uid, gid uint32) {
	return 0, 0
}
