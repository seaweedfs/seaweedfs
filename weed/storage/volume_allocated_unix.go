//go:build !windows

package storage

import (
	"os"
	"syscall"
)

// AllocatedSize reports the bytes the filesystem holds for a file, which
// exceeds its size when the file was preallocated with FALLOC_FL_KEEP_SIZE.
func AllocatedSize(fi os.FileInfo) uint64 {
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return 0
	}
	return uint64(st.Blocks) * 512
}
