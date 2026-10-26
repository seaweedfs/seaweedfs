//go:build windows

package storage

import "os"

// AllocatedSize is unknown on Windows, where volume files are never preallocated.
func AllocatedSize(fi os.FileInfo) uint64 {
	return 0
}
