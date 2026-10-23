//go:build windows

package storage

import "os"

// allocatedSize is unknown on Windows, where volume files are never preallocated.
func allocatedSize(fi os.FileInfo) uint64 {
	return 0
}
