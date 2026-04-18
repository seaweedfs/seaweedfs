//go:build linux

package util

import "golang.org/x/sys/unix"

// DropOSPageCache advises the kernel that the given byte range is no longer
// needed in the page cache. This is useful after reading from a user-space
// cache (e.g., on-disk chunk cache) to prevent the kernel from double-caching
// the same data.
func DropOSPageCache(fd int, offset int64, length int64) error {
	return unix.Fadvise(fd, offset, length, unix.FADV_DONTNEED)
}
