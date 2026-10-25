//go:build !linux

package util

// DropOSPageCache is a no-op on non-Linux platforms.
func DropOSPageCache(fd int, offset int64, length int64) error {
	return nil
}
