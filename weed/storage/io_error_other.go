//go:build !windows

package storage

func isWindowsStorageIoError(err error) bool {
	return false
}
