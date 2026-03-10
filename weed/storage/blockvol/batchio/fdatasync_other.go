//go:build !linux

package batchio

import "os"

// fdatasync flushes file data to disk. On non-Linux platforms, fdatasync is
// not available, so this falls back to fsync via os.File.Sync().
func fdatasync(fd *os.File) error {
	return fd.Sync()
}
