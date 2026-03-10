//go:build linux

package batchio

import (
	"os"
	"syscall"
)

// fdatasync flushes file data to disk without updating metadata (mtime, size).
// On Linux, this uses the fdatasync(2) syscall directly.
func fdatasync(fd *os.File) error {
	return syscall.Fdatasync(int(fd.Fd()))
}
