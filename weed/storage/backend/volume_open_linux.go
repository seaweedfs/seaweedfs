//go:build linux

package backend

import (
	"errors"
	"os"
	"syscall"
)

// OpenVolumeFile opens a volume data or index file with O_NOATIME. Nothing
// reads these files' atime, but without the flag every needle read dirties
// the inode — even relatime writes atime on the first read after each write,
// so an actively written volume pays a metadata write per read/write cycle.
func OpenVolumeFile(fileName string, flag int) (*os.File, error) {
	file, err := os.OpenFile(fileName, flag|syscall.O_NOATIME, 0644)
	if err != nil && errors.Is(err, syscall.EPERM) {
		// O_NOATIME is refused unless we own the file or hold CAP_FOWNER.
		return os.OpenFile(fileName, flag, 0644)
	}
	return file, err
}
