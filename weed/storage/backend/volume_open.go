//go:build !linux

package backend

import "os"

// OpenVolumeFile opens a volume data or index file. Only Linux can suppress
// atime updates per file descriptor; elsewhere this is a plain open.
func OpenVolumeFile(fileName string, flag int) (*os.File, error) {
	return os.OpenFile(fileName, flag, 0644)
}
