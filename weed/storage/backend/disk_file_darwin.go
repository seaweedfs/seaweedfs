//go:build darwin
// +build darwin

package backend

import (
	"syscall"

	"golang.org/x/sys/unix"
)

const (
	// Using default File.Sync function, same as fcntl(fd, F_FULLFSYNC)
	DM_SYNC = 1

	// Using syscall.Fsync function, for MacOS this is not safe but is very fast.
	DM_FSYNC = 2

	// Using fcntl with F_BARRIERFSYNC parameter, for more details please refer:
	// https://developer.apple.com/documentation/xcode/reducing-disk-writes
	DM_BFSYNC = 3

	F_BARRIERFSYNC = 85
)

var (
	// By default using F_BARRIERFSYNC
	DarwinSyncMode = DM_BFSYNC
)

func (df *DiskFile) Sync() error {
	switch DarwinSyncMode {
	case DM_SYNC:
		return df.File.Sync()
	case DM_BFSYNC:
		fd := df.File.Fd()
		_, err := unix.FcntlInt(fd, F_BARRIERFSYNC, 0)
		return err
	default:
		fd := df.File.Fd()
		return syscall.Fsync(int(fd))
	}
}
