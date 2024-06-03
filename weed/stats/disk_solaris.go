//go:build solaris
// +build solaris

package stats

import (
	"golang.org/x/sys/unix"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
)

func fillInDiskStatus(disk *volume_server_pb.DiskStatus) {
	var stat unix.Statvfs_t
	err := unix.Statvfs(disk.Dir, &stat)
	if err != nil {
		return
	}

	disk.All = stat.Blocks * uint64(stat.Bsize)
	disk.Free = stat.Bfree * uint64(stat.Bsize)
	calculateDiskRemaining(disk)

	return
}
