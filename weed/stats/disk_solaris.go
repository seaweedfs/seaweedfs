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
	disk.Used = disk.All - disk.Free

	if disk.All > 0 {
		disk.PercentFree = float32(float64(disk.Free) / float64(disk.All) * 100)
		disk.PercentUsed = float32(float64(disk.Used) / float64(disk.All) * 100)
	} else {
		disk.PercentFree = 0
		disk.PercentUsed = 0
	}
}
