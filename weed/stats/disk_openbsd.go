//go:build openbsd
// +build openbsd

package stats

import (
    "syscall"

    "github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
)

func fillInDiskStatus(disk *volume_server_pb.DiskStatus) {
    fs := syscall.Statfs_t{}
    err := syscall.Statfs(disk.Dir, &fs)
    if err != nil {
        return
    }
    disk.All = fs.F_blocks * uint64(fs.F_bsize)
    disk.Free = fs.F_bfree * uint64(fs.F_bsize)
    disk.Used = disk.All - disk.Free
    disk.PercentFree = float32((float64(disk.Free) / float64(disk.All)) * 100)
    disk.PercentUsed = float32((float64(disk.Used) / float64(disk.All)) * 100)
    return
}

