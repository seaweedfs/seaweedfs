package stats

import "github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"

func calculateDiskRemaining(disk *volume_server_pb.DiskStatus) {
	disk.Used = disk.All - disk.Free

	if disk.All > 0 {
		disk.PercentFree = float32((float64(disk.Free) / float64(disk.All)) * 100)
		disk.PercentUsed = float32((float64(disk.Used) / float64(disk.All)) * 100)
	} else {
		disk.PercentFree = 0
		disk.PercentUsed = 0
	}

	return
}
