package stats

import (
	"github.com/seaweedfs/seaweedfs/weed/util/log"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
)

func NewDiskStatus(path string) (disk *volume_server_pb.DiskStatus) {
	disk = &volume_server_pb.DiskStatus{Dir: path}
	fillInDiskStatus(disk)
	if disk.PercentUsed > 95 {
		log.V(3).Infof("disk status: %v", disk)
	}
	return
}
