package stats

import "github.com/HZ89/seaweedfs/weed/pb/volume_server_pb"

func NewDiskStatus(path string) (disk *volume_server_pb.DiskStatus) {
	disk = &volume_server_pb.DiskStatus{Dir: path}
	fillInDiskStatus(disk)
	return
}
