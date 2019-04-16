// +build !linux

package stats

import "github.com/HZ89/seaweedfs/weed/pb/volume_server_pb"

func fillInMemStatus(status *volume_server_pb.MemStatus) {
	return
}
