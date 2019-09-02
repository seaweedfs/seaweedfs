// +build !linux

package stats

import "github.com/joeslay/seaweedfs/weed/pb/volume_server_pb"

func fillInMemStatus(status *volume_server_pb.MemStatus) {
	return
}
