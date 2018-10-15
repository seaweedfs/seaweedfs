package topology

import (
	"context"

	"github.com/chrislusf/seaweedfs/weed/storage"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
)

type AllocateVolumeResult struct {
	Error string
}

func AllocateVolume(dn *DataNode, vid storage.VolumeId, option *VolumeGrowOption) error {

	return operation.WithVolumeServerClient(dn.Url(), func(client volume_server_pb.VolumeServerClient) error {
		_, deleteErr := client.AssignVolume(context.Background(), &volume_server_pb.AssignVolumeRequest{
			VolumdId:    uint32(vid),
			Collection:  option.Collection,
			Replication: option.ReplicaPlacement.String(),
			Ttl:         option.Ttl.String(),
			Preallocate: option.Prealloacte,
		})
		return deleteErr
	})

}
