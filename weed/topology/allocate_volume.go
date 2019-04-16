package topology

import (
	"context"

	"github.com/HZ89/seaweedfs/weed/operation"
	"github.com/HZ89/seaweedfs/weed/pb/volume_server_pb"
	"github.com/HZ89/seaweedfs/weed/storage"
	"google.golang.org/grpc"
)

type AllocateVolumeResult struct {
	Error string
}

func AllocateVolume(dn *DataNode, grpcDialOption grpc.DialOption, vid storage.VolumeId, option *VolumeGrowOption) error {

	return operation.WithVolumeServerClient(dn.Url(), grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {

		_, deleteErr := client.AllocateVolume(context.Background(), &volume_server_pb.AllocateVolumeRequest{
			VolumeId:    uint32(vid),
			Collection:  option.Collection,
			Replication: option.ReplicaPlacement.String(),
			Ttl:         option.Ttl.String(),
			Preallocate: option.Prealloacte,
		})
		return deleteErr
	})

}
