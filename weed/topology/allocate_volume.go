package topology

import (
	"context"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"google.golang.org/grpc"
)

// Cap the RPC so a hung volume server can't strand the layout's
// growRequest flag and block all future automatic growth.
const allocateVolumeTimeout = 1 * time.Minute

type AllocateVolumeResult struct {
	Error string
}

func AllocateVolume(dn *DataNode, grpcDialOption grpc.DialOption, vid needle.VolumeId, option *VolumeGrowOption) error {

	return operation.WithVolumeServerClient(false, dn.ServerAddress(), grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {

		ctx, cancel := context.WithTimeout(context.Background(), allocateVolumeTimeout)
		defer cancel()

		_, allocateErr := client.AllocateVolume(ctx, &volume_server_pb.AllocateVolumeRequest{
			VolumeId:           uint32(vid),
			Collection:         option.Collection,
			Replication:        option.ReplicaPlacement.String(),
			Ttl:                option.Ttl.String(),
			Preallocate:        option.Preallocate,
			MemoryMapMaxSizeMb: option.MemoryMapMaxSizeMb,
			DiskType:           string(option.DiskType),
			Version:            option.Version,
		})
		return allocateErr
	})

}

func DeleteVolume(dn *DataNode, grpcDialOption grpc.DialOption, vid needle.VolumeId) error {

	return operation.WithVolumeServerClient(false, dn.ServerAddress(), grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {

		ctx, cancel := context.WithTimeout(context.Background(), allocateVolumeTimeout)
		defer cancel()

		_, allocateErr := client.VolumeDelete(ctx, &volume_server_pb.VolumeDeleteRequest{
			VolumeId: uint32(vid),
		})
		return allocateErr
	})

}
