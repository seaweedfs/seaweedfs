package storage

import (
	"context"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/pb"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func (s *Store) DeleteEcShardNeedle(ctx context.Context, ecVolume *erasure_coding.EcVolume, n *needle.Needle, cookie types.Cookie) (int64, error) {

	count, err := s.ReadEcShardNeedle(ctx, ecVolume.VolumeId, n, nil)

	if err != nil {
		return 0, err
	}

	// cookie == 0 indicates SkipCookieCheck was requested (e.g., orphan cleanup)
	if cookie != 0 && cookie != n.Cookie {
		return 0, fmt.Errorf("unexpected cookie %x", cookie)
	}

	if err = s.doDeleteNeedleFromAtLeastOneRemoteEcShards(ecVolume, n.Id); err != nil {
		return 0, err
	}

	return int64(count), nil

}

func (s *Store) doDeleteNeedleFromAtLeastOneRemoteEcShards(ecVolume *erasure_coding.EcVolume, needleId types.NeedleId) error {

	_, _, intervals, err := ecVolume.LocateEcShardNeedle(needleId, ecVolume.Version)
	if err != nil {
		return err
	}
	if len(intervals) == 0 {
		return erasure_coding.NotFoundError
	}

	shardId, _ := intervals[0].ToShardIdAndOffset(erasure_coding.ErasureCodingLargeBlockSize, erasure_coding.ErasureCodingSmallBlockSize)

	err = s.doDeleteNeedleFromRemoteEcShardServers(shardId, ecVolume, needleId)
	if err == nil {
		return nil
	}

	for shardId = erasure_coding.DataShardsCount; shardId < erasure_coding.TotalShardsCount; shardId++ {
		if parityDeletionError := s.doDeleteNeedleFromRemoteEcShardServers(shardId, ecVolume, needleId); parityDeletionError == nil {
			return nil
		}
	}

	return err

}

func (s *Store) doDeleteNeedleFromRemoteEcShardServers(shardId erasure_coding.ShardId, ecVolume *erasure_coding.EcVolume, needleId types.NeedleId) error {

	ecVolume.ShardLocationsLock.RLock()
	sourceDataNodes, hasShardLocations := ecVolume.ShardLocations[shardId]
	ecVolume.ShardLocationsLock.RUnlock()

	if !hasShardLocations {
		return fmt.Errorf("ec shard %d.%d not located", ecVolume.VolumeId, shardId)
	}

	for _, sourceDataNode := range sourceDataNodes {
		glog.V(4).Infof("delete from remote ec shard %d.%d from %s", ecVolume.VolumeId, shardId, sourceDataNode)
		if err := s.doDeleteNeedleFromRemoteEcShard(sourceDataNode, ecVolume.VolumeId, ecVolume.Collection, ecVolume.Version, needleId); err != nil {
			return err
		}
	}

	return nil

}

func (s *Store) doDeleteNeedleFromRemoteEcShard(sourceDataNode pb.ServerAddress, vid needle.VolumeId, collection string, version needle.Version, needleId types.NeedleId) error {

	return operation.WithVolumeServerClient(false, sourceDataNode, s.grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {

		// copy data slice
		_, err := client.VolumeEcBlobDelete(context.Background(), &volume_server_pb.VolumeEcBlobDeleteRequest{
			VolumeId:   uint32(vid),
			Collection: collection,
			FileKey:    uint64(needleId),
			Version:    uint32(version),
		})
		if err != nil {
			return fmt.Errorf("failed to delete from ec shard %d on %s: %v", vid, sourceDataNode, err)
		}
		return nil
	})

}
