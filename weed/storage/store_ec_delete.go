package storage

import (
	"context"
	"errors"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/pb"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func (s *Store) DeleteEcShardNeedle(ecVolume *erasure_coding.EcVolume, n *needle.Needle, cookie types.Cookie) (int64, error) {

	count, err := s.ReadEcShardNeedle(ecVolume.VolumeId, n, nil)

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

// errEcShardMissing indicates that no data node in the topology currently
// holds the requested EC shard. Callers use this to trigger fallback to
// another shard, distinguishing "no home for this shard" from "the shard
// holder is reachable but the RPC failed".
var errEcShardMissing = fmt.Errorf("ec shard missing")

func (s *Store) doDeleteNeedleFromAtLeastOneRemoteEcShards(ecVolume *erasure_coding.EcVolume, needleId types.NeedleId) error {

	_, _, intervals, err := ecVolume.LocateEcShardNeedle(needleId, ecVolume.Version)
	if err != nil {
		return err
	}
	if len(intervals) == 0 {
		return erasure_coding.NotFoundError
	}

	primaryShardId, _ := intervals[0].ToShardIdAndOffset(erasure_coding.ErasureCodingLargeBlockSize, erasure_coding.ErasureCodingSmallBlockSize)

	// Normal path: delete on exactly one node holding the primary data shard.
	err = s.doDeleteNeedleFromRemoteEcShardServers(primaryShardId, ecVolume, needleId)
	if err == nil || !errors.Is(err, errEcShardMissing) {
		return err
	}

	// Primary data shard has no live holders; fall back to any other shard
	// (remaining data shards first, then parity) so a shard holder can still
	// tombstone the .ecx and the delete is durable.
	for shardId := erasure_coding.ShardId(0); shardId < erasure_coding.TotalShardsCount; shardId++ {
		if shardId == primaryShardId {
			continue
		}
		if fallbackErr := s.doDeleteNeedleFromRemoteEcShardServers(shardId, ecVolume, needleId); fallbackErr == nil {
			return nil
		}
	}

	return err

}

func (s *Store) doDeleteNeedleFromRemoteEcShardServers(shardId erasure_coding.ShardId, ecVolume *erasure_coding.EcVolume, needleId types.NeedleId) error {

	ecVolume.ShardLocationsLock.RLock()
	sourceDataNodes, hasShardLocations := ecVolume.ShardLocations[shardId]
	ecVolume.ShardLocationsLock.RUnlock()

	if !hasShardLocations || len(sourceDataNodes) == 0 {
		return fmt.Errorf("ec shard %d.%d: %w", ecVolume.VolumeId, shardId, errEcShardMissing)
	}

	// Apply the delete on exactly one node. Replicas of the same shard all
	// hold identical .ecx copies, so tombstoning more than one would double
	// the delete count in heartbeat reporting. Try nodes in order and stop
	// at the first success; only return error if every replica failed.
	var lastErr error
	for _, sourceDataNode := range sourceDataNodes {
		glog.V(4).Infof("delete from remote ec shard %d.%d from %s", ecVolume.VolumeId, shardId, sourceDataNode)
		if err := s.doDeleteNeedleFromRemoteEcShard(sourceDataNode, ecVolume.VolumeId, ecVolume.Collection, ecVolume.Version, needleId); err == nil {
			return nil
		} else {
			lastErr = err
		}
	}

	return lastErr

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
