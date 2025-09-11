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

func (s *Store) DeleteEcShardNeedle(ecVolume *erasure_coding.EcVolume, n *needle.Needle, cookie types.Cookie) (int64, error) {

	// VERSION CHECK - Should see this in logs if new binary is loaded
	glog.Errorf("⭐ VERSION 2024-08-18-06:51 EC DELETE STARTING: needle %d volume %d", n.Id, ecVolume.VolumeId)
	glog.Errorf("🚀 EC DELETE SHARD NEEDLE: starting deletion for needle %d volume %d", n.Id, ecVolume.VolumeId)

	// Early validation checks - using ERROR level to ensure they appear
	if ecVolume == nil {
		glog.Errorf("❌ EC DELETE: ecVolume is nil for needle %d", n.Id)
		return 0, fmt.Errorf("ecVolume is nil")
	}
	if n == nil {
		glog.Errorf("❌ EC DELETE: needle is nil")
		return 0, fmt.Errorf("needle is nil")
	}

	glog.Errorf("🔍 EC DELETE DEBUG: Validated inputs - needle %d, volume %d, generation %d", n.Id, ecVolume.VolumeId, ecVolume.Generation)

	defer func() {
		if r := recover(); r != nil {
			glog.Errorf("❌ EC DELETE PANIC: needle %d volume %d - %v", n.Id, ecVolume.VolumeId, r)
		}
	}()

	glog.Errorf("🔍 EC DELETE DEBUG: About to call ReadEcShardNeedle for needle %d volume %d", n.Id, ecVolume.VolumeId)
	count, err := s.ReadEcShardNeedle(ecVolume.VolumeId, n, nil)
	glog.Errorf("🔍 EC DELETE DEBUG: ReadEcShardNeedle returned count=%d, err=%v", count, err)

	if err != nil {
		glog.Errorf("❌ EC DELETE: Failed to read needle %d from volume %d: %v", n.Id, ecVolume.VolumeId, err)
		return 0, err
	}
	glog.Infof("✅ EC DELETE: Successfully read needle %d, count=%d", n.Id, count)

	glog.Infof("🔍 EC DELETE DEBUG: Checking cookie for needle %d (expected=%x, actual=%x)", n.Id, cookie, n.Cookie)
	if cookie != n.Cookie {
		glog.Errorf("❌ EC DELETE: Cookie mismatch for needle %d (expected=%x, actual=%x)", n.Id, cookie, n.Cookie)
		return 0, fmt.Errorf("unexpected cookie %x", cookie)
	}
	glog.Infof("✅ EC DELETE: Cookie validation passed for needle %d", n.Id)

	glog.Infof("🔍 EC DELETE DEBUG: Deleting needle %d from remote EC shards", n.Id)
	if err = s.doDeleteNeedleFromAtLeastOneRemoteEcShards(ecVolume, n.Id); err != nil {
		glog.Errorf("❌ EC DELETE: Failed to delete needle %d from remote EC shards: %v", n.Id, err)
		return 0, err
	}
	glog.Infof("✅ EC DELETE: Successfully deleted needle %d from remote EC shards", n.Id)

	// Record the deletion locally in the .ecj journal file
	glog.Infof("🔍 EC DELETION: Recording needle %d in volume %d generation %d",
		n.Id, ecVolume.VolumeId, ecVolume.Generation)
	if err = ecVolume.DeleteNeedleFromEcx(n.Id); err != nil {
		glog.Errorf("❌ Failed to record EC deletion in journal for needle %d: %v", n.Id, err)
		// Continue even if journal write fails - the remote deletion succeeded
	} else {
		glog.Infof("✅ EC deletion recording completed for needle %d", n.Id)
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

	hasDeletionSuccess := false
	err = s.doDeleteNeedleFromRemoteEcShardServers(shardId, ecVolume, needleId)
	if err == nil {
		hasDeletionSuccess = true
	}

	for shardId = erasure_coding.DataShardsCount; shardId < erasure_coding.TotalShardsCount; shardId++ {
		if parityDeletionError := s.doDeleteNeedleFromRemoteEcShardServers(shardId, ecVolume, needleId); parityDeletionError == nil {
			hasDeletionSuccess = true
		}
	}

	if hasDeletionSuccess {
		return nil
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
		err := s.doDeleteNeedleFromRemoteEcShard(sourceDataNode, ecVolume.VolumeId, ecVolume.Collection, ecVolume.Version, needleId)
		if err != nil {
			return err
		}
		glog.V(1).Infof("delete from remote ec shard %d.%d from %s: %v", ecVolume.VolumeId, shardId, sourceDataNode, err)
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
