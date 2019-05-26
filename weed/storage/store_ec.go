package storage

import (
	"fmt"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/storage/erasure_coding"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
)

func (s *Store) CollectErasureCodingHeartbeat() *master_pb.Heartbeat {
	var ecShardMessages []*master_pb.VolumeEcShardInformationMessage
	for _, location := range s.Locations {
		location.ecShardsLock.RLock()
		for _, ecShards := range location.ecShards {
			ecShardMessages = append(ecShardMessages, ecShards.ToVolumeEcShardInformationMessage()...)
		}
		location.ecShardsLock.RUnlock()
	}

	return &master_pb.Heartbeat{
		EcShards: ecShardMessages,
	}

}

func (s *Store) MountEcShards(collection string, vid needle.VolumeId, shardId erasure_coding.ShardId) error {
	for _, location := range s.Locations {
		if err := location.LoadEcShard(collection, vid, shardId); err == nil {
			glog.V(0).Infof("MountEcShards %d.%d", vid, shardId)

			var shardBits erasure_coding.ShardBits

			s.NewEcShardsChan <- master_pb.VolumeEcShardInformationMessage{
				Id:               uint32(vid),
				Collection:       collection,
				EcIndexBits:      uint32(shardBits.AddShardId(shardId)),
			}
			return nil
		}
	}

	return fmt.Errorf("MountEcShards %d.%d not found on disk", vid, shardId)
}

func (s *Store) UnmountEcShards(vid needle.VolumeId, shardId erasure_coding.ShardId) error {

	ecShard, found := s.findEcShard(vid, shardId)
	if !found {
		return nil
	}

	var shardBits erasure_coding.ShardBits
	message := master_pb.VolumeEcShardInformationMessage{
		Id:               uint32(vid),
		Collection:       ecShard.Collection,
		EcIndexBits:      uint32(shardBits.AddShardId(shardId)),
	}

	for _, location := range s.Locations {
		if deleted := location.UnloadEcShard(vid, shardId); deleted {
			glog.V(0).Infof("UnmountEcShards %d.%d", vid, shardId)
			s.DeletedEcShardsChan <- message
			return nil
		}
	}

	return fmt.Errorf("UnmountEcShards %d.%d not found on disk", vid, shardId)
}

func (s *Store) findEcShard(vid needle.VolumeId, shardId erasure_coding.ShardId) (*erasure_coding.EcVolumeShard, bool)  {
	for _, location := range s.Locations {
		if v, found := location.FindEcShard(vid, shardId); found {
			return v, found
		}
	}
	return nil, false
}
