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
				Id:          uint32(vid),
				Collection:  collection,
				EcIndexBits: uint32(shardBits.AddShardId(shardId)),
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
		Id:          uint32(vid),
		Collection:  ecShard.Collection,
		EcIndexBits: uint32(shardBits.AddShardId(shardId)),
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

func (s *Store) findEcShard(vid needle.VolumeId, shardId erasure_coding.ShardId) (*erasure_coding.EcVolumeShard, bool) {
	for _, location := range s.Locations {
		if v, found := location.FindEcShard(vid, shardId); found {
			return v, found
		}
	}
	return nil, false
}

func (s *Store) HasEcShard(vid needle.VolumeId) (erasure_coding.EcVolumeShards, bool) {
	for _, location := range s.Locations {
		if s, found := location.HasEcShard(vid); found {
			return s, true
		}
	}
	return nil, false
}

func (s *Store) ReadEcShardNeedle(vid needle.VolumeId, n *needle.Needle) (int, error) {
	for _, location := range s.Locations {
		if localEcShards, found := location.HasEcShard(vid); found {

			offset, size, intervals, err := localEcShards.LocateEcShardNeedle(n)
			if err != nil {
				return 0, err
			}

			bytes, err := s.ReadEcShardIntervals(vid, localEcShards, intervals)
			if err != nil {
				return 0, fmt.Errorf("ReadEcShardIntervals: %v", err)
			}

			version := needle.CurrentVersion

			err = n.ReadBytes(bytes, offset.ToAcutalOffset(), size, version)
			if err != nil {
				return 0, fmt.Errorf("readbytes: %v", err)
			}

			return len(bytes), nil
		}
	}
	return 0, fmt.Errorf("ec shard %d not found", vid)
}

func (s *Store) ReadEcShardIntervals(vid needle.VolumeId, localEcShards erasure_coding.EcVolumeShards, intervals []erasure_coding.Interval) (data []byte, err error) {
	for i, interval := range intervals {
		if d, e := s.readOneEcShardInterval(vid, localEcShards, interval); e != nil {
			return nil, e
		} else {
			if i == 0 {
				data = d
			} else {
				data = append(data, d...)
			}
		}
	}
	return
}

func (s *Store) readOneEcShardInterval(vid needle.VolumeId, localEcShards erasure_coding.EcVolumeShards, interval erasure_coding.Interval) (data []byte, err error) {
	shardId, actualOffset := interval.ToShardIdAndOffset(erasure_coding.ErasureCodingLargeBlockSize, erasure_coding.ErasureCodingSmallBlockSize)
	data = make([]byte, interval.Size)
	if shard, found := localEcShards.FindEcVolumeShard(shardId); found {
		if _, err = shard.ReadAt(data, actualOffset); err != nil {
			return
		}
	} else {
		s.readOneRemoteEcShardInterval(vid, shardId, data, actualOffset)
	}
	return
}

func (s *Store) readOneRemoteEcShardInterval(vid needle.VolumeId, shardId erasure_coding.ShardId, buf []byte, offset int64) (n int, err error) {



	return
}
