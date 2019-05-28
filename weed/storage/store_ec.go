package storage

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	"github.com/chrislusf/seaweedfs/weed/storage/erasure_coding"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
)

func (s *Store) CollectErasureCodingHeartbeat() *master_pb.Heartbeat {
	var ecShardMessages []*master_pb.VolumeEcShardInformationMessage
	for _, location := range s.Locations {
		location.ecVolumesLock.RLock()
		for _, ecShards := range location.ecVolumes {
			ecShardMessages = append(ecShardMessages, ecShards.ToVolumeEcShardInformationMessage()...)
		}
		location.ecVolumesLock.RUnlock()
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

func (s *Store) FindEcVolume(vid needle.VolumeId) (*erasure_coding.EcVolume, bool) {
	for _, location := range s.Locations {
		if s, found := location.FindEcVolume(vid); found {
			return s, true
		}
	}
	return nil, false
}

func (s *Store) ReadEcShardNeedle(ctx context.Context, vid needle.VolumeId, n *needle.Needle) (int, error) {
	for _, location := range s.Locations {
		if localEcVolume, found := location.FindEcVolume(vid); found {

			offset, size, intervals, err := localEcVolume.LocateEcShardNeedle(n)
			if err != nil {
				return 0, err
			}

			bytes, err := s.readEcShardIntervals(ctx, vid, localEcVolume, intervals)
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

func (s *Store) readEcShardIntervals(ctx context.Context, vid needle.VolumeId, ecVolume *erasure_coding.EcVolume, intervals []erasure_coding.Interval) (data []byte, err error) {

	if err = s.cachedLookupEcShardLocations(ctx, ecVolume); err != nil {
		return nil, fmt.Errorf("failed to locate shard via master grpc %s: %v", s.MasterAddress, err)
	}

	for i, interval := range intervals {
		if d, e := s.readOneEcShardInterval(ctx, ecVolume, interval); e != nil {
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

func (s *Store) readOneEcShardInterval(ctx context.Context, ecVolume *erasure_coding.EcVolume, interval erasure_coding.Interval) (data []byte, err error) {
	shardId, actualOffset := interval.ToShardIdAndOffset(erasure_coding.ErasureCodingLargeBlockSize, erasure_coding.ErasureCodingSmallBlockSize)
	data = make([]byte, interval.Size)
	if shard, found := ecVolume.FindEcVolumeShard(shardId); found {
		if _, err = shard.ReadAt(data, actualOffset); err != nil {
			return
		}
	} else {
		ecVolume.ShardLocationsLock.RLock()
		sourceDataNodes, found := ecVolume.ShardLocations[shardId]
		ecVolume.ShardLocationsLock.RUnlock()
		if !found || len(sourceDataNodes) == 0 {
			return nil, fmt.Errorf("failed to find ec shard %d.%d", ecVolume.VolumeId, shardId)
		}
		_, err = s.readOneRemoteEcShardInterval(ctx, sourceDataNodes[0], ecVolume.VolumeId, shardId, data, actualOffset)
		if err != nil {
			glog.V(1).Infof("failed to read from %s for ec shard %d.%d : %v", sourceDataNodes[0], ecVolume.VolumeId, shardId, err)
		}
	}
	return
}

func (s *Store) cachedLookupEcShardLocations(ctx context.Context, ecVolume *erasure_coding.EcVolume) (err error) {

	if ecVolume.ShardLocationsRefreshTime.Add(10 * time.Minute).After(time.Now()) {
		// still fresh
		return nil
	}

	ecVolume.ShardLocationsLock.Lock()
	defer ecVolume.ShardLocationsLock.Unlock()

	err = operation.WithMasterServerClient(s.MasterAddress, s.grpcDialOption, func(masterClient master_pb.SeaweedClient) error {
		return nil
	})
	return
}

func (s *Store) readOneRemoteEcShardInterval(ctx context.Context, sourceDataNode string, vid needle.VolumeId, shardId erasure_coding.ShardId, buf []byte, offset int64) (n int, err error) {

	err = operation.WithVolumeServerClient(sourceDataNode, s.grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {

		// copy data slice
		shardReadClient, err := client.VolumeEcShardRead(ctx, &volume_server_pb.VolumeEcShardReadRequest{
			VolumeId: uint32(vid),
			ShardId:  uint32(shardId),
			Offset:   offset,
			Size:     int64(len(buf)),
		})
		if err != nil {
			return fmt.Errorf("failed to start reading ec shard %d.%d from %s: %v", vid, shardId, sourceDataNode, err)
		}

		for {
			resp, receiveErr := shardReadClient.Recv()
			if receiveErr == io.EOF {
				break
			}
			if receiveErr != nil {
				return fmt.Errorf("receiving ec shard %d.%d from %s: %v", vid, shardId, sourceDataNode, err)
			}
			copy(buf[n:n+len(resp.Data)], resp.Data)
			n += len(resp.Data)
		}

		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("read ec shard %d.%d from %s: %v", vid, shardId, sourceDataNode, err)
	}

	return
}

func (s *Store) recoverOneRemoteEcShardInterval(ctx context.Context, string, vid needle.VolumeId, shardIdToRecover erasure_coding.ShardId, buf []byte, offset int64) (n int, err error) {
	glog.V(1).Infof("recover ec shard %d.%d from other locations", vid, shardIdToRecover)
	// TODO add recovering
	return 0, fmt.Errorf("recover is not implemented yet")
}
