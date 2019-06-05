package storage

import (
	"context"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	"github.com/chrislusf/seaweedfs/weed/storage/erasure_coding"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/klauspost/reedsolomon"
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
		} else {
			return fmt.Errorf("%s load ec shard %d.%d: %v", location.Directory, vid, shardId, err)
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

func (s *Store) DestroyEcVolume(vid needle.VolumeId) {
	for _, location := range s.Locations {
		location.DestroyEcVolume(vid)
	}
}

func (s *Store) ReadEcShardNeedle(ctx context.Context, vid needle.VolumeId, n *needle.Needle) (int, error) {
	for _, location := range s.Locations {
		if localEcVolume, found := location.FindEcVolume(vid); found {

			// read the volume version
			for localEcVolume.Version == 0 {
				err := s.readEcVolumeVersion(ctx, vid, localEcVolume)
				time.Sleep(1357 * time.Millisecond)
				glog.V(0).Infof("ReadEcShardNeedle vid %d version:%v: %v", vid, localEcVolume.Version, err)
			}
			version := localEcVolume.Version

			offset, size, intervals, err := localEcVolume.LocateEcShardNeedle(n, version)
			if err != nil {
				return 0, err
			}

			glog.V(4).Infof("read ec volume %d offset %d size %d intervals:%+v", vid, offset.ToAcutalOffset(), size, intervals)

			if len(intervals) > 1 {
				glog.V(4).Infof("ReadEcShardNeedle needle id %s intervals:%+v", n.String(), intervals)
			}
			bytes, err := s.readEcShardIntervals(ctx, vid, localEcVolume, intervals)
			if err != nil {
				return 0, fmt.Errorf("ReadEcShardIntervals: %v", err)
			}

			err = n.ReadBytes(bytes, offset.ToAcutalOffset(), size, version)
			if err != nil {
				return 0, fmt.Errorf("readbytes: %v", err)
			}

			return len(bytes), nil
		}
	}
	return 0, fmt.Errorf("ec shard %d not found", vid)
}

func (s *Store) readEcVolumeVersion(ctx context.Context, vid needle.VolumeId, ecVolume *erasure_coding.EcVolume) (err error) {

	interval := erasure_coding.Interval{
		BlockIndex:          0,
		InnerBlockOffset:    0,
		Size:                _SuperBlockSize,
		IsLargeBlock:        true, // it could be large block, but ok in this place
		LargeBlockRowsCount: 0,
	}
	data, err := s.readEcShardIntervals(ctx, vid, ecVolume, []erasure_coding.Interval{interval})
	if err == nil {
		ecVolume.Version = needle.Version(data[0])
	}
	return
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
			glog.V(0).Infof("read local ec shard %d.%d: %v", ecVolume.VolumeId, shardId, err)
			return
		}
	} else {
		ecVolume.ShardLocationsLock.RLock()
		sourceDataNodes, hasShardIdLocation := ecVolume.ShardLocations[shardId]
		ecVolume.ShardLocationsLock.RUnlock()

		// try reading directly
		if hasShardIdLocation {
			_, err = s.readRemoteEcShardInterval(ctx, sourceDataNodes, ecVolume.VolumeId, shardId, data, actualOffset)
			if err == nil {
				return
			}
			glog.V(0).Infof("clearing ec shard %d.%d locations: %v", ecVolume.VolumeId, shardId, err)
			forgetShardId(ecVolume, shardId)
		}

		// try reading by recovering from other shards
		_, err = s.recoverOneRemoteEcShardInterval(ctx, ecVolume, shardId, data, actualOffset)
		if err == nil {
			return
		}
		glog.V(0).Infof("recover ec shard %d.%d : %v", ecVolume.VolumeId, shardId, err)
	}
	return
}

func forgetShardId(ecVolume *erasure_coding.EcVolume, shardId erasure_coding.ShardId) {
	// failed to access the source data nodes, clear it up
	ecVolume.ShardLocationsLock.Lock()
	delete(ecVolume.ShardLocations, shardId)
	ecVolume.ShardLocationsLock.Unlock()
}

func (s *Store) cachedLookupEcShardLocations(ctx context.Context, ecVolume *erasure_coding.EcVolume) (err error) {

	shardCount := len(ecVolume.ShardLocations)
	if shardCount < erasure_coding.DataShardsCount &&
		ecVolume.ShardLocationsRefreshTime.Add(11*time.Second).After(time.Now()) ||
		shardCount == erasure_coding.TotalShardsCount &&
			ecVolume.ShardLocationsRefreshTime.Add(37*time.Minute).After(time.Now()) ||
		shardCount >= erasure_coding.DataShardsCount &&
			ecVolume.ShardLocationsRefreshTime.Add(7*time.Minute).After(time.Now()) {
		// still fresh
		return nil
	}

	glog.V(3).Infof("lookup and cache ec volume %d locations", ecVolume.VolumeId)

	err = operation.WithMasterServerClient(s.MasterAddress, s.grpcDialOption, func(masterClient master_pb.SeaweedClient) error {
		req := &master_pb.LookupEcVolumeRequest{
			VolumeId: uint32(ecVolume.VolumeId),
		}
		resp, err := masterClient.LookupEcVolume(ctx, req)
		if err != nil {
			return fmt.Errorf("lookup ec volume %d: %v", ecVolume.VolumeId, err)
		}
		if len(resp.ShardIdLocations) < erasure_coding.DataShardsCount {
			return fmt.Errorf("only %d shards found but %d required", len(resp.ShardIdLocations), erasure_coding.DataShardsCount)
		}

		ecVolume.ShardLocationsLock.Lock()
		for _, shardIdLocations := range resp.ShardIdLocations {
			shardId := erasure_coding.ShardId(shardIdLocations.ShardId)
			delete(ecVolume.ShardLocations, shardId)
			for _, loc := range shardIdLocations.Locations {
				ecVolume.ShardLocations[shardId] = append(ecVolume.ShardLocations[shardId], loc.Url)
			}
		}
		ecVolume.ShardLocationsRefreshTime = time.Now()
		ecVolume.ShardLocationsLock.Unlock()

		return nil
	})
	return
}

func (s *Store) readRemoteEcShardInterval(ctx context.Context, sourceDataNodes []string, vid needle.VolumeId, shardId erasure_coding.ShardId, buf []byte, offset int64) (n int, err error) {

	if len(sourceDataNodes) == 0 {
		return 0, fmt.Errorf("failed to find ec shard %d.%d", vid, shardId)
	}

	for _, sourceDataNode := range sourceDataNodes {
		glog.V(4).Infof("read remote ec shard %d.%d from %s", vid, shardId, sourceDataNode)
		n, err = s.doReadRemoteEcShardInterval(ctx, sourceDataNode, vid, shardId, buf, offset)
		if err == nil {
			return
		}
		glog.V(1).Infof("read remote ec shard %d.%d from %s: %v", vid, shardId, sourceDataNode, err)
	}

	return
}

func (s *Store) doReadRemoteEcShardInterval(ctx context.Context, sourceDataNode string, vid needle.VolumeId, shardId erasure_coding.ShardId, buf []byte, offset int64) (n int, err error) {

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

func (s *Store) recoverOneRemoteEcShardInterval(ctx context.Context, ecVolume *erasure_coding.EcVolume, shardIdToRecover erasure_coding.ShardId, buf []byte, offset int64) (n int, err error) {
	glog.V(4).Infof("recover ec shard %d.%d from other locations", ecVolume.VolumeId, shardIdToRecover)

	enc, err := reedsolomon.New(erasure_coding.DataShardsCount, erasure_coding.ParityShardsCount)
	if err != nil {
		return 0, fmt.Errorf("failed to create encoder: %v", err)
	}

	bufs := make([][]byte, erasure_coding.TotalShardsCount)

	var wg sync.WaitGroup
	ecVolume.ShardLocationsLock.RLock()
	for shardId, locations := range ecVolume.ShardLocations {

		// skip currnent shard or empty shard
		if shardId == shardIdToRecover {
			continue
		}
		if len(locations) == 0 {
			glog.V(3).Infof("readRemoteEcShardInterval missing %d.%d from %+v", ecVolume.VolumeId, shardId, locations)
			continue
		}

		// read from remote locations
		wg.Add(1)
		go func(shardId erasure_coding.ShardId, locations []string) {
			defer wg.Done()
			data := make([]byte, len(buf))
			nRead, readErr := s.readRemoteEcShardInterval(ctx, locations, ecVolume.VolumeId, shardId, data, offset)
			if readErr != nil {
				glog.V(3).Infof("recover: readRemoteEcShardInterval %d.%d %d bytes from %+v: %v", ecVolume.VolumeId, shardId, nRead, locations, readErr)
				forgetShardId(ecVolume, shardId)
			}
			if nRead == len(buf) {
				bufs[shardId] = data
			}
		}(shardId, locations)
	}
	ecVolume.ShardLocationsLock.RUnlock()

	wg.Wait()

	if err = enc.ReconstructData(bufs); err != nil {
		glog.V(3).Infof("recovered ec shard %d.%d failed: %v", ecVolume.VolumeId, shardIdToRecover, err)
		return 0, err
	}
	glog.V(4).Infof("recovered ec shard %d.%d from other locations", ecVolume.VolumeId, shardIdToRecover)

	copy(buf, bufs[shardIdToRecover])

	return len(buf), nil
}

func (s *Store) EcVolumes() (ecVolumes []*erasure_coding.EcVolume) {
	for _, location := range s.Locations {
		location.ecVolumesLock.RLock()
		for _, v := range location.ecVolumes {
			ecVolumes = append(ecVolumes, v)
		}
		location.ecVolumesLock.RUnlock()
	}
	sort.Slice(ecVolumes, func(i, j int) bool {
		return ecVolumes[i].VolumeId > ecVolumes[j].VolumeId
	})
	return ecVolumes
}
