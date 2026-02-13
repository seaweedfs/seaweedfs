package storage

import (
	"fmt"
	"slices"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// ScrubEcVolume checks the full integrity of a EC volume, across both local and remote shards.
// Returns a count of processed file entries, slice of found broken shards, and slice of found errors.
func (s *Store) ScrubEcVolume(vid needle.VolumeId) (int64, []*volume_server_pb.EcShardInfo, []error) {
	ecv, found := s.FindEcVolume(vid)
	if !found {
		return 0, nil, []error{fmt.Errorf("EC volume id %d not found", vid)}
	}
	if err := s.cachedLookupEcShardLocations(ecv); err != nil {
		return 0, nil, []error{fmt.Errorf("failed to locate shard via master grpc %s: %v", s.MasterAddress, err)}
	}

	// full scan means verifying indexes as well
	_, errs := ecv.ScrubIndex()

	var count int64
	// reads for EC chunks can hit the same shard multiple times, so dedupe upon read errors
	brokenShardsMap := map[erasure_coding.ShardId]*volume_server_pb.EcShardInfo{}

	err := ecv.WalkIndex(func(id types.NeedleId, offset types.Offset, size types.Size) error {
		count++
		if size.IsTombstone() {
			// nothing to do for tombstones...
			return nil
		}

		data := make([]byte, 0, needle.GetActualSize(size, ecv.Version))
		intervals := ecv.LocateEcShardNeedleInterval(ecv.Version, offset.ToActualOffset(), size)

		for i, iv := range intervals {
			chunk := make([]byte, iv.Size)
			shardId, offset := s.IntervalToShardIdAndOffset(iv)

			// try a local shard read first...
			if err := s.readLocalEcShardInterval(ecv, shardId, chunk, offset); err == nil {
				data = append(data, chunk...)
				continue
			}

			// ...then remote. note we do not try to recover EC-encoded data upon read failures;
			// we want check that shards are valid without decoding
			ecv.ShardLocationsLock.RLock()
			sourceDataNodes, ok := ecv.ShardLocations[shardId]
			ecv.ShardLocationsLock.RUnlock()
			if ok {
				if _, _, err := s.readRemoteEcShardInterval(sourceDataNodes, id, ecv.VolumeId, shardId, chunk, offset); err == nil {
					data = append(data, chunk...)
					continue
				}
			}

			// chunk read for shard failed :(
			errs = append(errs, fmt.Errorf("failed to read EC shard %d for needle %d on volume %d (interval %d/%d)", shardId, id, ecv.VolumeId, i+1, len(intervals)))
			brokenShardsMap[shardId] = &volume_server_pb.EcShardInfo{
				ShardId:    uint32(shardId),
				Size:       int64(iv.Size),
				Collection: ecv.Collection,
				VolumeId:   uint32(ecv.VolumeId),
			}
			break
		}

		if got, want := int64(len(data)), needle.GetActualSize(size, ecv.Version); got != want {
			errs = append(errs, fmt.Errorf("expected %d bytes for needle %d, got %d", want, id, got))
			return nil
		}

		n := needle.Needle{}
		if err := n.ReadBytes(data, 0, size, ecv.Version); err != nil {
			errs = append(errs, fmt.Errorf("needle %d on EC volume %d: %v", id, ecv.VolumeId, err))
		}

		return nil
	})
	if err != nil {
		errs = append(errs, err)
	}

	brokenShards := []*volume_server_pb.EcShardInfo{}
	for _, s := range brokenShardsMap {
		brokenShards = append(brokenShards, s)
	}
	slices.SortFunc(brokenShards, erasure_coding.CmpEcShardInfo)

	return count, brokenShards, errs
}
