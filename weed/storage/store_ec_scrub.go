package storage

import (
	"errors"
	"fmt"
	"slices"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// ScrubEcVolume checks the full integrity of a EC volume, across both local and remote shards.
// Returns a count of processed file entries, slice of found broken shards, and slice of found errors.
//
// FULL reports an unreadable shard and gives up on the needle. READS additionally rebuilds the
// interval from the surviving shards, so it reports the same broken shards but only errors on
// needles that parity can no longer recover - which is what exercises the parity data.
func (s *Store) ScrubEcVolume(vid needle.VolumeId, mode volume_server_pb.VolumeScrubMode, forceDeletedNeedlesCheck bool) (int64, []*volume_server_pb.EcShardInfo, []error) {
	ecv, found := s.FindEcVolume(vid)
	if !found {
		return 0, nil, []error{fmt.Errorf("EC volume id %d not found", vid)}
	}
	if err := s.cachedLookupEcShardLocations(ecv); err != nil {
		return 0, nil, []error{fmt.Errorf("failed to locate shard via master grpc %s: %v", s.MasterAddress, err)}
	}

	// full scan means verifying indexes as well
	_, errs := ecv.ScrubIndex()

	recoverUnreadable := mode == volume_server_pb.VolumeScrubMode_READS

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

		shardIds := make([]erasure_coding.ShardId, len(intervals))
		for i, iv := range intervals {
			sid, _ := ecv.IntervalToShardIdAndOffset(iv)
			shardIds[i] = sid
		}
		slices.Sort(shardIds)

		for i, iv := range intervals {
			chunk := make([]byte, iv.Size)
			shardId, offset := ecv.IntervalToShardIdAndOffset(iv)

			// try a local shard read first...
			if err := s.readLocalEcShardInterval(ecv, shardId, chunk, offset); err == nil {
				data = append(data, chunk...)
				continue
			}

			// ...then remote. neither read decodes: the point is to find shards that are
			// themselves broken, not to heal around them
			ecv.ShardLocationsLock.RLock()
			sourceDataNodes, ok := ecv.ShardLocations[shardId]
			ecv.ShardLocationsLock.RUnlock()
			readErr := errors.New("no known shard locations")
			if ok {
				if _, _, readErr = s.readRemoteEcShardInterval(sourceDataNodes, id, ecv.VolumeId, shardId, chunk, offset, ecv.EncodeTsNs); readErr == nil {
					data = append(data, chunk...)
					continue
				}
			}

			// the shard is broken whether or not the needle survives it, so report it either way
			brokenShardsMap[shardId] = &volume_server_pb.EcShardInfo{
				ShardId:    uint32(shardId),
				Size:       int64(iv.Size),
				Collection: ecv.Collection,
				VolumeId:   uint32(ecv.VolumeId),
			}

			if !recoverUnreadable {
				errs = append(errs, fmt.Errorf("failed to read EC shard %d for needle %d on volume %d (interval %d/%d): %v", shardId, id, ecv.VolumeId, i+1, len(intervals), readErr))
				break
			}
			// A holder reporting the needle deleted is authoritative, and it answers
			// with no bytes: the chunk stays zeroed and reaches ReadBytes as the
			// delete-state mismatch the walk below already tolerates.
			if _, isDeleted, err := s.recoverOneRemoteEcShardInterval(id, ecv, shardId, chunk, offset); err != nil && !isDeleted {
				errs = append(errs, fmt.Errorf("failed to recover EC shard %d for needle %d on volume %d (interval %d/%d): %v", shardId, id, ecv.VolumeId, i+1, len(intervals), err))
				break
			}
			data = append(data, chunk...)
		}

		if got, want := int64(len(data)), needle.GetActualSize(size, ecv.Version); got != want {
			errs = append(errs, fmt.Errorf("EC volume %d, needle %d on shards %v: expected %d bytes, got %d", ecv.VolumeId, id, shardIds, want, got))
			return nil
		}

		n := needle.Needle{}
		if err := n.ReadBytes(data, 0, size, ecv.Version); err != nil {
			// needles flagged as deleted in the index but not in the volume (or vice-versa) cannot
			// be properly hydrated, as the header read by needle.ReadBytes() will mismatch.
			deleteSizeMismatch := size.IsDeleted() != (n.Size == 0)
			if !errors.Is(err, needle.ErrorSizeMismatch) || !deleteSizeMismatch || forceDeletedNeedlesCheck {
				errs = append(errs, fmt.Errorf("EC volume %d, needle %d on shards %v: %v", ecv.VolumeId, id, shardIds, err))
			}
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
