package erasure_coding

import (
	"fmt"
	"slices"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/idx"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// ScrubIndex verifies index integrity of an EC volume.
func (ev *EcVolume) ScrubIndex() (int64, []error) {
	if ev.ecxFile == nil {
		return 0, []error{fmt.Errorf("no ECX file associated with EC volume %v", ev.VolumeId)}
	}
	if ev.ecxFileSize == 0 {
		return 0, []error{fmt.Errorf("zero-size ECX file for EC volume %v", ev.VolumeId)}
	}

	return idx.CheckIndexFile(ev.ecxFile, ev.ecxFileSize, ev.Version)
}

// ScrubLocal checks the integrity of local shards for a EC volume. Notably, it cannot verify CRC on needles.
// Returns a count of processed file entries, slice of found broken shards, and slice of found errors.
func (ecv *EcVolume) ScrubLocal() (int64, []*volume_server_pb.EcShardInfo, []error) {
	// local scan means verifying indexes as well
	_, errs := ecv.ScrubIndex()

	brokenShardsMap := map[ShardId]*EcVolumeShard{}
	var count int64

	flagShardBroken := func(ecs *EcVolumeShard, errFmt string, a ...any) {
		// reads for EC chunks can hit the same shard multiple times, so dedupe upon read errors
		brokenShardsMap[ecs.ShardId] = ecs
		errs = append(errs, fmt.Errorf(errFmt, a...))
	}

	err := idx.WalkIndexFile(ecv.ecxFile, 0, func(id types.NeedleId, offset types.Offset, size types.Size) error {
		count++
		if size.IsTombstone() {
			// nothing to do for tombstones...
			return nil
		}

		var read int64
		locations := ecv.LocateEcShardNeedleInterval(ecv.Version, offset.ToActualOffset(), size)

		for i, iv := range locations {
			sid, soffset := iv.ToShardIdAndOffset(ErasureCodingLargeBlockSize, ErasureCodingSmallBlockSize)
			ssize := int64(iv.Size.Raw())
			shard, found := ecv.FindEcVolumeShard(sid)
			if !found {
				// shard is not local :( skip it
				read += ssize
				continue
			}
			if soffset+int64(ssize) > shard.Size() {
				flagShardBroken(shard, "local shard %d for needle %d is too short (%d), cannot read chunk %d/%d", sid, id, shard.Size(), i+1, len(locations))
				continue
			}

			buf := make([]byte, ssize)
			got, err := shard.ReadAt(buf, soffset)
			if err != nil {
				flagShardBroken(shard, "failed to read chunk %d/%d for needle %d from local shard %d at offset %d: %v", i+1, len(locations), id, sid, soffset, err)
				continue
			}
			if int64(got) != ssize {
				flagShardBroken(shard, "expected %d bytes for chunk %d/%d for needle %d from local shard %d, got %d", ssize, i+1, len(locations), id, sid, got)
				continue
			}
			read += int64(got)
		}

		if got, want := read, needle.GetActualSize(size, ecv.Version); got != want {
			return fmt.Errorf("expected %d bytes for needle %d, got %d", want, id, got)
		}

		return nil
	})
	if err != nil {
		errs = append(errs, err)
	}

	// collect broken shard infos for reporting
	brokenShards := make([]*volume_server_pb.EcShardInfo, 0, len(brokenShardsMap))
	for _, s := range brokenShardsMap {
		brokenShards = append(brokenShards, s.ToEcShardInfo())
	}
	slices.SortFunc(brokenShards, func(a, b *volume_server_pb.EcShardInfo) int {
		if a.ShardId < b.ShardId {
			return -1
		}
		if a.ShardId > b.ShardId {
			return 1
		}
		return 0
	})

	return count, brokenShards, errs
}
