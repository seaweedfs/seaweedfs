package erasure_coding

import (
	"bytes"
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

// ChecksumScrub verifies every locally-held EC shard's raw bytes against the
// active-generation bitrot checksum sidecar. It is read-only: it detects and
// reports corruption but never mutates or quarantines anything (the apply path
// drives quarantine + rebuild). It is the only path that exercises cold parity
// shards, which are never read during normal serving.
//
// Returns the number of blocks scanned, the broken shards (sidecar mismatch or
// length drift), and errors. A wholesale mismatch (more than parity_shards
// shards differing) is reported as a suspect/stale sidecar — an integrity error
// — rather than flagging the shards as corrupt, so a bad sidecar can never be
// mistaken for mass bitrot.
func (ecv *EcVolume) ChecksumScrub() (blocksScanned int64, brokenShards []*volume_server_pb.EcShardInfo, errs []error) {
	prot, status := ecv.BitrotProtection()
	switch status {
	case BitrotOff:
		// Unprotected generation: nothing to verify. Not an error.
		return 0, nil, nil
	case BitrotInvalid:
		return 0, nil, []error{fmt.Errorf("ec volume %d: bitrot sidecar is malformed/unverifiable (sidecar integrity)", ecv.VolumeId)}
	}

	blockSize := int64(prot.BlockSize)
	var broken []*EcVolumeShard
	for _, shard := range ecv.Shards {
		entry := shardChecksums(prot, uint32(shard.ShardId))
		if entry == nil {
			errs = append(errs, fmt.Errorf("ec volume %d: no checksum entry for local shard %d", ecv.VolumeId, shard.ShardId))
			continue
		}
		if shard.Size() != entry.CoveredSize {
			errs = append(errs, fmt.Errorf("ec volume %d shard %d: size %d != covered_size %d (truncated or extended)",
				ecv.VolumeId, shard.ShardId, shard.Size(), entry.CoveredSize))
			broken = append(broken, shard)
			continue
		}
		want := unpackUint32LE(entry.BlockCrc32C)
		buf := make([]byte, blockSize)
		var offset int64
		shardBad := false
		for i := 0; i < len(want); i++ {
			toRead := blockSize
			if rem := entry.CoveredSize - offset; rem < toRead {
				toRead = rem
			}
			n, rerr := shard.ReadAt(buf[:toRead], offset)
			if rerr != nil || int64(n) != toRead {
				errs = append(errs, fmt.Errorf("ec volume %d shard %d: read block %d at %d: %v (got %d/%d)",
					ecv.VolumeId, shard.ShardId, i, offset, rerr, n, toRead))
				shardBad = true
				break
			}
			if uint32(needle.NewCRC(buf[:n])) != want[i] {
				errs = append(errs, fmt.Errorf("ec volume %d shard %d: checksum mismatch at block %d (offset %d)",
					ecv.VolumeId, shard.ShardId, i, offset))
				shardBad = true
			}
			offset += int64(n)
			blocksScanned++
		}
		if shardBad {
			broken = append(broken, shard)
		}
	}

	// Wholesale-mismatch guard: localized bitrot touches a few shards; a
	// stale/wrong sidecar mismatches more than parity_shards. Classify the
	// latter as sidecar integrity, never as mass shard corruption.
	if ecv.ECContext != nil && len(broken) > ecv.ECContext.ParityShards {
		return blocksScanned, nil, []error{fmt.Errorf("ec volume %d: %d/%d local shards mismatch (> parity %d): suspect stale sidecar, not flagging shard corruption",
			ecv.VolumeId, len(broken), len(ecv.Shards), ecv.ECContext.ParityShards)}
	}

	// Reed-Solomon arbitration: a sidecar mismatch could mean the shard rotted
	// OR the sidecar block is stale. When enough OTHER shards are local to
	// reconstruct, reconstruct each flagged shard from the verified-clean shards
	// and compare to its on-disk bytes. Only shards RS *also* disagrees with are
	// truly corrupt; the rest are stale-sidecar false positives we must not
	// quarantine. This is the arbiter that makes auto-repair safe.
	if ecv.ECContext != nil && len(broken) > 0 {
		brokenSet := make(map[ShardId]bool, len(broken))
		for _, s := range broken {
			brokenSet[s.ShardId] = true
		}
		cleanCount := 0
		for _, s := range ecv.Shards {
			if !brokenSet[s.ShardId] {
				cleanCount++
			}
		}
		if cleanCount >= ecv.ECContext.DataShards {
			confirmed := make([]*EcVolumeShard, 0, len(broken))
			for _, s := range broken {
				corrupt, aerr := ecv.rsConfirmsShardCorrupt(s.ShardId, brokenSet, blockSize)
				if aerr != nil {
					// Could not arbitrate this shard; stay conservative and keep it.
					errs = append(errs, fmt.Errorf("ec volume %d shard %d: RS arbitration failed: %v", ecv.VolumeId, s.ShardId, aerr))
					confirmed = append(confirmed, s)
					continue
				}
				if corrupt {
					confirmed = append(confirmed, s)
				} else {
					errs = append(errs, fmt.Errorf("ec volume %d shard %d: sidecar mismatch but Reed-Solomon confirms the bytes are correct: stale sidecar, not flagging shard",
						ecv.VolumeId, s.ShardId))
				}
			}
			broken = confirmed
		}
	}

	for _, s := range broken {
		brokenShards = append(brokenShards, s.ToEcShardInfo())
	}
	slices.SortFunc(brokenShards, func(a, b *volume_server_pb.EcShardInfo) int {
		return int(a.ShardId) - int(b.ShardId)
	})
	return blocksScanned, brokenShards, errs
}

// rsConfirmsShardCorrupt reconstructs targetId from the verified-clean local
// shards (all local shards except those in brokenSet) and compares the result
// to targetId's on-disk bytes block-by-block. It returns true only if
// Reed-Solomon disagrees with the disk (the shard is genuinely corrupt); a full
// match means the shard is fine and its sidecar block is stale. The caller
// guarantees at least DataShards clean shards are local before invoking this.
func (ecv *EcVolume) rsConfirmsShardCorrupt(targetId ShardId, brokenSet map[ShardId]bool, blockSize int64) (bool, error) {
	enc, err := ecv.ECContext.CreateEncoder()
	if err != nil {
		return false, err
	}
	total := ecv.ECContext.Total()
	local := make(map[ShardId]*EcVolumeShard, len(ecv.Shards))
	for _, s := range ecv.Shards {
		local[s.ShardId] = s
	}
	target := local[targetId]
	if target == nil {
		return false, fmt.Errorf("target shard not local")
	}
	size := target.Size()

	buffers := make([][]byte, total)
	diskBuf := make([]byte, blockSize)
	var offset int64
	for offset < size {
		n := blockSize
		if rem := size - offset; rem < n {
			n = rem
		}
		// Fill clean local shards; nil for the target and any broken/absent shard
		// so Reconstruct regenerates the target from the trusted inputs.
		for id := 0; id < total; id++ {
			sid := ShardId(id)
			if sid == targetId || brokenSet[sid] {
				buffers[id] = nil
				continue
			}
			s := local[sid]
			if s == nil {
				buffers[id] = nil
				continue
			}
			buf := make([]byte, n)
			got, rerr := s.ReadAt(buf, offset)
			if rerr != nil || int64(got) != n {
				return false, fmt.Errorf("read clean shard %d at %d: %v (got %d/%d)", sid, offset, rerr, got, n)
			}
			buffers[id] = buf
		}
		if err := enc.Reconstruct(buffers); err != nil {
			return false, fmt.Errorf("reconstruct: %w", err)
		}
		got, rerr := target.ReadAt(diskBuf[:n], offset)
		if rerr != nil || int64(got) != n {
			return false, fmt.Errorf("read target shard %d at %d: %v", targetId, offset, rerr)
		}
		if !bytes.Equal(buffers[targetId][:n], diskBuf[:n]) {
			return true, nil // Reed-Solomon disagrees with disk: genuinely corrupt.
		}
		offset += n
	}
	return false, nil // every block matches: shard is fine, sidecar block is stale.
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
		var hasRemoteChunks bool
		var data []byte

		locations := ecv.LocateEcShardNeedleInterval(ecv.Version, offset.ToActualOffset(), size)

		for i, iv := range locations {
			sid, soffset := iv.ToShardIdAndOffset(ErasureCodingLargeBlockSize, ErasureCodingSmallBlockSize)
			ssize := int64(iv.Size.Raw())
			shard, found := ecv.FindEcVolumeShard(sid)
			if !found {
				// shard is not local :( skip it
				hasRemoteChunks = true
				read += ssize
				continue
			}
			if soffset+int64(ssize) > shard.Size() {
				flagShardBroken(shard, "local shard %d for needle %d is too short (%d), cannot read chunk %d/%d", sid, id, shard.Size(), i+1, len(locations))
				continue
			}

			chunk := make([]byte, ssize)
			got, err := shard.ReadAt(chunk, soffset)
			if err != nil {
				flagShardBroken(shard, "failed to read chunk %d/%d for needle %d from local shard %d at offset %d: %v", i+1, len(locations), id, sid, soffset, err)
				continue
			}
			if int64(got) != ssize {
				flagShardBroken(shard, "expected %d bytes for chunk %d/%d for needle %d from local shard %d, got %d", ssize, i+1, len(locations), id, sid, got)
				continue
			}

			if !hasRemoteChunks {
				data = append(data, chunk...)
			}
			read += int64(got)
		}

		if got, want := read, needle.GetActualSize(size, ecv.Version); got != want {
			return fmt.Errorf("expected %d bytes for needle %d, got %d", want, id, got)
		}
		if !hasRemoteChunks {
			// needle was fully recovered from local shards \o/ let's check it
			n := needle.Needle{}
			if err := n.ReadBytes(data, 0, size, ecv.Version); err != nil {
				errs = append(errs, fmt.Errorf("needle %d on volume %d: %v", id, ecv.VolumeId, err))
			}
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
