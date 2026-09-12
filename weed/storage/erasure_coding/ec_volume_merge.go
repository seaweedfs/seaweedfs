package erasure_coding

import (
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
)

// MergedEcRuntimes resolves one volume id's per-disk runtimes into a single
// scrubbable view. Every scrub mode builds from this, so there is exactly one
// answer to "which disks count" per volume.
//
// Two fences admit a runtime into Merged: the anchor's EncodeTsNs (the
// maximum, so 0 implies every runtime is 0 and nothing is excluded — legacy
// leniency), and the anchor's geometry (DataShards, ParityShards, BlockSize).
// Equal timestamps do not guarantee equal layouts, so a same-generation
// runtime whose .vif disagrees is excluded and reported rather than merged
// into a plan that would apply the anchor's offsets and checksums to
// incompatible shards.
type MergedEcRuntimes struct {
	// Anchor is the volume-level metadata source: geometry, .ecx handles,
	// version. It is NOT the bitrot protection source — the .ecsum sidecar is
	// per-DISK state, so ChecksumScrubMerged sources it from the first Merged
	// runtime that has any.
	Anchor *EcVolume
	// Merged holds the runtimes whose shards are safe to verify together.
	Merged []*EcVolume
	// Slots is indexed BY SHARD ID. A nil entry is a shard no merged runtime
	// holds. The volume's shard-id range is the anchor's geometry
	// (0..DataShards+ParityShards); consumers truncate to it.
	Slots []*EcVolumeShard
	// Skipped holds one line per runtime excluded by the identity or geometry
	// fence. Reported, never dropped.
	Skipped []string
}

// MergeEcRuntimes resolves runtimes (one vid's mounts, in location order) into
// a merged view. Returns nil for an empty slice (the vanished-volume case).
func MergeEcRuntimes(runtimes []*EcVolume) *MergedEcRuntimes {
	if len(runtimes) == 0 {
		return nil
	}

	anchorGen := int64(0)
	for _, v := range runtimes {
		if v.EncodeTsNs > anchorGen {
			anchorGen = v.EncodeTsNs
		}
	}

	var genMatches []*EcVolume
	for _, v := range runtimes {
		if v.EncodeTsNs == anchorGen {
			genMatches = append(genMatches, v)
		}
	}

	anchor := genMatches[0]
	for _, v := range genMatches {
		if len(v.Shards) > 0 {
			anchor = v
			break
		}
	}

	var merged []*EcVolume
	for _, v := range genMatches {
		if geometryMatches(v, anchor) {
			merged = append(merged, v)
		}
	}

	total := ecDataShards(anchor) + ecParityShards(anchor)
	width := total
	for _, v := range merged {
		if n := int(v.Shards[len(v.Shards)-1].ShardId) + 1; n > width {
			width = n
		}
	}
	slots := make([]*EcVolumeShard, width)
	for _, v := range merged {
		for _, shard := range v.Shards {
			id := int(shard.ShardId)
			if id >= len(slots) {
				continue
			}
			if slots[id] == nil {
				slots[id] = shard
			}
		}
	}

	var skipped []string
	for pos, v := range runtimes {
		if v.EncodeTsNs != anchorGen {
			skipped = append(skipped, fmt.Sprintf(
				"EC volume %d shards at %s (position %d) belong to encode run %d but the scrub anchors on %d; they were not verified",
				v.VolumeId, v.dir, pos, v.EncodeTsNs, anchorGen))
			continue
		}
		if !geometryMatches(v, anchor) {
			skipped = append(skipped, fmt.Sprintf(
				"EC volume %d shards at %s (position %d) share encode run %d but disagree on geometry (%d+%d bs %d vs %d+%d bs %d); they were not verified",
				v.VolumeId, v.dir, pos, v.EncodeTsNs,
				ecDataShards(v), ecParityShards(v), ecBlockSize(v),
				ecDataShards(anchor), ecParityShards(anchor), ecBlockSize(anchor)))
		}
	}

	return &MergedEcRuntimes{
		Anchor:  anchor,
		Merged:  merged,
		Slots:   slots,
		Skipped: skipped,
	}
}

func geometryMatches(a, b *EcVolume) bool {
	return ecDataShards(a) == ecDataShards(b) &&
		ecParityShards(a) == ecParityShards(b) &&
		ecBlockSize(a) == ecBlockSize(b)
}

func ecDataShards(v *EcVolume) int {
	if v.ECContext != nil {
		return v.ECContext.DataShards
	}
	return 0
}

func ecParityShards(v *EcVolume) int {
	if v.ECContext != nil {
		return v.ECContext.ParityShards
	}
	return 0
}

func ecBlockSize(v *EcVolume) int64 {
	if v.ECContext != nil {
		return v.ECContext.BlockSize
	}
	return 0
}

// asVolume returns a synthetic EcVolume that shares the anchor's volume-level
// state (ecx handles, version, geometry, bitrot protection) but presents the
// merged shard set. ScrubLocal/ChecksumScrub read Shards, FindEcVolumeShard,
// ECContext, ecxFile, BitrotProtection and Version — all of which the anchor
// supplies except Shards, which is rebuilt from the merged slots.
//
// For legacy volumes (no datFileSize in .vif), LocateEcShardNeedleInterval
// derives the shard size from Shards[0].ecdFileSize. The merged shard set is
// compacted in shard-ID order, so a truncated lowest-ID shard would shrink
// every interval and misread intact sibling shards. To prevent that, asVolume
// synthesizes a datFileSize from the maximum mounted shard size when the
// anchor lacks one, so the datFileSize>0 path in LocateEcShardNeedleInterval
// uses the largest shard's size across all merged runtimes.
func (m *MergedEcRuntimes) asVolume() *EcVolume {
	anchor := m.Anchor
	shards := make([]*EcVolumeShard, 0, len(m.Slots))
	for _, s := range m.Slots {
		if s != nil {
			shards = append(shards, s)
		}
	}
	datFileSize := anchor.datFileSize
	if datFileSize == 0 && anchor.ECContext != nil && anchor.ECContext.DataShards > 0 {
		var maxShardSize int64
		for _, s := range shards {
			if s.ecdFileSize > maxShardSize {
				maxShardSize = s.ecdFileSize
			}
		}
		// Subtract 1 to match the legacy fallback in LocateEcShardNeedleInterval
		// (ecdFileSize - 1): an exact large-block boundary is ambiguous, and
		// the unadjusted size would select an extra large row.
		if maxShardSize > 0 {
			datFileSize = (maxShardSize - 1) * int64(anchor.ECContext.DataShards)
		}
	}
	return &EcVolume{
		VolumeId:     anchor.VolumeId,
		Collection:   anchor.Collection,
		dir:          anchor.dir,
		dirIdx:       anchor.dirIdx,
		ecxActualDir: anchor.ecxActualDir,
		ecxFile:      anchor.ecxFile,
		ecxFileSize:  anchor.ecxFileSize,
		ecxCreatedAt: anchor.ecxCreatedAt,
		Shards:       shards,
		Version:      anchor.Version,
		diskType:     anchor.diskType,
		datFileSize:  datFileSize,
		ECContext:    anchor.ECContext,
		EncodeTsNs:   anchor.EncodeTsNs,
		bitrot:       anchor.bitrot,
		bitrotStatus: anchor.bitrotStatus,
	}
}

// ScrubLocal checks the integrity of local shards across every merged
// runtime, mirroring EcVolume.ScrubLocal over the merged shard set. Skipped
// runtimes are reported alongside any scrub errors.
func (m *MergedEcRuntimes) ScrubLocal() (int64, []*volume_server_pb.EcShardInfo, []error) {
	files, shardInfos, errs := m.asVolume().ScrubLocal()
	for _, s := range m.Skipped {
		errs = append(errs, fmt.Errorf("%s", s))
	}
	return files, shardInfos, errs
}

// ChecksumScrub verifies every merged runtime's local shards against the
// bitrot sidecar, mirroring EcVolume.ChecksumScrub. The sidecar is per-DISK
// state: protection is taken from the first merged runtime that has any (On,
// else Invalid, else the anchor's Off), and every merged runtime whose
// sidecar resolved Invalid is reported even when protection is taken from a
// sibling that is On. Skipped runtimes are reported alongside any scrub
// errors.
func (m *MergedEcRuntimes) ChecksumScrub() (int64, []*volume_server_pb.EcShardInfo, []error) {
	// Pick the protection source: first On, else first Invalid, else anchor.
	var protectionSource *EcVolume
	for _, v := range m.Merged {
		if _, status := v.BitrotProtection(); status == BitrotOn {
			protectionSource = v
			break
		}
	}
	if protectionSource == nil {
		for _, v := range m.Merged {
			if _, status := v.BitrotProtection(); status == BitrotInvalid {
				protectionSource = v
				break
			}
		}
	}
	if protectionSource == nil {
		protectionSource = m.Anchor
	}

	prot, status := protectionSource.BitrotProtection()

	// Fence the sidecar's encode generation: a merged runtime can load a
	// sidecar from a sibling metadata directory (ReloadBitrotSidecar), and
	// the merge fence may then exclude the runtime owning that directory.
	// Generation-0 sidecars do not identify the encode run, so geometry
	// validation alone cannot prove the borrowed manifest describes the
	// anchor's shards. If the sidecar records a non-zero EncodeTsNs that
	// disagrees with the anchor's, scanning would apply stale checksums to
	// current shards and report false corruption. Refuse instead.
	if status == BitrotOn && prot != nil && prot.EcShardConfig != nil {
		sidecarGen := prot.EcShardConfig.EncodeTsNs
		if sidecarGen != 0 && m.Anchor.EncodeTsNs != 0 && sidecarGen != m.Anchor.EncodeTsNs {
			errs := []error{fmt.Errorf(
				"ec volume %d: bitrot sidecar at %s records encode run %d but the scrub anchors on %d; protection is unverifiable",
				m.Anchor.VolumeId, protectionSource.dir, sidecarGen, m.Anchor.EncodeTsNs)}
			for _, rt := range m.Merged {
				if rt == protectionSource {
					continue
				}
				if _, s := rt.BitrotProtection(); s == BitrotInvalid {
					errs = append(errs, fmt.Errorf(
						"ec volume %d bitrot sidecar at %s is malformed/unverifiable (sidecar integrity)",
						rt.VolumeId, rt.dir))
				}
			}
			for _, s := range m.Skipped {
				errs = append(errs, fmt.Errorf("%s", s))
			}
			return 0, nil, errs
		}
	}

	// Run the byte scan against the protection source's sidecar, but over
	// the merged shard set. The synthetic volume inherits the protection
	// source's bitrot state so ChecksumScrub's BitrotOff/Invalid arms fire.
	v := m.asVolume()
	v.bitrot = prot
	v.bitrotStatus = status

	blocks, broken, errs := v.ChecksumScrub()

	// Collect Invalid-sidecar errors from every merged runtime except the
	// protection source (whose error the Invalid arm already reports), so a
	// malformed sidecar is never silently discarded when a sibling is On.
	if status != BitrotInvalid {
		for _, rt := range m.Merged {
			if rt == protectionSource {
				continue
			}
			if _, s := rt.BitrotProtection(); s == BitrotInvalid {
				errs = append(errs, fmt.Errorf(
					"ec volume %d bitrot sidecar at %s is malformed/unverifiable (sidecar integrity)",
					rt.VolumeId, rt.dir))
			}
		}
	}
	for _, s := range m.Skipped {
		errs = append(errs, fmt.Errorf("%s", s))
	}
	return blocks, broken, errs
}
