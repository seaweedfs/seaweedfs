package master_pb

import "sort"

func (v *VolumeLocation) IsEmptyUrl() bool {
	return v.Url == "" || v.Url == ":0"
}

// SplitByPhysicalDisk returns one DiskInfo per physical disk on the node. The
// wire format keys DataNodeInfo.DiskInfos by disk type, so multiple same-type
// physical disks collapse into a single DiskInfo entry; this helper rebuilds a
// per-physical-disk view so consumers (topology indexes, shell output) can
// target individual disks instead of treating each node as one big disk.
//
// The physical disk set comes from PhysicalDisks when the master reports it —
// it lists every disk of this type, including disks holding no volumes or EC
// shards, so empty disks are no longer invisible — and each disk then reports
// its exact MaxVolumeCount. When PhysicalDisks is absent (older masters) the
// set is the DiskIds observed on VolumeInfos / EcShardInfos and the aggregate
// Max/Free are split evenly with the remainder on the lowest disk ids so the
// sums are preserved.
//
// ActiveVolumeCount and RemoteVolumeCount are always computed exactly from each
// disk's VolumeInfos.
func (d *DiskInfo) SplitByPhysicalDisk() []*DiskInfo {
	if d == nil {
		return nil
	}

	// DiskId 0 is overloaded: it is both the first physical disk (Locations[0])
	// and the protobuf default for "unset". Only treat 0 as unset when every
	// record reports 0 (the legacy case where the volume server didn't populate
	// DiskId). If any record carries a non-zero DiskId, the reporting is real and
	// a 0 means physical disk 0 — keep it distinct instead of folding it onto
	// d.DiskId, which would merge two physical disks and drop disk 0 from view.
	hasNonZero := false
	for _, vi := range d.VolumeInfos {
		if vi.DiskId != 0 {
			hasNonZero = true
			break
		}
	}
	if !hasNonZero {
		for _, eci := range d.EcShardInfos {
			if eci.DiskId != 0 {
				hasNonZero = true
				break
			}
		}
	}

	normalize := func(id uint32) uint32 {
		if id == 0 && !hasNonZero && d.DiskId != 0 {
			return d.DiskId
		}
		return id
	}

	perDiskVolumes := make(map[uint32][]*VolumeInformationMessage)
	for _, vi := range d.VolumeInfos {
		id := normalize(vi.DiskId)
		perDiskVolumes[id] = append(perDiskVolumes[id], vi)
	}
	perDiskShards := make(map[uint32][]*VolumeEcShardInformationMessage)
	for _, eci := range d.EcShardInfos {
		id := normalize(eci.DiskId)
		perDiskShards[id] = append(perDiskShards[id], eci)
	}

	diskIDs := make(map[uint32]struct{})
	for id := range perDiskVolumes {
		diskIDs[id] = struct{}{}
	}
	for id := range perDiskShards {
		diskIDs[id] = struct{}{}
	}

	// PhysicalDisks, when reported, is the authoritative full set: it includes
	// disks with no volumes or EC shards and carries each disk's exact max.
	exactMax := len(d.PhysicalDisks) > 0
	maxByID := make(map[uint32]int64, len(d.PhysicalDisks))
	for _, pd := range d.PhysicalDisks {
		diskIDs[pd.DiskId] = struct{}{}
		maxByID[pd.DiskId] = pd.MaxVolumeCount
	}

	if len(diskIDs) == 0 {
		diskIDs[d.DiskId] = struct{}{}
	}

	// Fast path: a single physical disk identical to the aggregate needs no
	// reconstruction. Skip it when PhysicalDisks is present so the disk reports
	// its exact max rather than the aggregate.
	if !exactMax && len(diskIDs) == 1 {
		for diskID := range diskIDs {
			if diskID == d.DiskId {
				return []*DiskInfo{d}
			}
		}
	}

	// Sort disk IDs so the remainder distribution is deterministic and the
	// reconstructed slice is in DiskId order, which is what downstream
	// renderers expect.
	ids := make([]uint32, 0, len(diskIDs))
	for id := range diskIDs {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	count := int64(len(ids))
	// share returns total / count, plus one extra for the first
	// (total % count) entries so the sum of shares equals total. Without
	// the remainder distribution, splitting 10 across 3 disks would yield
	// 3+3+3 = 9 and under-report aggregate capacity.
	share := func(total int64, idx int) int64 {
		base := total / count
		if int64(idx) < total%count {
			return base + 1
		}
		return base
	}

	result := make([]*DiskInfo, 0, len(ids))
	for i, diskID := range ids {
		var activeCount, remoteCount int64
		for _, vi := range perDiskVolumes[diskID] {
			if !vi.ReadOnly {
				activeCount++
			}
			if vi.RemoteStorageName != "" {
				remoteCount++
			}
		}
		volumeCount := int64(len(perDiskVolumes[diskID]))
		maxVolumeCount := share(d.MaxVolumeCount, i)
		freeVolumeCount := share(d.FreeVolumeCount, i)
		if exactMax {
			maxVolumeCount = maxByID[diskID]
			// Free derived from the exact max. EC shard slots are not subtracted
			// here (that needs the configurable EC ratio, which lives outside
			// this package); capacity planners subtract them from EcShardInfos.
			freeVolumeCount = maxVolumeCount - (volumeCount - remoteCount)
		}
		result = append(result, &DiskInfo{
			Type:              d.Type,
			MaxVolumeCount:    maxVolumeCount,
			VolumeCount:       volumeCount,
			FreeVolumeCount:   freeVolumeCount,
			ActiveVolumeCount: activeCount,
			RemoteVolumeCount: remoteCount,
			VolumeInfos:       perDiskVolumes[diskID],
			EcShardInfos:      perDiskShards[diskID],
			DiskId:            diskID,
			Tags:              append([]string(nil), d.Tags...),
		})
	}
	return result
}
