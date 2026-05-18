package master_pb

import "sort"

func (v *VolumeLocation) IsEmptyUrl() bool {
	return v.Url == "" || v.Url == ":0"
}

// SplitByPhysicalDisk returns one DiskInfo per physical disk_id observed in
// VolumeInfos / EcShardInfos. The wire format keys DataNodeInfo.DiskInfos by
// disk type, so multiple same-type physical disks on one DataNode collapse
// into a single DiskInfo entry. Per-volume and per-shard records carry the
// real physical DiskId; this helper rebuilds a per-physical-disk view from
// those records so consumers (topology indexes, shell output) can target
// individual disks instead of treating each node as one big disk.
//
// ActiveVolumeCount and RemoteVolumeCount are computed exactly from each
// disk's VolumeInfos (read-only and remote-backed are known per-volume).
// MaxVolumeCount and FreeVolumeCount are not derivable from per-volume
// records, so they are split across reconstructed disks with the remainder
// distributed to the lowest disk ids — the sums are preserved exactly.
func (d *DiskInfo) SplitByPhysicalDisk() []*DiskInfo {
	if d == nil {
		return nil
	}

	normalize := func(id uint32) uint32 {
		if id == 0 && d.DiskId != 0 {
			return d.DiskId
		}
		return id
	}

	diskIDs := make(map[uint32]struct{})
	for _, vi := range d.VolumeInfos {
		diskIDs[normalize(vi.DiskId)] = struct{}{}
	}
	for _, eci := range d.EcShardInfos {
		diskIDs[normalize(eci.DiskId)] = struct{}{}
	}
	if len(diskIDs) == 0 {
		diskIDs[d.DiskId] = struct{}{}
	}

	if len(diskIDs) == 1 {
		for diskID := range diskIDs {
			if diskID == d.DiskId {
				return []*DiskInfo{d}
			}
		}
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
		result = append(result, &DiskInfo{
			Type:              d.Type,
			MaxVolumeCount:    share(d.MaxVolumeCount, i),
			VolumeCount:       int64(len(perDiskVolumes[diskID])),
			FreeVolumeCount:   share(d.FreeVolumeCount, i),
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
