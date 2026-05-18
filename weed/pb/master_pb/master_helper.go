package master_pb

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
// Capacity counters (MaxVolumeCount, FreeVolumeCount, ActiveVolumeCount,
// RemoteVolumeCount) are split evenly across the reconstructed disks — the
// wire format does not carry per-disk capacity, and even split is the
// closest approximation that preserves the node-level totals.
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

	count := int64(len(diskIDs))
	share := func(total int64) int64 { return total / count }

	result := make([]*DiskInfo, 0, len(diskIDs))
	for diskID := range diskIDs {
		result = append(result, &DiskInfo{
			Type:              d.Type,
			MaxVolumeCount:    share(d.MaxVolumeCount),
			VolumeCount:       int64(len(perDiskVolumes[diskID])),
			FreeVolumeCount:   share(d.FreeVolumeCount),
			ActiveVolumeCount: share(d.ActiveVolumeCount),
			RemoteVolumeCount: share(d.RemoteVolumeCount),
			VolumeInfos:       perDiskVolumes[diskID],
			EcShardInfos:      perDiskShards[diskID],
			DiskId:            diskID,
			Tags:              append([]string(nil), d.Tags...),
		})
	}
	return result
}
