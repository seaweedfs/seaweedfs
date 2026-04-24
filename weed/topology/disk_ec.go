package topology

import (
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func (d *Disk) GetEcShards() (ret []*erasure_coding.EcVolumeInfo) {
	d.ecShardsLock.RLock()
	defer d.ecShardsLock.RUnlock()
	// Total entries = sum of per-volume per-disk entries, which typically
	// exceeds the number of unique volumes once shards span multiple disks.
	total := 0
	for _, byDisk := range d.ecShards {
		total += len(byDisk)
	}
	ret = make([]*erasure_coding.EcVolumeInfo, 0, total)
	for _, byDisk := range d.ecShards {
		for _, ecVolumeInfo := range byDisk {
			ret = append(ret, ecVolumeInfo)
		}
	}
	return ret
}

func (d *Disk) AddOrUpdateEcShard(s *erasure_coding.EcVolumeInfo) {
	d.ecShardsLock.Lock()
	defer d.ecShardsLock.Unlock()

	byDisk, ok := d.ecShards[s.VolumeId]
	if !ok {
		byDisk = make(map[types.DiskId]*erasure_coding.EcVolumeInfo, 1)
		d.ecShards[s.VolumeId] = byDisk
	}

	diskId := types.DiskId(s.DiskId)
	delta := 0
	if existing, ok := byDisk[diskId]; !ok {
		byDisk[diskId] = s
		delta = s.ShardsInfo.Count()
	} else {
		oldCount := existing.ShardsInfo.Count()
		existing.ShardsInfo.Add(s.ShardsInfo)
		delta = existing.ShardsInfo.Count() - oldCount
	}

	if delta != 0 {
		d.UpAdjustDiskUsageDelta(types.ToDiskType(string(d.Id())), &DiskUsageCounts{
			ecShardCount: int64(delta),
		})
	}
}

func (d *Disk) DeleteEcShard(s *erasure_coding.EcVolumeInfo) {
	d.ecShardsLock.Lock()
	defer d.ecShardsLock.Unlock()

	byDisk, ok := d.ecShards[s.VolumeId]
	if !ok {
		return
	}
	diskId := types.DiskId(s.DiskId)
	existing, ok := byDisk[diskId]
	if !ok {
		return
	}
	oldCount := existing.ShardsInfo.Count()
	existing.ShardsInfo.Subtract(s.ShardsInfo)
	delta := existing.ShardsInfo.Count() - oldCount

	if delta != 0 {
		d.UpAdjustDiskUsageDelta(types.ToDiskType(string(d.Id())), &DiskUsageCounts{
			ecShardCount: int64(delta),
		})
	}
	if existing.ShardsInfo.Count() == 0 {
		delete(byDisk, diskId)
		if len(byDisk) == 0 {
			delete(d.ecShards, s.VolumeId)
		}
	}
}

func (d *Disk) HasVolumesById(id needle.VolumeId) (hasVolumeId bool) {
	// check whether normal volumes has this volume id
	d.RLock()
	_, ok := d.volumes[id]
	if ok {
		hasVolumeId = true
	}
	d.RUnlock()

	if hasVolumeId {
		return
	}

	// check whether ec shards has this volume id
	d.ecShardsLock.RLock()
	if byDisk, ok := d.ecShards[id]; ok && len(byDisk) > 0 {
		hasVolumeId = true
	}
	d.ecShardsLock.RUnlock()

	return
}
