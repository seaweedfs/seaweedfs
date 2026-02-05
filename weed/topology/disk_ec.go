package topology

import (
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func (d *Disk) GetEcShards() (ret []*erasure_coding.EcVolumeInfo) {
	d.ecShardsLock.RLock()
	defer d.ecShardsLock.RUnlock()
	ret = make([]*erasure_coding.EcVolumeInfo, 0, len(d.ecShards))
	for _, ecVolumeInfo := range d.ecShards {
		ret = append(ret, ecVolumeInfo)
	}
	return ret
}

func (d *Disk) AddOrUpdateEcShard(s *erasure_coding.EcVolumeInfo) {
	d.ecShardsLock.Lock()
	defer d.ecShardsLock.Unlock()

	delta := 0
	if existing, ok := d.ecShards[s.VolumeId]; !ok {
		d.ecShards[s.VolumeId] = s
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

	if existing, ok := d.ecShards[s.VolumeId]; ok {
		oldCount := existing.ShardsInfo.Count()
		existing.ShardsInfo.Subtract(s.ShardsInfo)
		delta := existing.ShardsInfo.Count() - oldCount

		if delta != 0 {
			d.UpAdjustDiskUsageDelta(types.ToDiskType(string(d.Id())), &DiskUsageCounts{
				ecShardCount: int64(delta),
			})
		}
		if existing.ShardsInfo.Count() == 0 {
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
	_, ok = d.ecShards[id]
	if ok {
		hasVolumeId = true
	}
	d.ecShardsLock.RUnlock()

	return
}
