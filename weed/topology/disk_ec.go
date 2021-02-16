package topology

import (
	"github.com/chrislusf/seaweedfs/weed/storage/erasure_coding"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/storage/types"
)

func (d *Disk) GetEcShards() (ret []*erasure_coding.EcVolumeInfo) {
	d.RLock()
	for _, ecVolumeInfo := range d.ecShards {
		ret = append(ret, ecVolumeInfo)
	}
	d.RUnlock()
	return ret
}

func (d *Disk) AddOrUpdateEcShard(s *erasure_coding.EcVolumeInfo) {
	d.ecShardsLock.Lock()
	defer d.ecShardsLock.Unlock()

	delta := 0
	if existing, ok := d.ecShards[s.VolumeId]; !ok {
		d.ecShards[s.VolumeId] = s
		delta = s.ShardBits.ShardIdCount()
	} else {
		oldCount := existing.ShardBits.ShardIdCount()
		existing.ShardBits = existing.ShardBits.Plus(s.ShardBits)
		delta = existing.ShardBits.ShardIdCount() - oldCount
	}

	deltaDiskUsages := newDiskUsages()
	deltaDiskUsage := deltaDiskUsages.getOrCreateDisk(types.ToDiskType(string(d.Id())))
	deltaDiskUsage.ecShardCount = int64(delta)
	d.UpAdjustDiskUsageDelta(deltaDiskUsages)

}

func (d *Disk) DeleteEcShard(s *erasure_coding.EcVolumeInfo) {
	d.ecShardsLock.Lock()
	defer d.ecShardsLock.Unlock()

	if existing, ok := d.ecShards[s.VolumeId]; ok {
		oldCount := existing.ShardBits.ShardIdCount()
		existing.ShardBits = existing.ShardBits.Minus(s.ShardBits)
		delta := existing.ShardBits.ShardIdCount() - oldCount

		deltaDiskUsages := newDiskUsages()
		deltaDiskUsage := deltaDiskUsages.getOrCreateDisk(types.ToDiskType(string(d.Id())))
		deltaDiskUsage.ecShardCount = int64(delta)
		d.UpAdjustDiskUsageDelta(deltaDiskUsages)

		if existing.ShardBits.ShardIdCount() == 0 {
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
