package topology

import (
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// GetEcShards returns the stored records themselves, not copies. Callers
// read them after the lock is released, which is safe only because no stored
// record is ever written in place: every change swaps a replacement into the
// map, so a caller holding an old pointer sees one heartbeat's snapshot.
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
		// Build the merged record beside the stored one and swap it in;
		// see GetEcShards for why the stored one must stay as it is.
		updated := *existing
		updated.ShardsInfo = existing.ShardsInfo.Plus(s.ShardsInfo)
		delta = updated.ShardsInfo.Count() - existing.ShardsInfo.Count()
		// 0/0 means the message did not count (older volume servers omit the fields)
		if s.FileCount != 0 || s.DeleteCount != 0 {
			updated.FileCount, updated.DeleteCount = s.FileCount, s.DeleteCount
		}
		byDisk[diskId] = &updated
	}

	if delta != 0 {
		d.UpAdjustDiskUsageDelta(types.ToDiskType(string(d.Id())), &DiskUsageCounts{
			ecShardCount: int64(delta),
		})
	}
}

// refreshEcShardCounts publishes the counts a full heartbeat reported for one
// (volume, disk) record. The stored record is left untouched: a copy carrying
// the new counts takes its place, so a reader that already holds the old
// pointer keeps reading one heartbeat's FileCount/DeleteCount pair. The
// comparison runs here, under the lock, against whatever record is current,
// since the incremental path may have replaced it since the caller looked.
func (d *Disk) refreshEcShardCounts(vid needle.VolumeId, diskId types.DiskId, fileCount, deleteCount uint64) {
	d.ecShardsLock.Lock()
	defer d.ecShardsLock.Unlock()

	existing, ok := d.ecShards[vid][diskId]
	if !ok || (existing.FileCount == fileCount && existing.DeleteCount == deleteCount) {
		return
	}
	updated := *existing
	updated.FileCount, updated.DeleteCount = fileCount, deleteCount
	d.ecShards[vid][diskId] = &updated
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
	updated := *existing
	updated.ShardsInfo = existing.ShardsInfo.Minus(s.ShardsInfo)
	delta := updated.ShardsInfo.Count() - existing.ShardsInfo.Count()

	if delta != 0 {
		d.UpAdjustDiskUsageDelta(types.ToDiskType(string(d.Id())), &DiskUsageCounts{
			ecShardCount: int64(delta),
		})
	}
	if updated.ShardsInfo.Count() == 0 {
		delete(byDisk, diskId)
		if len(byDisk) == 0 {
			delete(d.ecShards, s.VolumeId)
		}
	} else if delta != 0 {
		byDisk[diskId] = &updated
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
