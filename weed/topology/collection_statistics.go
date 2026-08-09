package topology

import (
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// CollectionStatistics is what a collection holds, summarised so that callers
// tracking usage do not have to be sent every volume in the cluster to add it
// up themselves.
type CollectionStatistics struct {
	Collection       string
	FileCount        uint64
	DeleteCount      uint64
	DeletedByteCount uint64
	// Size counts one copy of the data: a single replica of a regular volume,
	// the data shards of an ec volume.
	Size uint64
	// PhysicalSize counts what is on disk: every replica, and parity shards.
	PhysicalSize uint64
	VolumeCount  uint64
}

type ecStatsKey struct {
	collection string
	volumeId   needle.VolumeId
}

// ecFileCounts holds the per-volume counts that can only be resolved once every
// shard holder has been seen.
type ecFileCounts struct {
	collection  string
	fileCount   uint64
	deleteCount uint64
}

// CollectionStatistics summarises every collection in one pass over the
// topology, allocating per collection rather than per volume.
func (t *Topology) CollectionStatistics() []*CollectionStatistics {
	byCollection := make(map[string]*CollectionStatistics)
	statsFor := func(collection string) *CollectionStatistics {
		stats, found := byCollection[collection]
		if !found {
			stats = &CollectionStatistics{Collection: collection}
			byCollection[collection] = stats
		}
		return stats
	}

	// Regular volumes are counted once each for logical totals and once per
	// replica for physical, which the lookup index gives without a set of seen
	// ids: it is already keyed by volume.
	for _, c := range t.collectionMap.Items() {
		collection := c.(*Collection)
		for _, vl := range collection.GetAllVolumeLayouts() {
			vl.accessLock.RLock()
			for vid, locations := range vl.vid2location {
				stats := statsFor(collection.Name)
				// Replicas of one volume can disagree while a write is landing
				// or a heartbeat is late. Count the one holding the most live
				// data, so the answer does not depend on which replica is
				// looked at first and usage is never reported lower than some
				// replica already holds. Quotas are enforced on size less
				// deletions, so that is what has to be the largest -- a replica
				// with the biggest raw size can be the one that has deleted the
				// most, and picking it would leave an over-quota bucket
				// writable.
				var largest storage.VolumeInfo
				var largestLive uint64
				found := false
				for _, dn := range locations.list {
					v, err := dn.GetVolumesById(vid)
					if err != nil {
						continue
					}
					stats.PhysicalSize += v.Size
					live := v.Size
					if v.DeletedByteCount < live {
						live -= v.DeletedByteCount
					} else {
						live = 0
					}
					if !found || live > largestLive {
						largest, largestLive, found = v, live, true
					}
				}
				if !found {
					continue
				}
				stats.Size += largest.Size
				stats.FileCount += uint64(largest.FileCount)
				stats.DeleteCount += uint64(largest.DeleteCount)
				stats.DeletedByteCount += largest.DeletedByteCount
				stats.VolumeCount++
			}
			vl.accessLock.RUnlock()
		}
	}

	// Ec shards are node-local rather than replicated, so their sizes sum
	// across holders. The file and delete counts describe the volume rather
	// than the shard, so they resolve once every holder has been seen.
	perEcVolume := make(map[ecStatsKey]*ecFileCounts)
	for _, dcNode := range t.Children() {
		for _, rackNode := range dcNode.(*DataCenter).Children() {
			for _, dnNode := range rackNode.(*Rack).Children() {
				for _, ecInfo := range dnNode.(*DataNode).GetEcShards() {
					message := ecInfo.ToVolumeEcShardInformationMessage()
					stats := statsFor(ecInfo.Collection)
					stats.PhysicalSize += uint64(erasure_coding.EcShardsTotalSize(message))
					stats.Size += uint64(erasure_coding.EcShardsDataSize(message, 0))

					key := ecStatsKey{collection: ecInfo.Collection, volumeId: ecInfo.VolumeId}
					counts, found := perEcVolume[key]
					if !found {
						counts = &ecFileCounts{collection: ecInfo.Collection}
						perEcVolume[key] = counts
						stats.VolumeCount++
					}
					if message.FileCount > counts.fileCount {
						counts.fileCount = message.FileCount
					}
					counts.deleteCount += message.DeleteCount
				}
			}
		}
	}
	for _, counts := range perEcVolume {
		stats := byCollection[counts.collection]
		if stats == nil {
			continue
		}
		stats.FileCount += counts.fileCount
		stats.DeleteCount += counts.deleteCount
	}

	ret := make([]*CollectionStatistics, 0, len(byCollection))
	for _, stats := range byCollection {
		ret = append(ret, stats)
	}
	return ret
}
