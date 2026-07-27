package topology

import (
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

type EcShardLocations struct {
	Collection string
	// Use MaxShardCount (32) to support custom EC ratios
	Locations [erasure_coding.MaxShardCount][]*DataNode
}

func (t *Topology) SyncDataNodeEcShards(shardInfos []*master_pb.VolumeEcShardInformationMessage, dn *DataNode) (newShards, deletedShards []*erasure_coding.EcVolumeInfo) {
	// convert into in memory struct storage.VolumeInfo
	var shards []*erasure_coding.EcVolumeInfo
	for _, shardInfo := range shardInfos {
		// Create EcVolumeInfo directly with optimized format
		ecVolumeInfo := &erasure_coding.EcVolumeInfo{
			VolumeId:    needle.VolumeId(shardInfo.Id),
			Collection:  shardInfo.Collection,
			ShardsInfo:  erasure_coding.ShardsInfoFromVolumeEcShardInformationMessage(shardInfo),
			DiskType:    shardInfo.DiskType,
			DiskId:      shardInfo.DiskId,
			ExpireAtSec: shardInfo.ExpireAtSec,
			FileCount:   shardInfo.FileCount,
			DeleteCount: shardInfo.DeleteCount,
			EncodeTsNs:  shardInfo.EncodeTsNs,
		}

		shards = append(shards, ecVolumeInfo)
	}
	// find out the delta volumes
	newShards, deletedShards = dn.UpdateEcShards(shards)
	for _, v := range newShards {
		t.RegisterEcShards(v, dn)
	}
	for _, v := range deletedShards {
		t.UnRegisterEcShards(v, dn)
	}
	return
}

func (t *Topology) IncrementalSyncDataNodeEcShards(newEcShards, deletedEcShards []*master_pb.VolumeEcShardInformationMessage, dn *DataNode) {
	// convert into in memory struct storage.VolumeInfo
	var newShards, deletedShards []*erasure_coding.EcVolumeInfo
	for _, shardInfo := range newEcShards {
		// Create EcVolumeInfo directly with optimized format
		ecVolumeInfo := &erasure_coding.EcVolumeInfo{
			VolumeId:    needle.VolumeId(shardInfo.Id),
			Collection:  shardInfo.Collection,
			ShardsInfo:  erasure_coding.ShardsInfoFromVolumeEcShardInformationMessage(shardInfo),
			DiskType:    shardInfo.DiskType,
			DiskId:      shardInfo.DiskId,
			ExpireAtSec: shardInfo.ExpireAtSec,
			FileCount:   shardInfo.FileCount,
			DeleteCount: shardInfo.DeleteCount,
			EncodeTsNs:  shardInfo.EncodeTsNs,
		}

		newShards = append(newShards, ecVolumeInfo)
	}
	for _, shardInfo := range deletedEcShards {
		// Create EcVolumeInfo directly with optimized format
		ecVolumeInfo := &erasure_coding.EcVolumeInfo{
			VolumeId:    needle.VolumeId(shardInfo.Id),
			Collection:  shardInfo.Collection,
			ShardsInfo:  erasure_coding.ShardsInfoFromVolumeEcShardInformationMessage(shardInfo),
			DiskType:    shardInfo.DiskType,
			DiskId:      shardInfo.DiskId,
			ExpireAtSec: shardInfo.ExpireAtSec,
			FileCount:   shardInfo.FileCount,
			DeleteCount: shardInfo.DeleteCount,
			EncodeTsNs:  shardInfo.EncodeTsNs,
		}

		deletedShards = append(deletedShards, ecVolumeInfo)
	}

	dn.DeltaUpdateEcShards(newShards, deletedShards)

	for _, v := range newShards {
		t.RegisterEcShards(v, dn)
	}
	for _, v := range deletedShards {
		t.UnRegisterEcShards(v, dn)
	}
}

func NewEcShardLocations(collection string) *EcShardLocations {
	return &EcShardLocations{
		Collection: collection,
	}
}

func (loc *EcShardLocations) AddShard(shardId erasure_coding.ShardId, dn *DataNode) (added bool) {
	// Defensive bounds check to prevent panic with out-of-range shard IDs
	if int(shardId) >= erasure_coding.MaxShardCount {
		return false
	}
	dataNodes := loc.Locations[shardId]
	for _, n := range dataNodes {
		if n.Id() == dn.Id() {
			return false
		}
	}
	loc.Locations[shardId] = append(dataNodes, dn)
	return true
}

func (loc *EcShardLocations) DeleteShard(shardId erasure_coding.ShardId, dn *DataNode) (deleted bool) {
	// Defensive bounds check to prevent panic with out-of-range shard IDs
	if int(shardId) >= erasure_coding.MaxShardCount {
		return false
	}
	dataNodes := loc.Locations[shardId]
	foundIndex := -1
	for index, n := range dataNodes {
		if n.Id() == dn.Id() {
			foundIndex = index
		}
	}
	if foundIndex < 0 {
		return false
	}
	loc.Locations[shardId] = append(dataNodes[:foundIndex], dataNodes[foundIndex+1:]...)
	return true
}

func (t *Topology) RegisterEcShards(ecvi *erasure_coding.EcVolumeInfo, dn *DataNode) {

	// EC-only volumes (source volume deleted after encoding) must bump
	// maxVolumeId too, or a heartbeat-rebuilt master could re-issue their id.
	t.UpAdjustMaxVolumeId(ecvi.VolumeId)

	t.ecShardMapLock.Lock()
	defer t.ecShardMapLock.Unlock()

	locations, found := t.ecShardMap[ecvi.VolumeId]
	if !found {
		locations = NewEcShardLocations(ecvi.Collection)
		t.ecShardMap[ecvi.VolumeId] = locations
	}
	for _, shardId := range ecvi.ShardsInfo.Ids() {
		locations.AddShard(shardId, dn)
	}
}

func (t *Topology) UnRegisterEcShards(ecvi *erasure_coding.EcVolumeInfo, dn *DataNode) {
	glog.Infof("removing ec shard info:%+v", ecvi)
	t.ecShardMapLock.Lock()
	defer t.ecShardMapLock.Unlock()

	locations, found := t.ecShardMap[ecvi.VolumeId]
	if !found {
		return
	}
	for _, shardId := range ecvi.ShardsInfo.Ids() {
		locations.DeleteShard(shardId, dn)
	}
}

func (t *Topology) LookupEcShards(vid needle.VolumeId) (locations *EcShardLocations, found bool) {
	t.ecShardMapLock.RLock()
	defer t.ecShardMapLock.RUnlock()

	locations, found = t.ecShardMap[vid]

	return
}

// ecVolumeCounts accumulates one EC volume's needle counts while they are
// collected from every node reporting its shards.
type ecVolumeCounts struct {
	fileCount   uint64
	deleteCount uint64
}

// CollectionEcVolumeStats sums the disk footprint and live needle count of the
// EC volumes in one collection, or in every collection when collectionName is
// empty. Every shard copy counts, parity included, the way a regular volume's
// used size counts every replica; needle counts are per volume, again as a
// regular volume reports them.
func (t *Topology) CollectionEcVolumeStats(collectionName string) *VolumeLayoutStats {
	ret := &VolumeLayoutStats{}
	perVolume := make(map[needle.VolumeId]*ecVolumeCounts)

	for _, c := range t.Children() {
		for _, r := range c.(*DataCenter).Children() {
			for _, n := range r.(*Rack).Children() {
				for _, ecInfo := range n.(*DataNode).GetEcShards() {
					if collectionName != "" && ecInfo.Collection != collectionName {
						continue
					}
					ret.UsedSize += uint64(ecInfo.ShardsInfo.TotalSize())
					counts, found := perVolume[ecInfo.VolumeId]
					if !found {
						counts = &ecVolumeCounts{}
						perVolume[ecInfo.VolumeId] = counts
					}
					// .ecx and .ecj are both volume-wide files that travel with
					// the shards, so take the largest count any holder reports
					// rather than summing: a node still loading .ecx reports 0
					// and must not pin the total down, and a shard move copies
					// the journal, so several holders can report the same
					// tombstones. Deletes recorded only on another holder since
					// then are missed, which errs toward reporting files that
					// are gone rather than losing a whole volume's count.
					if ecInfo.FileCount > counts.fileCount {
						counts.fileCount = ecInfo.FileCount
					}
					if ecInfo.DeleteCount > counts.deleteCount {
						counts.deleteCount = ecInfo.DeleteCount
					}
				}
			}
		}
	}

	// an EC volume is sealed, so it offers no room beyond what it holds
	ret.TotalSize = ret.UsedSize
	for _, counts := range perVolume {
		if counts.fileCount > counts.deleteCount {
			ret.FileCount += counts.fileCount - counts.deleteCount
		}
	}
	return ret
}

func (t *Topology) ListEcServersByCollection(collection string) (dataNodes []pb.ServerAddress) {
	t.ecShardMapLock.RLock()
	defer t.ecShardMapLock.RUnlock()

	dateNodeMap := make(map[pb.ServerAddress]bool)
	for _, ecVolumeLocation := range t.ecShardMap {
		if ecVolumeLocation.Collection == collection {
			for _, locations := range ecVolumeLocation.Locations {
				for _, loc := range locations {
					dateNodeMap[loc.ServerAddress()] = true
				}
			}
		}
	}

	for k, _ := range dateNodeMap {
		dataNodes = append(dataNodes, k)
	}

	return
}

func (t *Topology) DeleteEcCollection(collection string) {
	t.ecShardMapLock.Lock()
	defer t.ecShardMapLock.Unlock()

	var vids []needle.VolumeId
	for vid, ecVolumeLocation := range t.ecShardMap {
		if ecVolumeLocation.Collection == collection {
			vids = append(vids, vid)
		}
	}

	for _, vid := range vids {
		delete(t.ecShardMap, vid)
	}
}
