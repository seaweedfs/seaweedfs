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
