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
	Locations  [erasure_coding.TotalShardsCount][]*DataNode
}

func (t *Topology) SyncDataNodeEcShards(shardInfos []*master_pb.VolumeEcShardInformationMessage, dn *DataNode) (newShards, deletedShards []*erasure_coding.EcVolumeInfo) {
	// convert into in memory struct storage.VolumeInfo
	var shards []*erasure_coding.EcVolumeInfo
	for _, shardInfo := range shardInfos {
		shards = append(shards,
			erasure_coding.NewEcVolumeInfo(
				shardInfo.DiskType,
				shardInfo.Collection,
				needle.VolumeId(shardInfo.Id),
				erasure_coding.ShardBits(shardInfo.EcIndexBits),
				shardInfo.ExpireAtSec))
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
		newShards = append(newShards,
			erasure_coding.NewEcVolumeInfo(
				shardInfo.DiskType,
				shardInfo.Collection,
				needle.VolumeId(shardInfo.Id),
				erasure_coding.ShardBits(shardInfo.EcIndexBits), shardInfo.ExpireAtSec))
	}
	for _, shardInfo := range deletedEcShards {
		deletedShards = append(deletedShards,
			erasure_coding.NewEcVolumeInfo(
				shardInfo.DiskType,
				shardInfo.Collection,
				needle.VolumeId(shardInfo.Id),
				erasure_coding.ShardBits(shardInfo.EcIndexBits), shardInfo.ExpireAtSec))
	}

	dn.DeltaUpdateEcShards(newShards, deletedShards)

	for _, v := range newShards {
		t.RegisterEcShards(v, dn)
	}
	for _, v := range deletedShards {
		t.UnRegisterEcShards(v, dn)
	}
	return
}

func NewEcShardLocations(collection string) *EcShardLocations {
	return &EcShardLocations{
		Collection: collection,
	}
}

func (loc *EcShardLocations) AddShard(shardId erasure_coding.ShardId, dn *DataNode) (added bool) {
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

func (t *Topology) RegisterEcShards(ecShardInfos *erasure_coding.EcVolumeInfo, dn *DataNode) {

	t.ecShardMapLock.Lock()
	defer t.ecShardMapLock.Unlock()

	locations, found := t.ecShardMap[ecShardInfos.VolumeId]
	if !found {
		locations = NewEcShardLocations(ecShardInfos.Collection)
		t.ecShardMap[ecShardInfos.VolumeId] = locations
	}
	for _, shardId := range ecShardInfos.ShardIds() {
		locations.AddShard(shardId, dn)
	}
}

func (t *Topology) UnRegisterEcShards(ecShardInfos *erasure_coding.EcVolumeInfo, dn *DataNode) {
	glog.Infof("removing ec shard info:%+v", ecShardInfos)
	t.ecShardMapLock.Lock()
	defer t.ecShardMapLock.Unlock()

	locations, found := t.ecShardMap[ecShardInfos.VolumeId]
	if !found {
		return
	}
	for _, shardId := range ecShardInfos.ShardIds() {
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

	return
}
