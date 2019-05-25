package topology

import (
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/storage/erasure_coding"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
)

type EcShardLocations struct {
	Collection string
	locations  [erasure_coding.TotalShardsCount][]*DataNode
}

func (t *Topology) SyncDataNodeEcShards(shardInfos []*master_pb.VolumeEcShardInformationMessage, dn *DataNode) (newShards, deletedShards []*erasure_coding.EcVolumeInfo) {
	// convert into in memory struct storage.VolumeInfo
	var shards []*erasure_coding.EcVolumeInfo
	for _, shardInfo := range shardInfos {
		shards = append(shards,
			erasure_coding.NewEcVolumeInfo(
				shardInfo.Collection,
				needle.VolumeId(shardInfo.Id),
				erasure_coding.ShardBits(shardInfo.EcIndexBits)))
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

func NewEcShardLocations(collection string) *EcShardLocations {
	return &EcShardLocations{
		Collection: collection,
	}
}

func (loc *EcShardLocations) AddShard(shardId erasure_coding.ShardId, dn *DataNode) (added bool) {
	dataNodes := loc.locations[shardId]
	for _, n := range dataNodes {
		if n.Id() == dn.Id() {
			return false
		}
	}
	loc.locations[shardId] = append(dataNodes, dn)
	return true
}

func (loc *EcShardLocations) DeleteShard(shardId erasure_coding.ShardId, dn *DataNode) (deleted bool) {
	dataNodes := loc.locations[shardId]
	foundIndex := -1
	for index, n := range dataNodes {
		if n.Id() == dn.Id() {
			foundIndex = index
		}
	}
	if foundIndex < 0 {
		return false
	}
	loc.locations[shardId] = append(dataNodes[:foundIndex], dataNodes[foundIndex+1:]...)
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
