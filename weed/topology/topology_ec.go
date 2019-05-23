package topology

import (
	"sort"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/storage/erasure_coding"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
)


func (t *Topology) SyncDataNodeEcShards(shardInfos []*master_pb.VolumeEcShardInformationMessage, dn *DataNode) (newShards, deletedShards []*erasure_coding.EcVolumeInfo) {
	// convert into in memory struct storage.VolumeInfo
	var shards []*erasure_coding.EcVolumeInfo
	sort.Slice(shardInfos, func(i, j int) bool {
		return shardInfos[i].Id < shardInfos[j].Id
	})
	var prevVolumeId uint32
	var ecVolumeInfo *erasure_coding.EcVolumeInfo
	for _, shardInfo := range shardInfos {
		if shardInfo.Id != prevVolumeId {
			ecVolumeInfo = erasure_coding.NewEcVolumeInfo(shardInfo.Collection, needle.VolumeId(shardInfo.Id))
			shards = append(shards, ecVolumeInfo)
		}
		ecVolumeInfo.AddShardId(erasure_coding.ShardId(shardInfo.EcIndex))
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

func (t *Topology) RegisterEcShards(ecShardInfos *erasure_coding.EcVolumeInfo, dn *DataNode) {
}
func (t *Topology) UnRegisterEcShards(ecShardInfos *erasure_coding.EcVolumeInfo, dn *DataNode) {
	glog.Infof("removing ec shard info:%+v", ecShardInfos)
}
