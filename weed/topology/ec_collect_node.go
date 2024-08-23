package topology

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

type RackId string
type EcCollectNode struct {
}

func MasterCollectEcVolumeServersByDc(topo *master_pb.TopologyInfo, selectedDataCenter string, toDiskType types.DiskType) (ecNodes []*master_pb.EcNodeInfo) {
	MasterEachDataNode(topo, func(dc string, rack RackId, dn *master_pb.DataNodeInfo) {
		if selectedDataCenter != "" && selectedDataCenter != dc {
			return
		}
		freeEcSlots := MasterCountFreeShardSlots(dn, toDiskType)
		node := &master_pb.EcNodeInfo{
			Id:            dn.Id,
			GrpcPort:      dn.GrpcPort,
			FreeShardSlot: int64(freeEcSlots),
		}
		ecNodes = append(ecNodes, node)
	})
	return
}

func MasterEachDataNode(topo *master_pb.TopologyInfo, fn func(dc string, rack RackId, dn *master_pb.DataNodeInfo)) {
	for _, dc := range topo.DataCenterInfos {
		for _, rack := range dc.RackInfos {
			for _, dn := range rack.DataNodeInfos {
				fn(dc.Id, RackId(rack.Id), dn)
			}
		}
	}
}
func MasterCountFreeShardSlots(dn *master_pb.DataNodeInfo, diskType types.DiskType) (count int) {
	if dn.DiskInfos == nil {
		return 0
	}
	diskInfo := dn.DiskInfos[string(diskType)]
	if diskInfo == nil {
		return 0
	}
	return int(diskInfo.MaxVolumeCount-diskInfo.VolumeCount)*erasure_coding.DataShardsCount - MasterCountShards(diskInfo.EcShardInfos)
}
func MasterCountShards(ecShardInfos []*master_pb.VolumeEcShardInformationMessage) (count int) {
	for _, ecShardInfo := range ecShardInfos {
		shardBits := erasure_coding.ShardBits(ecShardInfo.EcIndexBits)
		count += shardBits.ShardIdCount()
	}
	return
}
