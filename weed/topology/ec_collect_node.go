package topology

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
)

type RackId string
type EcCollectNode struct {
}

func MasterCollectEcVolumeServersByDc(topo *master_pb.TopologyInfo, selectedDataCenter string) (ecNodes []*master_pb.EcNodeInfo) {
	MasterEachDataNode(topo, func(dc string, rack RackId, dn *master_pb.DataNodeInfo) {
		if selectedDataCenter != "" && selectedDataCenter != dc {
			return
		}
		diskInfos := MasterCountFreeShardSlots(dn)
		node := &master_pb.EcNodeInfo{
			Id:              dn.Id,
			GrpcPort:        dn.GrpcPort,
			EcNodeDiskInfos: diskInfos,
			DataCenter:      dc,
			RackInfo:        string(rack),
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
func MasterCountFreeShardSlots(dn *master_pb.DataNodeInfo) (diskInfos map[string]*master_pb.EcNodeDiskInfo) {
	ret := make(map[string]*master_pb.EcNodeDiskInfo)
	if dn.DiskInfos == nil {
		return ret
	}
	for diskType, diskUsageCounts := range dn.DiskInfos {
		ecShardsCount := MasterCountShards(diskUsageCounts.EcShardInfos)
		m := &master_pb.EcNodeDiskInfo{
			VolumeCount:       diskUsageCounts.VolumeCount,
			MaxVolumeCount:    diskUsageCounts.MaxVolumeCount,
			FreeVolumeCount:   diskUsageCounts.MaxVolumeCount - diskUsageCounts.VolumeCount,
			ActiveVolumeCount: diskUsageCounts.ActiveVolumeCount,
			RemoteVolumeCount: diskUsageCounts.RemoteVolumeCount,
			CountEcShards:     int64(ecShardsCount),
		}
		ret[diskType] = m
	}
	return ret
}
func MasterCountShards(ecShardInfos []*master_pb.VolumeEcShardInformationMessage) (count int) {
	for _, ecShardInfo := range ecShardInfos {
		shardBits := erasure_coding.ShardBits(ecShardInfo.EcIndexBits)
		count += shardBits.ShardIdCount()
	}
	return
}
