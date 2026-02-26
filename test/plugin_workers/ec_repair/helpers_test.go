package ec_repair_test

import (
	"sort"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

type nodeSpec struct {
	id       string
	address  string
	diskType string
	diskID   uint32
	ecShards []*master_pb.VolumeEcShardInformationMessage
}

func buildTopology(nodes []nodeSpec) *master_pb.TopologyInfo {
	dataNodes := make([]*master_pb.DataNodeInfo, 0, len(nodes))
	for _, node := range nodes {
		diskInfo := &master_pb.DiskInfo{
			DiskId:         node.diskID,
			MaxVolumeCount: 100,
			VolumeCount:    10,
			EcShardInfos:   node.ecShards,
		}
		dataNodes = append(dataNodes, &master_pb.DataNodeInfo{
			Id:        node.id,
			Address:   node.address,
			DiskInfos: map[string]*master_pb.DiskInfo{node.diskType: diskInfo},
		})
	}
	return &master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{
			{
				Id: "dc1",
				RackInfos: []*master_pb.RackInfo{
					{
						Id:            "rack1",
						DataNodeInfos: dataNodes,
					},
				},
			},
		},
	}
}

func buildEcShardInfo(volumeID uint32, collection, diskType string, diskID uint32, shardSizes map[uint32]int64) *master_pb.VolumeEcShardInformationMessage {
	shardIDs := make([]int, 0, len(shardSizes))
	for shardID := range shardSizes {
		shardIDs = append(shardIDs, int(shardID))
	}
	sort.Ints(shardIDs)

	var bits uint32
	sizes := make([]int64, 0, len(shardIDs))
	for _, shardID := range shardIDs {
		bits |= (1 << shardID)
		sizes = append(sizes, shardSizes[uint32(shardID)])
	}

	return &master_pb.VolumeEcShardInformationMessage{
		Id:          volumeID,
		Collection:  collection,
		EcIndexBits: bits,
		DiskType:    diskType,
		DiskId:      diskID,
		ShardSizes:  sizes,
	}
}
