package shell

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func TestPickRackForShardType_AntiAffinityRacks(t *testing.T) {
	// Setup topology with 3 racks, each with 1 node, enough free slots
	topo := &master_pb.TopologyInfo{
		Id: "test_topo",
		DataCenterInfos: []*master_pb.DataCenterInfo{
			{
				Id: "dc1",
				RackInfos: []*master_pb.RackInfo{
					buildRackWithEcShards("rack0", "node0:8080", 100, nil),
					buildRackWithEcShards("rack1", "node1:8080", 100, nil),
					buildRackWithEcShards("rack2", "node2:8080", 100, nil),
				},
			},
		},
	}

	ecNodes, _ := collectEcVolumeServersByDc(topo, "", types.HardDriveType)
	ecb := &ecBalancer{
		ecNodes:  ecNodes,
		diskType: types.HardDriveType,
	}

	racks := ecb.racks()
	rackToShardCount := make(map[string]int) // empty counts
	shardsPerRack := make(map[string][]erasure_coding.ShardId)
	maxPerRack := 2

	// Case 1: Avoid rack0
	antiAffinityRacks := map[string]bool{"rack0": true}

	// Try multiple times to ensure randomness doesn't accidentally pass
	for i := 0; i < 20; i++ {
		picked, err := ecb.pickRackForShardType(racks, shardsPerRack, maxPerRack, rackToShardCount, antiAffinityRacks)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if picked == "rack0" {
			t.Errorf("picked avoided rack rack0")
		}
	}

	// Case 2: Avoid all racks (Fallback check)
	avoidAll := map[string]bool{"rack0": true, "rack1": true, "rack2": true}
	picked, err := ecb.pickRackForShardType(racks, shardsPerRack, maxPerRack, rackToShardCount, avoidAll)
	if err != nil {
		t.Fatalf("fallback failed: %v", err)
	}
	if picked == "" {
		t.Errorf("expected some rack to be picked in fallback")
	}
}

func TestPickNodeForShardType_AntiAffinityNodes(t *testing.T) {
	// Setup topology with 1 rack, 3 nodes
	topo := &master_pb.TopologyInfo{
		Id: "test_topo",
		DataCenterInfos: []*master_pb.DataCenterInfo{
			{
				Id: "dc1",
				RackInfos: []*master_pb.RackInfo{
					{
						Id: "rack0",
						DataNodeInfos: []*master_pb.DataNodeInfo{
							buildDataNode("node0:8080", 100),
							buildDataNode("node1:8080", 100),
							buildDataNode("node2:8080", 100),
						},
					},
				},
			},
		},
	}

	ecNodes, _ := collectEcVolumeServersByDc(topo, "", types.HardDriveType)
	ecb := &ecBalancer{
		ecNodes:  ecNodes,
		diskType: types.HardDriveType,
	}

	nodeToShardCount := make(map[string]int)
	shardsPerNode := make(map[string][]erasure_coding.ShardId)
	maxPerNode := 2

	// Case 1: Avoid node0
	antiAffinityNodes := map[string]bool{"node0:8080": true}

	for i := 0; i < 20; i++ {
		picked, err := ecb.pickNodeForShardType(ecNodes, shardsPerNode, maxPerNode, nodeToShardCount, antiAffinityNodes)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if picked.info.Id == "node0:8080" {
			t.Errorf("picked avoided node node0")
		}
	}
}

func buildDataNode(nodeId string, maxVolumes int64) *master_pb.DataNodeInfo {
	return &master_pb.DataNodeInfo{
		Id: nodeId,
		DiskInfos: map[string]*master_pb.DiskInfo{
			string(types.HardDriveType): {
				Type:           string(types.HardDriveType),
				MaxVolumeCount: maxVolumes,
				VolumeCount:    0,
			},
		},
	}
}
