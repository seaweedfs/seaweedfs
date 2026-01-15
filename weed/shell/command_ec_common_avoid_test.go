package shell

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
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
	rackToShardCount := make(map[string]int)
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

	// Case 2: Fallback - avoid all racks
	avoidAll := map[string]bool{"rack0": true, "rack1": true, "rack2": true}
	picked, err := ecb.pickRackForShardType(racks, shardsPerRack, maxPerRack, rackToShardCount, avoidAll)
	if err != nil {
		t.Fatalf("fallback failed: %v", err)
	}
	if picked == "" {
		t.Errorf("expected some rack to be picked in fallback")
	}
}

func TestPickRackForShardType_EdgeCases(t *testing.T) {
	t.Run("NoFreeSlots", func(t *testing.T) {
		topo := &master_pb.TopologyInfo{
			Id: "test_topo",
			DataCenterInfos: []*master_pb.DataCenterInfo{
				{
					Id: "dc1",
					RackInfos: []*master_pb.RackInfo{
						buildRackWithEcShards("rack0", "node0:8080", 0, nil), // maxVolumes=0
						buildRackWithEcShards("rack1", "node1:8080", 0, nil),
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
		_, err := ecb.pickRackForShardType(racks, make(map[string][]erasure_coding.ShardId), 2, make(map[string]int), nil)
		if err == nil {
			t.Error("expected error when no free slots, got nil")
		}
	})

	t.Run("AllRacksAtMaxCapacity", func(t *testing.T) {
		topo := &master_pb.TopologyInfo{
			Id: "test_topo",
			DataCenterInfos: []*master_pb.DataCenterInfo{
				{
					Id: "dc1",
					RackInfos: []*master_pb.RackInfo{
						buildRackWithEcShards("rack0", "node0:8080", 100, nil),
						buildRackWithEcShards("rack1", "node1:8080", 100, nil),
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
		shardsPerRack := map[string][]erasure_coding.ShardId{
			"rack0": {0, 1}, // 2 shards
			"rack1": {2, 3}, // 2 shards
		}
		maxPerRack := 2

		_, err := ecb.pickRackForShardType(racks, shardsPerRack, maxPerRack, make(map[string]int), nil)
		if err == nil {
			t.Error("expected error when all racks at max capacity, got nil")
		}
	})

	t.Run("ReplicaPlacementLimit", func(t *testing.T) {
		topo := &master_pb.TopologyInfo{
			Id: "test_topo",
			DataCenterInfos: []*master_pb.DataCenterInfo{
				{
					Id: "dc1",
					RackInfos: []*master_pb.RackInfo{
						buildRackWithEcShards("rack0", "node0:8080", 100, nil),
						buildRackWithEcShards("rack1", "node1:8080", 100, nil),
					},
				},
			},
		}

		ecNodes, _ := collectEcVolumeServersByDc(topo, "", types.HardDriveType)
		rp, _ := super_block.NewReplicaPlacementFromString("012") // DiffRackCount = 1
		ecb := &ecBalancer{
			ecNodes:          ecNodes,
			diskType:         types.HardDriveType,
			replicaPlacement: rp,
		}

		racks := ecb.racks()
		rackToShardCount := map[string]int{
			"rack0": 1, // At limit
			"rack1": 0,
		}

		picked, err := ecb.pickRackForShardType(racks, make(map[string][]erasure_coding.ShardId), 5, rackToShardCount, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if picked != "rack1" {
			t.Errorf("expected rack1 (not at limit), got %v", picked)
		}
	})

	t.Run("PreferFewerShards", func(t *testing.T) {
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
		shardsPerRack := map[string][]erasure_coding.ShardId{
			"rack0": {0, 1}, // 2 shards
			"rack1": {2},    // 1 shard
			"rack2": {},     // 0 shards
		}

		// Should pick rack2 (fewest shards)
		picked, err := ecb.pickRackForShardType(racks, shardsPerRack, 5, make(map[string]int), nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if picked != "rack2" {
			t.Errorf("expected rack2 (fewest shards), got %v", picked)
		}
	})
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

func TestPickNodeForShardType_EdgeCases(t *testing.T) {
	t.Run("NoFreeSlots", func(t *testing.T) {
		topo := &master_pb.TopologyInfo{
			Id: "test_topo",
			DataCenterInfos: []*master_pb.DataCenterInfo{
				{
					Id: "dc1",
					RackInfos: []*master_pb.RackInfo{
						{
							Id: "rack0",
							DataNodeInfos: []*master_pb.DataNodeInfo{
								buildDataNode("node0:8080", 0), // No capacity
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

		_, err := ecb.pickNodeForShardType(ecNodes, make(map[string][]erasure_coding.ShardId), 2, make(map[string]int), nil)
		if err == nil {
			t.Error("expected error when no free slots, got nil")
		}
	})

	t.Run("ReplicaPlacementSameRackLimit", func(t *testing.T) {
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
							},
						},
					},
				},
			},
		}

		ecNodes, _ := collectEcVolumeServersByDc(topo, "", types.HardDriveType)
		rp, _ := super_block.NewReplicaPlacementFromString("021") // SameRackCount = 2
		ecb := &ecBalancer{
			ecNodes:          ecNodes,
			diskType:         types.HardDriveType,
			replicaPlacement: rp,
		}

		nodeToShardCount := map[string]int{
			"node0:8080": 3, // Exceeds SameRackCount + 1
			"node1:8080": 0,
		}

		picked, err := ecb.pickNodeForShardType(ecNodes, make(map[string][]erasure_coding.ShardId), 5, nodeToShardCount, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if picked.info.Id != "node1:8080" {
			t.Errorf("expected node1 (not at limit), got %v", picked.info.Id)
		}
	})
}

func TestShardsByType(t *testing.T) {
	vid := needle.VolumeId(123)

	// Create mock nodes with shards
	nodes := []*EcNode{
		{
			info: &master_pb.DataNodeInfo{
				Id: "node1",
				DiskInfos: map[string]*master_pb.DiskInfo{
					string(types.HardDriveType): {
						EcShardInfos: []*master_pb.VolumeEcShardInformationMessage{
							{
								Id:          uint32(vid),
								EcIndexBits: uint32((1 << 0) | (1 << 1) | (1 << 10) | (1 << 11)), // data: 0,1 parity: 10,11
							},
						},
					},
				},
			},
			rack: "rack1",
		},
	}

	t.Run("Standard10Plus4", func(t *testing.T) {
		dataPerRack, parityPerRack := shardsByTypePerRack(vid, nodes, types.HardDriveType, 10)

		if len(dataPerRack["rack1"]) != 2 {
			t.Errorf("expected 2 data shards, got %d", len(dataPerRack["rack1"]))
		}
		if len(parityPerRack["rack1"]) != 2 {
			t.Errorf("expected 2 parity shards, got %d", len(parityPerRack["rack1"]))
		}
	})

	t.Run("NodeGrouping", func(t *testing.T) {
		dataPerNode, parityPerNode := shardsByTypePerNode(vid, nodes, types.HardDriveType, 10)

		if len(dataPerNode["node1"]) != 2 {
			t.Errorf("expected 2 data shards on node1, got %d", len(dataPerNode["node1"]))
		}
		if len(parityPerNode["node1"]) != 2 {
			t.Errorf("expected 2 parity shards on node1, got %d", len(parityPerNode["node1"]))
		}
	})
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
