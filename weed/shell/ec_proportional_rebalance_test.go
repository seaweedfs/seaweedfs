package shell

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding/distribution"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func TestCalculateECDistributionShell(t *testing.T) {
	// Test the shell wrapper function
	rp, _ := super_block.NewReplicaPlacementFromString("110")

	dist := CalculateECDistribution(
		erasure_coding.TotalShardsCount,
		erasure_coding.ParityShardsCount,
		rp,
	)

	if dist.ReplicationConfig.MinDataCenters != 2 {
		t.Errorf("Expected 2 DCs, got %d", dist.ReplicationConfig.MinDataCenters)
	}
	if dist.TargetShardsPerDC != 7 {
		t.Errorf("Expected 7 shards per DC, got %d", dist.TargetShardsPerDC)
	}

	t.Log(dist.Summary())
}

func TestAnalyzeVolumeDistributionShell(t *testing.T) {
	diskType := types.HardDriveType
	diskTypeKey := string(diskType)

	// Build a topology with unbalanced distribution
	node1 := &EcNode{
		info: &master_pb.DataNodeInfo{
			Id: "127.0.0.1:8080",
			DiskInfos: map[string]*master_pb.DiskInfo{
				diskTypeKey: {
					Type:           diskTypeKey,
					MaxVolumeCount: 10,
					EcShardInfos: []*master_pb.VolumeEcShardInformationMessage{
						{
							Id:          1,
							Collection:  "test",
							EcIndexBits: 0x3FFF, // All 14 shards
						},
					},
				},
			},
		},
		dc:         "dc1",
		rack:       "rack1",
		freeEcSlot: 5,
	}

	node2 := &EcNode{
		info: &master_pb.DataNodeInfo{
			Id: "127.0.0.1:8081",
			DiskInfos: map[string]*master_pb.DiskInfo{
				diskTypeKey: {
					Type:           diskTypeKey,
					MaxVolumeCount: 10,
					EcShardInfos:   []*master_pb.VolumeEcShardInformationMessage{},
				},
			},
		},
		dc:         "dc2",
		rack:       "rack2",
		freeEcSlot: 10,
	}

	locations := []*EcNode{node1, node2}
	volumeId := needle.VolumeId(1)

	analysis := AnalyzeVolumeDistribution(volumeId, locations, diskType)

	shardsByDC := analysis.GetShardsByDC()
	if shardsByDC["dc1"] != 14 {
		t.Errorf("Expected 14 shards in dc1, got %d", shardsByDC["dc1"])
	}

	t.Log(analysis.DetailedString())
}

func TestProportionalRebalancerShell(t *testing.T) {
	diskType := types.HardDriveType
	diskTypeKey := string(diskType)

	// Build topology: 2 DCs, 2 racks each, all shards on one node
	nodes := []*EcNode{
		{
			info: &master_pb.DataNodeInfo{
				Id: "dc1-rack1-node1",
				DiskInfos: map[string]*master_pb.DiskInfo{
					diskTypeKey: {
						Type:           diskTypeKey,
						MaxVolumeCount: 10,
						EcShardInfos: []*master_pb.VolumeEcShardInformationMessage{
							{Id: 1, Collection: "test", EcIndexBits: 0x3FFF},
						},
					},
				},
			},
			dc: "dc1", rack: "dc1-rack1", freeEcSlot: 0,
		},
		{
			info: &master_pb.DataNodeInfo{
				Id: "dc1-rack2-node1",
				DiskInfos: map[string]*master_pb.DiskInfo{
					diskTypeKey: {Type: diskTypeKey, MaxVolumeCount: 10},
				},
			},
			dc: "dc1", rack: "dc1-rack2", freeEcSlot: 10,
		},
		{
			info: &master_pb.DataNodeInfo{
				Id: "dc2-rack1-node1",
				DiskInfos: map[string]*master_pb.DiskInfo{
					diskTypeKey: {Type: diskTypeKey, MaxVolumeCount: 10},
				},
			},
			dc: "dc2", rack: "dc2-rack1", freeEcSlot: 10,
		},
		{
			info: &master_pb.DataNodeInfo{
				Id: "dc2-rack2-node1",
				DiskInfos: map[string]*master_pb.DiskInfo{
					diskTypeKey: {Type: diskTypeKey, MaxVolumeCount: 10},
				},
			},
			dc: "dc2", rack: "dc2-rack2", freeEcSlot: 10,
		},
	}

	rp, _ := super_block.NewReplicaPlacementFromString("110")
	rebalancer := NewProportionalECRebalancer(nodes, rp, diskType)

	volumeId := needle.VolumeId(1)
	moves, err := rebalancer.PlanMoves(volumeId, []*EcNode{nodes[0]})

	if err != nil {
		t.Fatalf("PlanMoves failed: %v", err)
	}

	t.Logf("Planned %d moves", len(moves))
	for i, move := range moves {
		t.Logf("  %d. %s", i+1, move.String())
	}

	// Verify moves to dc2
	movedToDC2 := 0
	for _, move := range moves {
		if move.DestNode.dc == "dc2" {
			movedToDC2++
		}
	}

	if movedToDC2 == 0 {
		t.Error("Expected some moves to dc2")
	}
}

func TestCustomECConfigRebalancer(t *testing.T) {
	diskType := types.HardDriveType
	diskTypeKey := string(diskType)

	// Test with custom 8+4 EC configuration
	ecConfig, err := distribution.NewECConfig(8, 4)
	if err != nil {
		t.Fatalf("Failed to create EC config: %v", err)
	}

	// Build topology for 12 shards (8+4)
	nodes := []*EcNode{
		{
			info: &master_pb.DataNodeInfo{
				Id: "dc1-node1",
				DiskInfos: map[string]*master_pb.DiskInfo{
					diskTypeKey: {
						Type:           diskTypeKey,
						MaxVolumeCount: 10,
						EcShardInfos: []*master_pb.VolumeEcShardInformationMessage{
							{Id: 1, Collection: "test", EcIndexBits: 0x0FFF}, // 12 shards (bits 0-11)
						},
					},
				},
			},
			dc: "dc1", rack: "dc1-rack1", freeEcSlot: 0,
		},
		{
			info: &master_pb.DataNodeInfo{
				Id: "dc2-node1",
				DiskInfos: map[string]*master_pb.DiskInfo{
					diskTypeKey: {Type: diskTypeKey, MaxVolumeCount: 10},
				},
			},
			dc: "dc2", rack: "dc2-rack1", freeEcSlot: 10,
		},
		{
			info: &master_pb.DataNodeInfo{
				Id: "dc3-node1",
				DiskInfos: map[string]*master_pb.DiskInfo{
					diskTypeKey: {Type: diskTypeKey, MaxVolumeCount: 10},
				},
			},
			dc: "dc3", rack: "dc3-rack1", freeEcSlot: 10,
		},
	}

	rp, _ := super_block.NewReplicaPlacementFromString("200") // 3 DCs
	rebalancer := NewProportionalECRebalancerWithConfig(nodes, rp, diskType, ecConfig)

	volumeId := needle.VolumeId(1)
	moves, err := rebalancer.PlanMoves(volumeId, []*EcNode{nodes[0]})

	if err != nil {
		t.Fatalf("PlanMoves failed: %v", err)
	}

	t.Logf("Custom 8+4 EC with 200 replication: planned %d moves", len(moves))

	// Get the distribution summary
	summary := GetDistributionSummaryWithConfig(rp, ecConfig)
	t.Log(summary)

	analysis := GetFaultToleranceAnalysisWithConfig(rp, ecConfig)
	t.Log(analysis)
}

func TestGetDistributionSummaryShell(t *testing.T) {
	rp, _ := super_block.NewReplicaPlacementFromString("110")

	summary := GetDistributionSummary(rp)
	t.Log(summary)

	if len(summary) == 0 {
		t.Error("Summary should not be empty")
	}

	analysis := GetFaultToleranceAnalysis(rp)
	t.Log(analysis)

	if len(analysis) == 0 {
		t.Error("Analysis should not be empty")
	}
}
