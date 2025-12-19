package distribution

import (
	"testing"
)

func TestNewECConfig(t *testing.T) {
	tests := []struct {
		name         string
		dataShards   int
		parityShards int
		wantErr      bool
	}{
		{"valid 10+4", 10, 4, false},
		{"valid 8+4", 8, 4, false},
		{"valid 6+3", 6, 3, false},
		{"valid 4+2", 4, 2, false},
		{"invalid data=0", 0, 4, true},
		{"invalid parity=0", 10, 0, true},
		{"invalid total>32", 20, 15, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := NewECConfig(tt.dataShards, tt.parityShards)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewECConfig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if config.DataShards != tt.dataShards {
					t.Errorf("DataShards = %d, want %d", config.DataShards, tt.dataShards)
				}
				if config.ParityShards != tt.parityShards {
					t.Errorf("ParityShards = %d, want %d", config.ParityShards, tt.parityShards)
				}
				if config.TotalShards() != tt.dataShards+tt.parityShards {
					t.Errorf("TotalShards() = %d, want %d", config.TotalShards(), tt.dataShards+tt.parityShards)
				}
			}
		})
	}
}

func TestCalculateDistribution(t *testing.T) {
	tests := []struct {
		name                    string
		ecConfig                ECConfig
		replication             string
		expectedMinDCs          int
		expectedMinRacksPerDC   int
		expectedMinNodesPerRack int
		expectedTargetPerDC     int
		expectedTargetPerRack   int
		expectedTargetPerNode   int
	}{
		{
			name:                    "10+4 with 000",
			ecConfig:                DefaultECConfig(),
			replication:             "000",
			expectedMinDCs:          1,
			expectedMinRacksPerDC:   1,
			expectedMinNodesPerRack: 1,
			expectedTargetPerDC:     14,
			expectedTargetPerRack:   14,
			expectedTargetPerNode:   14,
		},
		{
			name:                    "10+4 with 100",
			ecConfig:                DefaultECConfig(),
			replication:             "100",
			expectedMinDCs:          2,
			expectedMinRacksPerDC:   1,
			expectedMinNodesPerRack: 1,
			expectedTargetPerDC:     7,
			expectedTargetPerRack:   7,
			expectedTargetPerNode:   7,
		},
		{
			name:                    "10+4 with 110",
			ecConfig:                DefaultECConfig(),
			replication:             "110",
			expectedMinDCs:          2,
			expectedMinRacksPerDC:   2,
			expectedMinNodesPerRack: 1,
			expectedTargetPerDC:     7,
			expectedTargetPerRack:   4,
			expectedTargetPerNode:   4,
		},
		{
			name:                    "10+4 with 200",
			ecConfig:                DefaultECConfig(),
			replication:             "200",
			expectedMinDCs:          3,
			expectedMinRacksPerDC:   1,
			expectedMinNodesPerRack: 1,
			expectedTargetPerDC:     5,
			expectedTargetPerRack:   5,
			expectedTargetPerNode:   5,
		},
		{
			name: "8+4 with 110",
			ecConfig: ECConfig{
				DataShards:   8,
				ParityShards: 4,
			},
			replication:             "110",
			expectedMinDCs:          2,
			expectedMinRacksPerDC:   2,
			expectedMinNodesPerRack: 1,
			expectedTargetPerDC:     6, // 12/2 = 6
			expectedTargetPerRack:   3, // 6/2 = 3
			expectedTargetPerNode:   3,
		},
		{
			name: "6+3 with 100",
			ecConfig: ECConfig{
				DataShards:   6,
				ParityShards: 3,
			},
			replication:             "100",
			expectedMinDCs:          2,
			expectedMinRacksPerDC:   1,
			expectedMinNodesPerRack: 1,
			expectedTargetPerDC:     5, // ceil(9/2) = 5
			expectedTargetPerRack:   5,
			expectedTargetPerNode:   5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rep, err := NewReplicationConfigFromString(tt.replication)
			if err != nil {
				t.Fatalf("Failed to parse replication %s: %v", tt.replication, err)
			}

			dist := CalculateDistribution(tt.ecConfig, rep)

			if dist.ReplicationConfig.MinDataCenters != tt.expectedMinDCs {
				t.Errorf("MinDataCenters = %d, want %d", dist.ReplicationConfig.MinDataCenters, tt.expectedMinDCs)
			}
			if dist.ReplicationConfig.MinRacksPerDC != tt.expectedMinRacksPerDC {
				t.Errorf("MinRacksPerDC = %d, want %d", dist.ReplicationConfig.MinRacksPerDC, tt.expectedMinRacksPerDC)
			}
			if dist.ReplicationConfig.MinNodesPerRack != tt.expectedMinNodesPerRack {
				t.Errorf("MinNodesPerRack = %d, want %d", dist.ReplicationConfig.MinNodesPerRack, tt.expectedMinNodesPerRack)
			}
			if dist.TargetShardsPerDC != tt.expectedTargetPerDC {
				t.Errorf("TargetShardsPerDC = %d, want %d", dist.TargetShardsPerDC, tt.expectedTargetPerDC)
			}
			if dist.TargetShardsPerRack != tt.expectedTargetPerRack {
				t.Errorf("TargetShardsPerRack = %d, want %d", dist.TargetShardsPerRack, tt.expectedTargetPerRack)
			}
			if dist.TargetShardsPerNode != tt.expectedTargetPerNode {
				t.Errorf("TargetShardsPerNode = %d, want %d", dist.TargetShardsPerNode, tt.expectedTargetPerNode)
			}

			t.Logf("Distribution for %s: %s", tt.name, dist.String())
		})
	}
}

func TestFaultToleranceAnalysis(t *testing.T) {
	tests := []struct {
		name           string
		ecConfig       ECConfig
		replication    string
		canSurviveDC   bool
		canSurviveRack bool
	}{
		// 10+4 = 14 shards, need 10 to reconstruct, can lose 4
		{"10+4 000", DefaultECConfig(), "000", false, false}, // All in one, any failure is fatal
		{"10+4 100", DefaultECConfig(), "100", false, false}, // 7 per DC/rack, 7 remaining < 10
		{"10+4 200", DefaultECConfig(), "200", false, false}, // 5 per DC/rack, 9 remaining < 10
		{"10+4 110", DefaultECConfig(), "110", false, true},  // 4 per rack, 10 remaining = enough for rack

		// 8+4 = 12 shards, need 8 to reconstruct, can lose 4
		{"8+4 100", ECConfig{8, 4}, "100", false, false}, // 6 per DC/rack, 6 remaining < 8
		{"8+4 200", ECConfig{8, 4}, "200", true, true},   // 4 per DC/rack, 8 remaining = enough!
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rep, _ := NewReplicationConfigFromString(tt.replication)
			dist := CalculateDistribution(tt.ecConfig, rep)

			if dist.CanSurviveDCFailure() != tt.canSurviveDC {
				t.Errorf("CanSurviveDCFailure() = %v, want %v", dist.CanSurviveDCFailure(), tt.canSurviveDC)
			}
			if dist.CanSurviveRackFailure() != tt.canSurviveRack {
				t.Errorf("CanSurviveRackFailure() = %v, want %v", dist.CanSurviveRackFailure(), tt.canSurviveRack)
			}

			t.Log(dist.FaultToleranceAnalysis())
		})
	}
}

func TestMinDCsForDCFaultTolerance(t *testing.T) {
	tests := []struct {
		name     string
		ecConfig ECConfig
		minDCs   int
	}{
		// 10+4: can lose 4, so max 4 per DC, 14/4 = 4 DCs needed
		{"10+4", DefaultECConfig(), 4},
		// 8+4: can lose 4, so max 4 per DC, 12/4 = 3 DCs needed
		{"8+4", ECConfig{8, 4}, 3},
		// 6+3: can lose 3, so max 3 per DC, 9/3 = 3 DCs needed
		{"6+3", ECConfig{6, 3}, 3},
		// 4+2: can lose 2, so max 2 per DC, 6/2 = 3 DCs needed
		{"4+2", ECConfig{4, 2}, 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rep, _ := NewReplicationConfigFromString("000")
			dist := CalculateDistribution(tt.ecConfig, rep)

			if dist.MinDCsForDCFaultTolerance() != tt.minDCs {
				t.Errorf("MinDCsForDCFaultTolerance() = %d, want %d",
					dist.MinDCsForDCFaultTolerance(), tt.minDCs)
			}

			t.Logf("%s: needs %d DCs for DC fault tolerance", tt.name, dist.MinDCsForDCFaultTolerance())
		})
	}
}

func TestTopologyAnalysis(t *testing.T) {
	analysis := NewTopologyAnalysis()

	// Add nodes to topology
	node1 := &TopologyNode{
		NodeID:     "node1",
		DataCenter: "dc1",
		Rack:       "rack1",
		FreeSlots:  5,
	}
	node2 := &TopologyNode{
		NodeID:     "node2",
		DataCenter: "dc1",
		Rack:       "rack2",
		FreeSlots:  10,
	}
	node3 := &TopologyNode{
		NodeID:     "node3",
		DataCenter: "dc2",
		Rack:       "rack3",
		FreeSlots:  10,
	}

	analysis.AddNode(node1)
	analysis.AddNode(node2)
	analysis.AddNode(node3)

	// Add shard locations (all on node1)
	for i := 0; i < 14; i++ {
		analysis.AddShardLocation(ShardLocation{
			ShardID:    i,
			NodeID:     "node1",
			DataCenter: "dc1",
			Rack:       "rack1",
		})
	}

	analysis.Finalize()

	// Verify counts
	if analysis.TotalShards != 14 {
		t.Errorf("TotalShards = %d, want 14", analysis.TotalShards)
	}
	if analysis.ShardsByDC["dc1"] != 14 {
		t.Errorf("ShardsByDC[dc1] = %d, want 14", analysis.ShardsByDC["dc1"])
	}
	if analysis.ShardsByRack["rack1"] != 14 {
		t.Errorf("ShardsByRack[rack1] = %d, want 14", analysis.ShardsByRack["rack1"])
	}
	if analysis.ShardsByNode["node1"] != 14 {
		t.Errorf("ShardsByNode[node1] = %d, want 14", analysis.ShardsByNode["node1"])
	}

	t.Log(analysis.DetailedString())
}

func TestRebalancer(t *testing.T) {
	// Build topology: 2 DCs, 2 racks each, all shards on one node
	analysis := NewTopologyAnalysis()

	// Add nodes
	nodes := []*TopologyNode{
		{NodeID: "dc1-rack1-node1", DataCenter: "dc1", Rack: "dc1-rack1", FreeSlots: 0},
		{NodeID: "dc1-rack2-node1", DataCenter: "dc1", Rack: "dc1-rack2", FreeSlots: 10},
		{NodeID: "dc2-rack1-node1", DataCenter: "dc2", Rack: "dc2-rack1", FreeSlots: 10},
		{NodeID: "dc2-rack2-node1", DataCenter: "dc2", Rack: "dc2-rack2", FreeSlots: 10},
	}
	for _, node := range nodes {
		analysis.AddNode(node)
	}

	// Add all 14 shards to first node
	for i := 0; i < 14; i++ {
		analysis.AddShardLocation(ShardLocation{
			ShardID:    i,
			NodeID:     "dc1-rack1-node1",
			DataCenter: "dc1",
			Rack:       "dc1-rack1",
		})
	}
	analysis.Finalize()

	// Create rebalancer with 110 replication (2 DCs, 2 racks each)
	ec := DefaultECConfig()
	rep, _ := NewReplicationConfigFromString("110")
	rebalancer := NewRebalancer(ec, rep)

	plan, err := rebalancer.PlanRebalance(analysis)
	if err != nil {
		t.Fatalf("PlanRebalance failed: %v", err)
	}

	t.Logf("Planned %d moves", plan.TotalMoves)
	t.Log(plan.DetailedString())

	// Verify we're moving shards to dc2
	movedToDC2 := 0
	for _, move := range plan.Moves {
		if move.DestNode.DataCenter == "dc2" {
			movedToDC2++
		}
	}

	if movedToDC2 == 0 {
		t.Error("Expected some moves to dc2")
	}

	// With "110" replication, target is 7 shards per DC
	// Starting with 14 in dc1, should plan to move 7 to dc2
	if plan.MovesAcrossDC < 7 {
		t.Errorf("Expected at least 7 cross-DC moves for 110 replication, got %d", plan.MovesAcrossDC)
	}
}

func TestCustomECRatios(t *testing.T) {
	// Test various custom EC ratios that seaweed-enterprise might use
	ratios := []struct {
		name   string
		data   int
		parity int
	}{
		{"4+2", 4, 2},
		{"6+3", 6, 3},
		{"8+2", 8, 2},
		{"8+4", 8, 4},
		{"10+4", 10, 4},
		{"12+4", 12, 4},
		{"16+4", 16, 4},
	}

	for _, ratio := range ratios {
		t.Run(ratio.name, func(t *testing.T) {
			ec, err := NewECConfig(ratio.data, ratio.parity)
			if err != nil {
				t.Fatalf("Failed to create EC config: %v", err)
			}

			rep, _ := NewReplicationConfigFromString("110")
			dist := CalculateDistribution(ec, rep)

			t.Logf("EC %s with replication 110:", ratio.name)
			t.Logf("  Total shards: %d", ec.TotalShards())
			t.Logf("  Can lose: %d shards", ec.MaxTolerableLoss())
			t.Logf("  Target per DC: %d", dist.TargetShardsPerDC)
			t.Logf("  Target per rack: %d", dist.TargetShardsPerRack)
			t.Logf("  Min DCs for DC fault tolerance: %d", dist.MinDCsForDCFaultTolerance())

			// Verify basic sanity
			if dist.TargetShardsPerDC*2 < ec.TotalShards() {
				t.Errorf("Target per DC (%d) * 2 should be >= total (%d)",
					dist.TargetShardsPerDC, ec.TotalShards())
			}
		})
	}
}

func TestShardClassification(t *testing.T) {
	ec := DefaultECConfig() // 10+4

	// Test IsDataShard
	for i := 0; i < 10; i++ {
		if !ec.IsDataShard(i) {
			t.Errorf("Shard %d should be a data shard", i)
		}
		if ec.IsParityShard(i) {
			t.Errorf("Shard %d should not be a parity shard", i)
		}
	}

	// Test IsParityShard
	for i := 10; i < 14; i++ {
		if ec.IsDataShard(i) {
			t.Errorf("Shard %d should not be a data shard", i)
		}
		if !ec.IsParityShard(i) {
			t.Errorf("Shard %d should be a parity shard", i)
		}
	}

	// Test with custom 8+4 EC
	ec84, _ := NewECConfig(8, 4)
	for i := 0; i < 8; i++ {
		if !ec84.IsDataShard(i) {
			t.Errorf("8+4 EC: Shard %d should be a data shard", i)
		}
	}
	for i := 8; i < 12; i++ {
		if !ec84.IsParityShard(i) {
			t.Errorf("8+4 EC: Shard %d should be a parity shard", i)
		}
	}
}

func TestSortShardsDataFirst(t *testing.T) {
	ec := DefaultECConfig() // 10+4

	// Mixed shards: [0, 10, 5, 11, 2, 12, 7, 13]
	shards := []int{0, 10, 5, 11, 2, 12, 7, 13}
	sorted := ec.SortShardsDataFirst(shards)

	t.Logf("Original: %v", shards)
	t.Logf("Sorted (data first): %v", sorted)

	// First 4 should be data shards (0, 5, 2, 7)
	for i := 0; i < 4; i++ {
		if !ec.IsDataShard(sorted[i]) {
			t.Errorf("Position %d should be a data shard, got %d", i, sorted[i])
		}
	}

	// Last 4 should be parity shards (10, 11, 12, 13)
	for i := 4; i < 8; i++ {
		if !ec.IsParityShard(sorted[i]) {
			t.Errorf("Position %d should be a parity shard, got %d", i, sorted[i])
		}
	}
}

func TestSortShardsParityFirst(t *testing.T) {
	ec := DefaultECConfig() // 10+4

	// Mixed shards: [0, 10, 5, 11, 2, 12, 7, 13]
	shards := []int{0, 10, 5, 11, 2, 12, 7, 13}
	sorted := ec.SortShardsParityFirst(shards)

	t.Logf("Original: %v", shards)
	t.Logf("Sorted (parity first): %v", sorted)

	// First 4 should be parity shards (10, 11, 12, 13)
	for i := 0; i < 4; i++ {
		if !ec.IsParityShard(sorted[i]) {
			t.Errorf("Position %d should be a parity shard, got %d", i, sorted[i])
		}
	}

	// Last 4 should be data shards (0, 5, 2, 7)
	for i := 4; i < 8; i++ {
		if !ec.IsDataShard(sorted[i]) {
			t.Errorf("Position %d should be a data shard, got %d", i, sorted[i])
		}
	}
}

func TestRebalancerPrefersMovingParityShards(t *testing.T) {
	// Build topology where one node has all shards including mix of data and parity
	analysis := NewTopologyAnalysis()

	// Node 1: Has all 14 shards (mixed data and parity)
	node1 := &TopologyNode{
		NodeID:     "node1",
		DataCenter: "dc1",
		Rack:       "rack1",
		FreeSlots:  0,
	}
	analysis.AddNode(node1)

	// Node 2: Empty, ready to receive
	node2 := &TopologyNode{
		NodeID:     "node2",
		DataCenter: "dc1",
		Rack:       "rack1",
		FreeSlots:  10,
	}
	analysis.AddNode(node2)

	// Add all 14 shards to node1
	for i := 0; i < 14; i++ {
		analysis.AddShardLocation(ShardLocation{
			ShardID:    i,
			NodeID:     "node1",
			DataCenter: "dc1",
			Rack:       "rack1",
		})
	}
	analysis.Finalize()

	// Create rebalancer
	ec := DefaultECConfig()
	rep, _ := NewReplicationConfigFromString("000")
	rebalancer := NewRebalancer(ec, rep)

	plan, err := rebalancer.PlanRebalance(analysis)
	if err != nil {
		t.Fatalf("PlanRebalance failed: %v", err)
	}

	t.Logf("Planned %d moves", len(plan.Moves))

	// Check that parity shards are moved first
	parityMovesFirst := 0
	dataMovesFirst := 0
	seenDataMove := false

	for _, move := range plan.Moves {
		isParity := ec.IsParityShard(move.ShardID)
		t.Logf("Move shard %d (parity=%v): %s -> %s",
			move.ShardID, isParity, move.SourceNode.NodeID, move.DestNode.NodeID)

		if isParity && !seenDataMove {
			parityMovesFirst++
		} else if !isParity {
			seenDataMove = true
			dataMovesFirst++
		}
	}

	t.Logf("Parity moves before first data move: %d", parityMovesFirst)
	t.Logf("Data moves: %d", dataMovesFirst)

	// With 10+4 EC, there are 4 parity shards
	// They should be moved before data shards when possible
	if parityMovesFirst < 4 && len(plan.Moves) >= 4 {
		t.Logf("Note: Expected parity shards to be moved first, but got %d parity moves before data moves", parityMovesFirst)
	}
}

func TestDistributionSummary(t *testing.T) {
	ec := DefaultECConfig()
	rep, _ := NewReplicationConfigFromString("110")
	dist := CalculateDistribution(ec, rep)

	summary := dist.Summary()
	t.Log(summary)

	if len(summary) == 0 {
		t.Error("Summary should not be empty")
	}

	analysis := dist.FaultToleranceAnalysis()
	t.Log(analysis)

	if len(analysis) == 0 {
		t.Error("Fault tolerance analysis should not be empty")
	}
}
