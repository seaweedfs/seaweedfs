package ec_vacuum

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
)

func TestDetermineGenerationsFromParams(t *testing.T) {
	logic := NewEcVacuumLogic()

	tests := []struct {
		name        string
		params      *worker_pb.TaskParams
		expectSrc   uint32
		expectTgt   uint32
		expectError bool
	}{
		{
			name:        "nil params",
			params:      nil,
			expectError: true,
		},
		{
			name: "empty sources - fallback to defaults",
			params: &worker_pb.TaskParams{
				Sources: []*worker_pb.TaskSource{},
			},
			expectSrc: 0,
			expectTgt: 1,
		},
		{
			name: "generation 0 source",
			params: &worker_pb.TaskParams{
				Sources: []*worker_pb.TaskSource{
					{Generation: 0},
				},
			},
			expectSrc: 0,
			expectTgt: 1,
		},
		{
			name: "generation 1 source",
			params: &worker_pb.TaskParams{
				Sources: []*worker_pb.TaskSource{
					{Generation: 1},
				},
			},
			expectSrc: 1,
			expectTgt: 2,
		},
		{
			name: "generation 5 source",
			params: &worker_pb.TaskParams{
				Sources: []*worker_pb.TaskSource{
					{Generation: 5},
				},
			},
			expectSrc: 5,
			expectTgt: 6,
		},
		{
			name: "inconsistent generations",
			params: &worker_pb.TaskParams{
				Sources: []*worker_pb.TaskSource{
					{Generation: 1},
					{Generation: 2}, // Different generation!
				},
			},
			expectError: true,
		},
		{
			name: "multiple sources same generation",
			params: &worker_pb.TaskParams{
				Sources: []*worker_pb.TaskSource{
					{Generation: 3},
					{Generation: 3},
					{Generation: 3},
				},
			},
			expectSrc: 3,
			expectTgt: 4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srcGen, tgtGen, err := logic.DetermineGenerationsFromParams(tt.params)

			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if srcGen != tt.expectSrc {
				t.Errorf("source generation: expected %d, got %d", tt.expectSrc, srcGen)
			}

			if tgtGen != tt.expectTgt {
				t.Errorf("target generation: expected %d, got %d", tt.expectTgt, tgtGen)
			}
		})
	}
}

func TestParseSourceNodes(t *testing.T) {
	logic := NewEcVacuumLogic()

	tests := []struct {
		name         string
		params       *worker_pb.TaskParams
		expectNodes  int
		expectShards map[string][]int // node -> shard IDs
		expectError  bool
	}{
		{
			name:        "nil params",
			params:      nil,
			expectError: true,
		},
		{
			name: "empty sources",
			params: &worker_pb.TaskParams{
				Sources: []*worker_pb.TaskSource{},
			},
			expectError: true,
		},
		{
			name: "single node with shards",
			params: &worker_pb.TaskParams{
				Sources: []*worker_pb.TaskSource{
					{
						Node:     "node1:8080",
						ShardIds: []uint32{0, 1, 2, 3, 4, 5},
					},
				},
			},
			expectNodes: 1,
			expectShards: map[string][]int{
				"node1:8080": {0, 1, 2, 3, 4, 5},
			},
		},
		{
			name: "multiple nodes with different shards",
			params: &worker_pb.TaskParams{
				Sources: []*worker_pb.TaskSource{
					{
						Node:     "node1:8080",
						ShardIds: []uint32{0, 1, 2, 3, 4},
					},
					{
						Node:     "node2:8080",
						ShardIds: []uint32{5, 6, 7, 8, 9},
					},
					{
						Node:     "node3:8080",
						ShardIds: []uint32{10, 11, 12, 13},
					},
				},
			},
			expectNodes: 3,
			expectShards: map[string][]int{
				"node1:8080": {0, 1, 2, 3, 4},
				"node2:8080": {5, 6, 7, 8, 9},
				"node3:8080": {10, 11, 12, 13},
			},
		},
		{
			name: "overlapping shards across nodes",
			params: &worker_pb.TaskParams{
				Sources: []*worker_pb.TaskSource{
					{
						Node:     "node1:8080",
						ShardIds: []uint32{0, 1, 2},
					},
					{
						Node:     "node2:8080",
						ShardIds: []uint32{0, 3, 4}, // Shard 0 is on both nodes
					},
				},
			},
			expectNodes: 2,
			expectShards: map[string][]int{
				"node1:8080": {0, 1, 2},
				"node2:8080": {0, 3, 4},
			},
		},
		{
			name: "empty node name ignored",
			params: &worker_pb.TaskParams{
				Sources: []*worker_pb.TaskSource{
					{
						Node:     "",
						ShardIds: []uint32{0, 1, 2},
					},
					{
						Node:     "node1:8080",
						ShardIds: []uint32{3, 4, 5},
					},
				},
			},
			expectNodes: 1,
			expectShards: map[string][]int{
				"node1:8080": {3, 4, 5},
			},
		},
		{
			name: "invalid shard IDs filtered out",
			params: &worker_pb.TaskParams{
				Sources: []*worker_pb.TaskSource{
					{
						Node:     "node1:8080",
						ShardIds: []uint32{0, 1, 14, 15, 100}, // 14+ are invalid
					},
				},
			},
			expectNodes: 1,
			expectShards: map[string][]int{
				"node1:8080": {0, 1}, // Only valid shards
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sourceNodes, err := logic.ParseSourceNodes(tt.params)

			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if len(sourceNodes) != tt.expectNodes {
				t.Errorf("node count: expected %d, got %d", tt.expectNodes, len(sourceNodes))
				return
			}

			// Verify shard distribution
			for nodeAddr, expectedShardIds := range tt.expectShards {
				shardBits, exists := sourceNodes[pb.ServerAddress(nodeAddr)]
				if !exists {
					t.Errorf("expected node %s not found", nodeAddr)
					continue
				}

				// Convert ShardBits back to slice for comparison
				var actualShardIds []int
				for i := 0; i < erasure_coding.TotalShardsCount; i++ {
					if shardBits.HasShardId(erasure_coding.ShardId(i)) {
						actualShardIds = append(actualShardIds, i)
					}
				}

				if len(actualShardIds) != len(expectedShardIds) {
					t.Errorf("node %s shard count: expected %d, got %d",
						nodeAddr, len(expectedShardIds), len(actualShardIds))
					continue
				}

				// Check each expected shard
				for _, expectedId := range expectedShardIds {
					found := false
					for _, actualId := range actualShardIds {
						if actualId == expectedId {
							found = true
							break
						}
					}
					if !found {
						t.Errorf("node %s missing expected shard %d", nodeAddr, expectedId)
					}
				}
			}
		})
	}
}

func TestValidateShardDistribution(t *testing.T) {
	logic := NewEcVacuumLogic()

	tests := []struct {
		name         string
		distribution ShardDistribution
		expectError  bool
		description  string
	}{
		{
			name: "sufficient shards for reconstruction",
			distribution: ShardDistribution{
				Generation: 1,
				Nodes: map[pb.ServerAddress]erasure_coding.ShardBits{
					"node1:8080": createShardBits([]int{0, 1, 2, 3, 4}),
					"node2:8080": createShardBits([]int{5, 6, 7, 8, 9}),
				},
			},
			expectError: false,
			description: "10 shards >= 10 data shards required",
		},
		{
			name: "exactly minimum data shards",
			distribution: ShardDistribution{
				Generation: 1,
				Nodes: map[pb.ServerAddress]erasure_coding.ShardBits{
					"node1:8080": createShardBits([]int{0, 1, 2, 3, 4}),
					"node2:8080": createShardBits([]int{5, 6, 7, 8, 9}),
				},
			},
			expectError: false,
			description: "Exactly 10 data shards",
		},
		{
			name: "insufficient shards",
			distribution: ShardDistribution{
				Generation: 1,
				Nodes: map[pb.ServerAddress]erasure_coding.ShardBits{
					"node1:8080": createShardBits([]int{0, 1, 2}),
					"node2:8080": createShardBits([]int{3, 4, 5}),
				},
			},
			expectError: true,
			description: "Only 6 shards < 10 data shards required",
		},
		{
			name: "all shards available",
			distribution: ShardDistribution{
				Generation: 1,
				Nodes: map[pb.ServerAddress]erasure_coding.ShardBits{
					"node1:8080": createShardBits([]int{0, 1, 2, 3, 4}),
					"node2:8080": createShardBits([]int{5, 6, 7, 8, 9}),
					"node3:8080": createShardBits([]int{10, 11, 12, 13}),
				},
			},
			expectError: false,
			description: "All 14 shards available",
		},
		{
			name: "single node with all shards",
			distribution: ShardDistribution{
				Generation: 1,
				Nodes: map[pb.ServerAddress]erasure_coding.ShardBits{
					"node1:8080": createShardBits([]int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13}),
				},
			},
			expectError: false,
			description: "All shards on single node",
		},
		{
			name: "empty distribution",
			distribution: ShardDistribution{
				Generation: 1,
				Nodes:      map[pb.ServerAddress]erasure_coding.ShardBits{},
			},
			expectError: true,
			description: "No shards available",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := logic.ValidateShardDistribution(tt.distribution)

			if tt.expectError && err == nil {
				t.Errorf("expected error for %s but got none", tt.description)
			}

			if !tt.expectError && err != nil {
				t.Errorf("unexpected error for %s: %v", tt.description, err)
			}
		})
	}
}

func TestCreateVacuumPlan(t *testing.T) {
	logic := NewEcVacuumLogic()

	tests := []struct {
		name        string
		volumeID    uint32
		collection  string
		params      *worker_pb.TaskParams
		expectError bool
		validate    func(*testing.T, *VacuumPlan)
	}{
		{
			name:       "basic generation 0 to 1 plan",
			volumeID:   123,
			collection: "test",
			params: &worker_pb.TaskParams{
				Sources: []*worker_pb.TaskSource{
					{
						Node:       "node1:8080",
						Generation: 0,
						ShardIds:   []uint32{0, 1, 2, 3, 4, 5},
					},
					{
						Node:       "node2:8080",
						Generation: 0,
						ShardIds:   []uint32{6, 7, 8, 9, 10, 11, 12, 13},
					},
				},
			},
			validate: func(t *testing.T, plan *VacuumPlan) {
				if plan.VolumeID != 123 {
					t.Errorf("volume ID: expected 123, got %d", plan.VolumeID)
				}
				if plan.Collection != "test" {
					t.Errorf("collection: expected 'test', got '%s'", plan.Collection)
				}
				if plan.CurrentGeneration != 0 {
					t.Errorf("current generation: expected 0, got %d", plan.CurrentGeneration)
				}
				if plan.TargetGeneration != 1 {
					t.Errorf("target generation: expected 1, got %d", plan.TargetGeneration)
				}
				if len(plan.GenerationsToCleanup) != 1 || plan.GenerationsToCleanup[0] != 0 {
					t.Errorf("cleanup generations: expected [0], got %v", plan.GenerationsToCleanup)
				}
				if len(plan.SourceDistribution.Nodes) != 2 {
					t.Errorf("source nodes: expected 2, got %d", len(plan.SourceDistribution.Nodes))
				}
				if len(plan.ExpectedDistribution.Nodes) != 2 {
					t.Errorf("expected nodes: expected 2, got %d", len(plan.ExpectedDistribution.Nodes))
				}
			},
		},
		{
			name:       "generation 3 to 4 plan",
			volumeID:   456,
			collection: "data",
			params: &worker_pb.TaskParams{
				Sources: []*worker_pb.TaskSource{
					{
						Node:       "node1:8080",
						Generation: 3,
						ShardIds:   []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
					},
					{
						Node:       "node2:8080",
						Generation: 3,
						ShardIds:   []uint32{10, 11, 12, 13},
					},
				},
			},
			validate: func(t *testing.T, plan *VacuumPlan) {
				if plan.CurrentGeneration != 3 {
					t.Errorf("current generation: expected 3, got %d", plan.CurrentGeneration)
				}
				if plan.TargetGeneration != 4 {
					t.Errorf("target generation: expected 4, got %d", plan.TargetGeneration)
				}
				if len(plan.GenerationsToCleanup) != 1 || plan.GenerationsToCleanup[0] != 3 {
					t.Errorf("cleanup generations: expected [3], got %v", plan.GenerationsToCleanup)
				}
			},
		},
		{
			name:       "inconsistent generations",
			volumeID:   789,
			collection: "test",
			params: &worker_pb.TaskParams{
				Sources: []*worker_pb.TaskSource{
					{Generation: 1},
					{Generation: 2},
				},
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan, err := logic.CreateVacuumPlan(tt.volumeID, tt.collection, tt.params)

			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if tt.validate != nil {
				tt.validate(t, plan)
			}
		})
	}
}

func TestCalculateCleanupGenerations(t *testing.T) {
	logic := NewEcVacuumLogic()

	tests := []struct {
		name                 string
		currentGen           uint32
		targetGen            uint32
		availableGenerations []uint32
		expectedCleanup      []uint32
	}{
		{
			name:                 "single generation cleanup",
			currentGen:           0,
			targetGen:            1,
			availableGenerations: []uint32{0, 1},
			expectedCleanup:      []uint32{0}, // Don't cleanup target generation 1
		},
		{
			name:                 "multiple generations cleanup",
			currentGen:           2,
			targetGen:            3,
			availableGenerations: []uint32{0, 1, 2, 3},
			expectedCleanup:      []uint32{0, 1, 2}, // Don't cleanup target generation 3
		},
		{
			name:                 "no cleanup needed",
			currentGen:           0,
			targetGen:            1,
			availableGenerations: []uint32{1},
			expectedCleanup:      []uint32{}, // Only target generation exists
		},
		{
			name:                 "cleanup all except target",
			currentGen:           5,
			targetGen:            6,
			availableGenerations: []uint32{0, 1, 2, 3, 4, 5, 6},
			expectedCleanup:      []uint32{0, 1, 2, 3, 4, 5}, // Don't cleanup target generation 6
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := logic.CalculateCleanupGenerations(tt.currentGen, tt.targetGen, tt.availableGenerations)

			if len(result) != len(tt.expectedCleanup) {
				t.Errorf("cleanup generations length: expected %d, got %d", len(tt.expectedCleanup), len(result))
				return
			}

			// Convert to map for easier comparison
			expectedMap := make(map[uint32]bool)
			for _, gen := range tt.expectedCleanup {
				expectedMap[gen] = true
			}

			for _, gen := range result {
				if !expectedMap[gen] {
					t.Errorf("unexpected generation in cleanup: %d", gen)
				}
				delete(expectedMap, gen)
			}

			// Check for missing generations
			for gen := range expectedMap {
				t.Errorf("missing generation in cleanup: %d", gen)
			}
		})
	}
}

func TestEstimateCleanupImpact(t *testing.T) {
	logic := NewEcVacuumLogic()

	plan := &VacuumPlan{
		VolumeID:          123,
		CurrentGeneration: 2,
		TargetGeneration:  3,
		SourceDistribution: ShardDistribution{
			Generation: 2,
			Nodes: map[pb.ServerAddress]erasure_coding.ShardBits{
				"node1:8080": createShardBits([]int{0, 1, 2, 3, 4}),
				"node2:8080": createShardBits([]int{5, 6, 7, 8, 9}),
				"node3:8080": createShardBits([]int{10, 11, 12, 13}),
			},
		},
		GenerationsToCleanup: []uint32{0, 1, 2}, // 3 generations to cleanup
	}

	volumeSize := uint64(1000000) // 1MB

	impact := logic.EstimateCleanupImpact(plan, volumeSize)

	if impact.GenerationsToCleanup != 3 {
		t.Errorf("generations to cleanup: expected 3, got %d", impact.GenerationsToCleanup)
	}

	if impact.EstimatedSizeFreed != 3000000 { // 3 generations * 1MB each
		t.Errorf("estimated size freed: expected 3000000, got %d", impact.EstimatedSizeFreed)
	}

	if impact.NodesAffected != 3 {
		t.Errorf("nodes affected: expected 3, got %d", impact.NodesAffected)
	}

	expectedShardsToDelete := (5 + 5 + 4) * 3 // Total shards per generation * generations
	if impact.ShardsToDelete != expectedShardsToDelete {
		t.Errorf("shards to delete: expected %d, got %d", expectedShardsToDelete, impact.ShardsToDelete)
	}
}

// Helper function to create ShardBits from shard ID slice
func createShardBits(shardIds []int) erasure_coding.ShardBits {
	var bits erasure_coding.ShardBits
	for _, id := range shardIds {
		bits = bits.AddShardId(erasure_coding.ShardId(id))
	}
	return bits
}

// Test helper to create realistic topology scenarios
func createRealisticTopologyTest(t *testing.T) {
	logic := NewEcVacuumLogic()

	// Scenario: 3-node cluster with distributed EC shards
	params := &worker_pb.TaskParams{
		VolumeId: 100,
		Sources: []*worker_pb.TaskSource{
			{
				Node:       "volume1:8080",
				Generation: 1,
				ShardIds:   []uint32{0, 1, 2, 3, 4},
			},
			{
				Node:       "volume2:8080",
				Generation: 1,
				ShardIds:   []uint32{5, 6, 7, 8, 9},
			},
			{
				Node:       "volume3:8080",
				Generation: 1,
				ShardIds:   []uint32{10, 11, 12, 13},
			},
		},
	}

	plan, err := logic.CreateVacuumPlan(100, "data", params)
	if err != nil {
		t.Fatalf("failed to create plan: %v", err)
	}

	// Validate the plan makes sense
	if plan.CurrentGeneration != 1 || plan.TargetGeneration != 2 {
		t.Errorf("generation transition: expected 1->2, got %d->%d",
			plan.CurrentGeneration, plan.TargetGeneration)
	}

	// Validate shard distribution
	err = logic.ValidateShardDistribution(plan.SourceDistribution)
	if err != nil {
		t.Errorf("invalid source distribution: %v", err)
	}

	// All source nodes should become destination nodes
	if len(plan.SourceDistribution.Nodes) != len(plan.ExpectedDistribution.Nodes) {
		t.Errorf("source/destination node count mismatch: %d vs %d",
			len(plan.SourceDistribution.Nodes), len(plan.ExpectedDistribution.Nodes))
	}

	t.Logf("Plan created successfully:")
	t.Logf("  Volume: %d, Collection: %s", plan.VolumeID, plan.Collection)
	t.Logf("  Generation: %d -> %d", plan.CurrentGeneration, plan.TargetGeneration)
	t.Logf("  Nodes: %d", len(plan.SourceDistribution.Nodes))
	t.Logf("  Cleanup: %v", plan.GenerationsToCleanup)
	t.Logf("  Safety checks: %d", len(plan.SafetyChecks))
}

func TestRealisticTopologyScenarios(t *testing.T) {
	t.Run("3-node distributed shards", createRealisticTopologyTest)
}
