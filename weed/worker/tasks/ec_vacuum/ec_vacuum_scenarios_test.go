package ec_vacuum

import (
	"fmt"
	"sort"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
)

// TestTopologyBasedTaskGeneration tests generating EC vacuum tasks from different active topologies
func TestTopologyBasedTaskGeneration(t *testing.T) {
	scenarios := []struct {
		name        string
		topology    TopologyScenario
		expectTasks int
		validate    func(*testing.T, []*GeneratedTask)
	}{
		{
			name: "single_volume_distributed_shards",
			topology: TopologyScenario{
				Volumes: []VolumeTopology{
					{
						VolumeID:   100,
						Collection: "data",
						Generation: 0,
						ShardDistribution: map[string][]int{
							"node1:8080": {0, 1, 2, 3, 4},
							"node2:8080": {5, 6, 7, 8, 9},
							"node3:8080": {10, 11, 12, 13},
						},
						Size:          1000000,
						DeletionRatio: 0.4,
					},
				},
			},
			expectTasks: 1,
			validate: func(t *testing.T, tasks []*GeneratedTask) {
				task := tasks[0]
				if task.VolumeID != 100 {
					t.Errorf("volume ID: expected 100, got %d", task.VolumeID)
				}
				if len(task.SourceNodes) != 3 {
					t.Errorf("source nodes: expected 3, got %d", len(task.SourceNodes))
				}

				// Verify all shards are accounted for
				totalShards := 0
				for _, shards := range task.SourceNodes {
					totalShards += len(shards)
				}
				if totalShards != 14 {
					t.Errorf("total shards: expected 14, got %d", totalShards)
				}
			},
		},
		{
			name: "multiple_volumes_different_generations",
			topology: TopologyScenario{
				Volumes: []VolumeTopology{
					{
						VolumeID:   200,
						Generation: 0,
						ShardDistribution: map[string][]int{
							"node1:8080": {0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
							"node2:8080": {10, 11, 12, 13},
						},
						DeletionRatio: 0.6,
					},
					{
						VolumeID:   201,
						Generation: 2,
						ShardDistribution: map[string][]int{
							"node1:8080": {0, 1, 2, 3, 4},
							"node2:8080": {5, 6, 7, 8, 9},
							"node3:8080": {10, 11, 12, 13},
						},
						DeletionRatio: 0.5,
					},
				},
			},
			expectTasks: 2,
			validate: func(t *testing.T, tasks []*GeneratedTask) {
				// Sort tasks by volume ID for predictable testing
				sort.Slice(tasks, func(i, j int) bool {
					return tasks[i].VolumeID < tasks[j].VolumeID
				})

				// Validate volume 200 (generation 0 -> 1)
				task0 := tasks[0]
				if task0.SourceGeneration != 0 || task0.TargetGeneration != 1 {
					t.Errorf("volume 200 generations: expected 0->1, got %d->%d",
						task0.SourceGeneration, task0.TargetGeneration)
				}

				// Validate volume 201 (generation 2 -> 3)
				task1 := tasks[1]
				if task1.SourceGeneration != 2 || task1.TargetGeneration != 3 {
					t.Errorf("volume 201 generations: expected 2->3, got %d->%d",
						task1.SourceGeneration, task1.TargetGeneration)
				}
			},
		},
		{
			name: "unbalanced_shard_distribution",
			topology: TopologyScenario{
				Volumes: []VolumeTopology{
					{
						VolumeID:   300,
						Generation: 1,
						ShardDistribution: map[string][]int{
							"node1:8080": {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, // 11 shards
							"node2:8080": {11, 12, 13},                       // 3 shards
						},
						DeletionRatio: 0.3,
					},
				},
			},
			expectTasks: 1,
			validate: func(t *testing.T, tasks []*GeneratedTask) {
				task := tasks[0]

				// Verify unbalanced distribution is handled correctly
				node1Shards := len(task.SourceNodes["node1:8080"])
				node2Shards := len(task.SourceNodes["node2:8080"])

				if node1Shards != 11 {
					t.Errorf("node1 shards: expected 11, got %d", node1Shards)
				}
				if node2Shards != 3 {
					t.Errorf("node2 shards: expected 3, got %d", node2Shards)
				}

				// Total should still be 14
				if node1Shards+node2Shards != 14 {
					t.Errorf("total shards: expected 14, got %d", node1Shards+node2Shards)
				}
			},
		},
		{
			name: "insufficient_shards_for_reconstruction",
			topology: TopologyScenario{
				Volumes: []VolumeTopology{
					{
						VolumeID:   400,
						Generation: 0,
						ShardDistribution: map[string][]int{
							"node1:8080": {0, 1, 2}, // Only 6 shards total < 10 required
							"node2:8080": {3, 4, 5},
						},
						DeletionRatio: 0.8,
					},
				},
			},
			expectTasks: 0, // Should not generate task due to insufficient shards
		},
	}

	generator := NewTopologyTaskGenerator()

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			tasks, err := generator.GenerateEcVacuumTasks(scenario.topology)
			if err != nil {
				t.Fatalf("failed to generate tasks: %v", err)
			}

			if len(tasks) != scenario.expectTasks {
				t.Errorf("task count: expected %d, got %d", scenario.expectTasks, len(tasks))
				return
			}

			if scenario.validate != nil {
				scenario.validate(t, tasks)
			}
		})
	}
}

// TestShardSelectionAndDeletion tests what shards are actually selected and deleted
func TestShardSelectionAndDeletion(t *testing.T) {
	scenarios := []struct {
		name         string
		initialState MultiGenerationState
		expectedPlan ExpectedDeletionPlan
	}{
		{
			name: "single_generation_cleanup",
			initialState: MultiGenerationState{
				VolumeID:   500,
				Collection: "test",
				Generations: map[uint32]GenerationData{
					0: {
						ShardDistribution: map[string][]int{
							"node1:8080": {0, 1, 2, 3, 4, 5},
							"node2:8080": {6, 7, 8, 9, 10, 11, 12, 13},
						},
						FilesOnDisk: []string{
							"test_500.ec00", "test_500.ec01", "test_500.ec02", "test_500.ec03", "test_500.ec04", "test_500.ec05",
							"test_500.ec06", "test_500.ec07", "test_500.ec08", "test_500.ec09", "test_500.ec10", "test_500.ec11", "test_500.ec12", "test_500.ec13",
							"test_500.ecx", "test_500.ecj", "test_500.vif",
						},
					},
				},
				ActiveGeneration: 0,
			},
			expectedPlan: ExpectedDeletionPlan{
				SourceGeneration:    0,
				TargetGeneration:    1,
				GenerationsToDelete: []uint32{0},
				ShardsToDeleteByNode: map[string][]int{
					"node1:8080": {0, 1, 2, 3, 4, 5},
					"node2:8080": {6, 7, 8, 9, 10, 11, 12, 13},
				},
				FilesToDeleteByNode: map[string][]string{
					"node1:8080": {
						"test_500.ec00", "test_500.ec01", "test_500.ec02", "test_500.ec03", "test_500.ec04", "test_500.ec05",
						"test_500.ecx", "test_500.ecj", "test_500.vif",
					},
					"node2:8080": {
						"test_500.ec06", "test_500.ec07", "test_500.ec08", "test_500.ec09", "test_500.ec10", "test_500.ec11", "test_500.ec12", "test_500.ec13",
					},
				},
				ExpectedFilesAfterCleanup: []string{
					// New generation 1 files
					"test_500_g1.ec00", "test_500_g1.ec01", "test_500_g1.ec02", "test_500_g1.ec03", "test_500_g1.ec04", "test_500_g1.ec05",
					"test_500_g1.ec06", "test_500_g1.ec07", "test_500_g1.ec08", "test_500_g1.ec09", "test_500_g1.ec10", "test_500_g1.ec11", "test_500_g1.ec12", "test_500_g1.ec13",
					"test_500_g1.ecx", "test_500_g1.ecj", "test_500_g1.vif",
				},
			},
		},
		{
			name: "multi_generation_cleanup",
			initialState: MultiGenerationState{
				VolumeID:   600,
				Collection: "data",
				Generations: map[uint32]GenerationData{
					0: {
						ShardDistribution: map[string][]int{
							"node1:8080": {0, 1, 2, 3, 4},
							"node2:8080": {5, 6, 7, 8, 9},
							"node3:8080": {10, 11, 12, 13},
						},
						FilesOnDisk: []string{
							"data_600.ec00", "data_600.ec01", "data_600.ec02", "data_600.ec03", "data_600.ec04",
							"data_600.ec05", "data_600.ec06", "data_600.ec07", "data_600.ec08", "data_600.ec09",
							"data_600.ec10", "data_600.ec11", "data_600.ec12", "data_600.ec13",
							"data_600.ecx", "data_600.ecj", "data_600.vif",
						},
					},
					1: {
						ShardDistribution: map[string][]int{
							"node1:8080": {0, 1, 2, 3, 4},
							"node2:8080": {5, 6, 7, 8, 9},
							"node3:8080": {10, 11, 12, 13},
						},
						FilesOnDisk: []string{
							"data_600_g1.ec00", "data_600_g1.ec01", "data_600_g1.ec02", "data_600_g1.ec03", "data_600_g1.ec04",
							"data_600_g1.ec05", "data_600_g1.ec06", "data_600_g1.ec07", "data_600_g1.ec08", "data_600_g1.ec09",
							"data_600_g1.ec10", "data_600_g1.ec11", "data_600_g1.ec12", "data_600_g1.ec13",
							"data_600_g1.ecx", "data_600_g1.ecj", "data_600_g1.vif",
						},
					},
					2: {
						ShardDistribution: map[string][]int{
							"node1:8080": {0, 1, 2, 3, 4},
							"node2:8080": {5, 6, 7, 8, 9},
							"node3:8080": {10, 11, 12, 13},
						},
						FilesOnDisk: []string{
							"data_600_g2.ec00", "data_600_g2.ec01", "data_600_g2.ec02", "data_600_g2.ec03", "data_600_g2.ec04",
							"data_600_g2.ec05", "data_600_g2.ec06", "data_600_g2.ec07", "data_600_g2.ec08", "data_600_g2.ec09",
							"data_600_g2.ec10", "data_600_g2.ec11", "data_600_g2.ec12", "data_600_g2.ec13",
							"data_600_g2.ecx", "data_600_g2.ecj", "data_600_g2.vif",
						},
					},
				},
				ActiveGeneration: 2,
			},
			expectedPlan: ExpectedDeletionPlan{
				SourceGeneration:    2,
				TargetGeneration:    3,
				GenerationsToDelete: []uint32{2}, // Only current generation (0 and 1 should have been cleaned up in previous runs)
				ShardsToDeleteByNode: map[string][]int{
					"node1:8080": {0, 1, 2, 3, 4},
					"node2:8080": {5, 6, 7, 8, 9},
					"node3:8080": {10, 11, 12, 13},
				},
				FilesToDeleteByNode: map[string][]string{
					"node1:8080": {
						"data_600_g2.ec00", "data_600_g2.ec01", "data_600_g2.ec02", "data_600_g2.ec03", "data_600_g2.ec04",
						"data_600_g2.ecx", "data_600_g2.ecj", "data_600_g2.vif",
					},
					"node2:8080": {
						"data_600_g2.ec05", "data_600_g2.ec06", "data_600_g2.ec07", "data_600_g2.ec08", "data_600_g2.ec09",
					},
					"node3:8080": {
						"data_600_g2.ec10", "data_600_g2.ec11", "data_600_g2.ec12", "data_600_g2.ec13",
					},
				},
				ExpectedFilesAfterCleanup: []string{
					// Old generations should remain (they should have been cleaned up before)
					"data_600.ec00", "data_600.ec01", "data_600.ec02", "data_600.ec03", "data_600.ec04",
					"data_600.ec05", "data_600.ec06", "data_600.ec07", "data_600.ec08", "data_600.ec09",
					"data_600.ec10", "data_600.ec11", "data_600.ec12", "data_600.ec13",
					"data_600.ecx", "data_600.ecj", "data_600.vif",
					"data_600_g1.ec00", "data_600_g1.ec01", "data_600_g1.ec02", "data_600_g1.ec03", "data_600_g1.ec04",
					"data_600_g1.ec05", "data_600_g1.ec06", "data_600_g1.ec07", "data_600_g1.ec08", "data_600_g1.ec09",
					"data_600_g1.ec10", "data_600_g1.ec11", "data_600_g1.ec12", "data_600_g1.ec13",
					"data_600_g1.ecx", "data_600_g1.ecj", "data_600_g1.vif",
					// New generation 3 files
					"data_600_g3.ec00", "data_600_g3.ec01", "data_600_g3.ec02", "data_600_g3.ec03", "data_600_g3.ec04",
					"data_600_g3.ec05", "data_600_g3.ec06", "data_600_g3.ec07", "data_600_g3.ec08", "data_600_g3.ec09",
					"data_600_g3.ec10", "data_600_g3.ec11", "data_600_g3.ec12", "data_600_g3.ec13",
					"data_600_g3.ecx", "data_600_g3.ecj", "data_600_g3.vif",
				},
			},
		},
	}

	logic := NewEcVacuumLogic()

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			// Convert multi-generation state to task parameters
			params := convertMultiGenerationStateToParams(scenario.initialState)

			// Create vacuum plan
			plan, err := logic.CreateVacuumPlan(scenario.initialState.VolumeID, scenario.initialState.Collection, params)
			if err != nil {
				t.Fatalf("failed to create plan: %v", err)
			}

			// Validate generation transitions
			if plan.CurrentGeneration != scenario.expectedPlan.SourceGeneration {
				t.Errorf("source generation: expected %d, got %d",
					scenario.expectedPlan.SourceGeneration, plan.CurrentGeneration)
			}

			if plan.TargetGeneration != scenario.expectedPlan.TargetGeneration {
				t.Errorf("target generation: expected %d, got %d",
					scenario.expectedPlan.TargetGeneration, plan.TargetGeneration)
			}

			// Validate cleanup generations
			if !equalUint32Slices(plan.GenerationsToCleanup, scenario.expectedPlan.GenerationsToDelete) {
				t.Errorf("cleanup generations: expected %v, got %v",
					scenario.expectedPlan.GenerationsToDelete, plan.GenerationsToCleanup)
			}

			// Validate shard distribution
			for nodeAddr, expectedShards := range scenario.expectedPlan.ShardsToDeleteByNode {
				shardBits, exists := plan.SourceDistribution.Nodes[pb.ServerAddress(nodeAddr)]
				if !exists {
					t.Errorf("expected node %s not found in plan", nodeAddr)
					continue
				}

				actualShards := shardBitsToSlice(shardBits)
				if !equalIntSlices(actualShards, expectedShards) {
					t.Errorf("node %s shards: expected %v, got %v", nodeAddr, expectedShards, actualShards)
				}
			}

			t.Logf("Plan validation successful:")
			t.Logf("  Volume: %d (%s)", plan.VolumeID, plan.Collection)
			t.Logf("  Generation transition: %d -> %d", plan.CurrentGeneration, plan.TargetGeneration)
			t.Logf("  Cleanup generations: %v", plan.GenerationsToCleanup)
			t.Logf("  Nodes affected: %d", len(plan.SourceDistribution.Nodes))

			// Estimate cleanup impact
			impact := logic.EstimateCleanupImpact(plan, 1000000) // 1MB volume
			t.Logf("  Estimated impact: %d shards deleted, %d bytes freed",
				impact.ShardsToDelete, impact.EstimatedSizeFreed)
		})
	}
}

// Test data structures for comprehensive testing
type VolumeTopology struct {
	VolumeID          uint32
	Collection        string
	Generation        uint32
	ShardDistribution map[string][]int // node -> shard IDs
	Size              uint64
	DeletionRatio     float64
}

type TopologyScenario struct {
	Volumes []VolumeTopology
}

type GenerationData struct {
	ShardDistribution map[string][]int // node -> shard IDs
	FilesOnDisk       []string
}

type MultiGenerationState struct {
	VolumeID         uint32
	Collection       string
	Generations      map[uint32]GenerationData
	ActiveGeneration uint32
}

type ExpectedDeletionPlan struct {
	SourceGeneration          uint32
	TargetGeneration          uint32
	GenerationsToDelete       []uint32
	ShardsToDeleteByNode      map[string][]int
	FilesToDeleteByNode       map[string][]string
	ExpectedFilesAfterCleanup []string
}

type GeneratedTask struct {
	VolumeID         uint32
	Collection       string
	SourceGeneration uint32
	TargetGeneration uint32
	SourceNodes      map[string][]int // node -> shard IDs
}

type TopologyTaskGenerator struct {
	logic *EcVacuumLogic
}

func NewTopologyTaskGenerator() *TopologyTaskGenerator {
	return &TopologyTaskGenerator{
		logic: NewEcVacuumLogic(),
	}
}

func (g *TopologyTaskGenerator) GenerateEcVacuumTasks(scenario TopologyScenario) ([]*GeneratedTask, error) {
	var tasks []*GeneratedTask

	for _, volume := range scenario.Volumes {
		// Check if volume qualifies for vacuum (sufficient shards + deletion ratio)
		if !g.qualifiesForVacuum(volume) {
			continue
		}

		// Convert to task parameters
		params := g.volumeTopologyToParams(volume)

		// Create plan using logic
		plan, err := g.logic.CreateVacuumPlan(volume.VolumeID, volume.Collection, params)
		if err != nil {
			return nil, fmt.Errorf("failed to create plan for volume %d: %w", volume.VolumeID, err)
		}

		// Convert plan to generated task
		task := &GeneratedTask{
			VolumeID:         plan.VolumeID,
			Collection:       plan.Collection,
			SourceGeneration: plan.CurrentGeneration,
			TargetGeneration: plan.TargetGeneration,
			SourceNodes:      make(map[string][]int),
		}

		// Convert shard distribution
		for node, shardBits := range plan.SourceDistribution.Nodes {
			task.SourceNodes[string(node)] = shardBitsToSlice(shardBits)
		}

		tasks = append(tasks, task)
	}

	return tasks, nil
}

func (g *TopologyTaskGenerator) qualifiesForVacuum(volume VolumeTopology) bool {
	// Check deletion ratio threshold (minimum 0.3)
	if volume.DeletionRatio < 0.3 {
		return false
	}

	// Check sufficient shards for reconstruction
	totalShards := 0
	for _, shards := range volume.ShardDistribution {
		totalShards += len(shards)
	}

	return totalShards >= erasure_coding.DataShardsCount
}

func (g *TopologyTaskGenerator) volumeTopologyToParams(volume VolumeTopology) *worker_pb.TaskParams {
	var sources []*worker_pb.TaskSource

	for node, shardIds := range volume.ShardDistribution {
		shardIds32 := make([]uint32, len(shardIds))
		for i, id := range shardIds {
			shardIds32[i] = uint32(id)
		}

		sources = append(sources, &worker_pb.TaskSource{
			Node:       node,
			VolumeId:   volume.VolumeID,
			ShardIds:   shardIds32,
			Generation: volume.Generation,
		})
	}

	return &worker_pb.TaskParams{
		VolumeId: volume.VolumeID,
		Sources:  sources,
	}
}

// Helper functions
func convertMultiGenerationStateToParams(state MultiGenerationState) *worker_pb.TaskParams {
	// Use active generation as source
	activeData := state.Generations[state.ActiveGeneration]

	var sources []*worker_pb.TaskSource
	for node, shardIds := range activeData.ShardDistribution {
		shardIds32 := make([]uint32, len(shardIds))
		for i, id := range shardIds {
			shardIds32[i] = uint32(id)
		}

		sources = append(sources, &worker_pb.TaskSource{
			Node:       node,
			VolumeId:   state.VolumeID,
			ShardIds:   shardIds32,
			Generation: state.ActiveGeneration,
		})
	}

	return &worker_pb.TaskParams{
		VolumeId: state.VolumeID,
		Sources:  sources,
	}
}

func shardBitsToSlice(bits erasure_coding.ShardBits) []int {
	var shards []int
	for i := 0; i < erasure_coding.TotalShardsCount; i++ {
		if bits.HasShardId(erasure_coding.ShardId(i)) {
			shards = append(shards, i)
		}
	}
	return shards
}

func equalUint32Slices(a, b []uint32) bool {
	if len(a) != len(b) {
		return false
	}
	sortedA := make([]uint32, len(a))
	sortedB := make([]uint32, len(b))
	copy(sortedA, a)
	copy(sortedB, b)
	sort.Slice(sortedA, func(i, j int) bool { return sortedA[i] < sortedA[j] })
	sort.Slice(sortedB, func(i, j int) bool { return sortedB[i] < sortedB[j] })

	for i := range sortedA {
		if sortedA[i] != sortedB[i] {
			return false
		}
	}
	return true
}

func equalIntSlices(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	sortedA := make([]int, len(a))
	sortedB := make([]int, len(b))
	copy(sortedA, a)
	copy(sortedB, b)
	sort.Ints(sortedA)
	sort.Ints(sortedB)

	for i := range sortedA {
		if sortedA[i] != sortedB[i] {
			return false
		}
	}
	return true
}
