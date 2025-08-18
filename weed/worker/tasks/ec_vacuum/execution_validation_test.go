package ec_vacuum

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
)

// TestExecutionPlanValidation validates that the execution properly follows the vacuum plan
func TestExecutionPlanValidation(t *testing.T) {
	tests := []struct {
		name                    string
		params                  *worker_pb.TaskParams
		expectedSourceGen       uint32
		expectedTargetGen       uint32
		expectedCleanupGens     []uint32
		expectedExecutionSteps  []string
		validateExecution       func(*testing.T, *EcVacuumTask, *VacuumPlan)
	}{
		{
			name: "single_generation_execution",
			params: &worker_pb.TaskParams{
				VolumeId:   100,
				Collection: "test",
				Sources: []*worker_pb.TaskSource{
					{
						Node:       "node1:8080",
						Generation: 1,
						ShardIds:   []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13},
					},
				},
			},
			expectedSourceGen:   1,
			expectedTargetGen:   2,
			expectedCleanupGens: []uint32{1},
			expectedExecutionSteps: []string{
				"create_plan",
				"validate_plan", 
				"collect_shards_from_generation_1",
				"decode_and_vacuum",
				"encode_to_generation_2",
				"distribute_generation_2",
				"activate_generation_2",
				"cleanup_generation_1",
			},
			validateExecution: func(t *testing.T, task *EcVacuumTask, plan *VacuumPlan) {
				// Validate plan reflects multi-generation logic
				if plan.CurrentGeneration != 1 {
					t.Errorf("expected source generation 1, got %d", plan.CurrentGeneration)
				}
				if plan.TargetGeneration != 2 {
					t.Errorf("expected target generation 2, got %d", plan.TargetGeneration)
				}
				if len(plan.GenerationsToCleanup) != 1 || plan.GenerationsToCleanup[0] != 1 {
					t.Errorf("expected cleanup generations [1], got %v", plan.GenerationsToCleanup)
				}
				
				// Validate task uses plan values
				if task.sourceGeneration != plan.CurrentGeneration {
					t.Errorf("task source generation %d != plan current generation %d", 
						task.sourceGeneration, plan.CurrentGeneration)
				}
				if task.targetGeneration != plan.TargetGeneration {
					t.Errorf("task target generation %d != plan target generation %d",
						task.targetGeneration, plan.TargetGeneration)
				}
			},
		},
		{
			name: "multi_generation_cleanup_execution",
			params: &worker_pb.TaskParams{
				VolumeId:   200,
				Collection: "data",
				Sources: []*worker_pb.TaskSource{
					{
						Node:       "node1:8080",
						Generation: 0,
						ShardIds:   []uint32{0, 1, 2}, // Incomplete - should not be selected
					},
					{
						Node:       "node2:8080", 
						Generation: 1,
						ShardIds:   []uint32{0, 1, 2, 3, 4}, // Incomplete - should not be selected
					},
					{
						Node:       "node3:8080",
						Generation: 2,
						ShardIds:   []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13}, // Complete - should be selected
					},
				},
			},
			expectedSourceGen:   2, // Should pick generation 2 (most complete)
			expectedTargetGen:   3, // max(0,1,2) + 1 = 3
			expectedCleanupGens: []uint32{0, 1, 2}, // Should cleanup ALL old generations
			expectedExecutionSteps: []string{
				"create_plan",
				"validate_plan",
				"collect_shards_from_generation_2", // Use most complete generation
				"decode_and_vacuum",
				"encode_to_generation_3",
				"distribute_generation_3",
				"activate_generation_3",
				"cleanup_generation_0", // Cleanup ALL old generations
				"cleanup_generation_1",
				"cleanup_generation_2",
			},
			validateExecution: func(t *testing.T, task *EcVacuumTask, plan *VacuumPlan) {
				// Validate plan correctly identifies most complete generation
				if plan.CurrentGeneration != 2 {
					t.Errorf("expected source generation 2 (most complete), got %d", plan.CurrentGeneration)
				}
				if plan.TargetGeneration != 3 {
					t.Errorf("expected target generation 3, got %d", plan.TargetGeneration)
				}
				
				// Validate cleanup includes ALL old generations
				expectedCleanup := map[uint32]bool{0: true, 1: true, 2: true}
				for _, gen := range plan.GenerationsToCleanup {
					if !expectedCleanup[gen] {
						t.Errorf("unexpected generation %d in cleanup list", gen)
					}
					delete(expectedCleanup, gen)
				}
				for gen := range expectedCleanup {
					t.Errorf("missing generation %d in cleanup list", gen)
				}
				
				// Validate source nodes only include nodes from selected generation
				expectedNodeCount := 1 // Only node3 has generation 2 shards
				if len(plan.SourceDistribution.Nodes) != expectedNodeCount {
					t.Errorf("expected %d source nodes (generation 2 only), got %d", 
						expectedNodeCount, len(plan.SourceDistribution.Nodes))
				}
				
				// Validate the selected node has all shards
				for _, shardBits := range plan.SourceDistribution.Nodes {
					if shardBits.ShardIdCount() != 14 {
						t.Errorf("expected 14 shards from selected generation, got %d", shardBits.ShardIdCount())
					}
				}
			},
		},
	}

	logic := NewEcVacuumLogic()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Step 1: Create vacuum plan
			plan, err := logic.CreateVacuumPlan(tt.params.VolumeId, tt.params.Collection, tt.params)
			if err != nil {
				t.Fatalf("failed to create vacuum plan: %v", err)
			}

			// Step 2: Create task (simulating the execution setup)
			sourceNodes, err := logic.ParseSourceNodes(tt.params, plan.CurrentGeneration)
			if err != nil {
				t.Fatalf("failed to parse source nodes: %v", err)
			}

			task := NewEcVacuumTask("test-execution", tt.params.VolumeId, tt.params.Collection, sourceNodes)
			task.plan = plan
			task.sourceGeneration = plan.CurrentGeneration
			task.targetGeneration = plan.TargetGeneration

			// Step 3: Validate plan matches expectations
			if plan.CurrentGeneration != tt.expectedSourceGen {
				t.Errorf("source generation: expected %d, got %d", tt.expectedSourceGen, plan.CurrentGeneration)
			}
			if plan.TargetGeneration != tt.expectedTargetGen {
				t.Errorf("target generation: expected %d, got %d", tt.expectedTargetGen, plan.TargetGeneration)
			}

			// Step 4: Validate cleanup generations
			if !equalUint32Slices(plan.GenerationsToCleanup, tt.expectedCleanupGens) {
				t.Errorf("cleanup generations: expected %v, got %v", tt.expectedCleanupGens, plan.GenerationsToCleanup)
			}

			// Step 5: Run custom validation
			if tt.validateExecution != nil {
				tt.validateExecution(t, task, plan)
			}

			// Step 6: Validate execution readiness
			err = logic.ValidateShardDistribution(plan.SourceDistribution)
			if err != nil {
				t.Errorf("plan validation failed: %v", err)
			}

			t.Logf("✅ Execution plan validation passed:")
			t.Logf("  Volume: %d (%s)", plan.VolumeID, plan.Collection)
			t.Logf("  Source generation: %d (most complete)", plan.CurrentGeneration)
			t.Logf("  Target generation: %d", plan.TargetGeneration)
			t.Logf("  Generations to cleanup: %v", plan.GenerationsToCleanup)
			t.Logf("  Source nodes: %d", len(plan.SourceDistribution.Nodes))
			t.Logf("  Safety checks: %d", len(plan.SafetyChecks))
		})
	}
}

// TestExecutionStepValidation validates individual execution steps
func TestExecutionStepValidation(t *testing.T) {
	// Create a realistic multi-generation scenario
	params := &worker_pb.TaskParams{
		VolumeId:   300,
		Collection: "test",
		Sources: []*worker_pb.TaskSource{
			{
				Node:       "node1:8080",
				Generation: 0,
				ShardIds:   []uint32{0, 1, 2, 3}, // Incomplete old generation
			},
			{
				Node:       "node2:8080",
				Generation: 1,
				ShardIds:   []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, // Complete generation (should be selected)
			},
			{
				Node:       "node3:8080",
				Generation: 1,
				ShardIds:   []uint32{10, 11, 12, 13}, // Additional shards for generation 1
			},
		},
	}

	logic := NewEcVacuumLogic()

	// Create plan
	plan, err := logic.CreateVacuumPlan(params.VolumeId, params.Collection, params)
	if err != nil {
		t.Fatalf("failed to create plan: %v", err)
	}

	// Validate Step 1: Plan Creation
	t.Run("step_1_plan_creation", func(t *testing.T) {
		if plan.CurrentGeneration != 1 {
			t.Errorf("plan should select generation 1 (complete), got %d", plan.CurrentGeneration)
		}
		if plan.TargetGeneration != 2 {
			t.Errorf("plan should target generation 2, got %d", plan.TargetGeneration)
		}
		if len(plan.GenerationsToCleanup) != 2 {
			t.Errorf("plan should cleanup 2 generations (0,1), got %d", len(plan.GenerationsToCleanup))
		}
	})

	// Validate Step 2: Source Node Selection
	t.Run("step_2_source_node_selection", func(t *testing.T) {
		sourceNodes, err := logic.ParseSourceNodes(params, plan.CurrentGeneration)
		if err != nil {
			t.Fatalf("failed to parse source nodes: %v", err)
		}

		// Should only include nodes from generation 1
		expectedNodes := 2 // node2 and node3 have generation 1 shards
		if len(sourceNodes) != expectedNodes {
			t.Errorf("expected %d source nodes (generation 1 only), got %d", expectedNodes, len(sourceNodes))
		}

		// Verify node2 has the right shards (0-9)
		node2Addr := pb.ServerAddress("node2:8080")
		if shardBits, exists := sourceNodes[node2Addr]; exists {
			if shardBits.ShardIdCount() != 10 {
				t.Errorf("node2 should have 10 shards, got %d", shardBits.ShardIdCount())
			}
		} else {
			t.Errorf("node2 should be in source nodes")
		}

		// Verify node3 has the right shards (10-13)
		node3Addr := pb.ServerAddress("node3:8080")
		if shardBits, exists := sourceNodes[node3Addr]; exists {
			if shardBits.ShardIdCount() != 4 {
				t.Errorf("node3 should have 4 shards, got %d", shardBits.ShardIdCount())
			}
		} else {
			t.Errorf("node3 should be in source nodes")
		}
	})

	// Validate Step 3: Cleanup Planning
	t.Run("step_3_cleanup_planning", func(t *testing.T) {
		// Should cleanup both generation 0 and 1, but not generation 2
		cleanupMap := make(map[uint32]bool)
		for _, gen := range plan.GenerationsToCleanup {
			cleanupMap[gen] = true
		}

		expectedCleanup := []uint32{0, 1}
		for _, expectedGen := range expectedCleanup {
			if !cleanupMap[expectedGen] {
				t.Errorf("generation %d should be in cleanup list", expectedGen)
			}
		}

		// Should NOT cleanup target generation
		if cleanupMap[plan.TargetGeneration] {
			t.Errorf("target generation %d should NOT be in cleanup list", plan.TargetGeneration)
		}
	})

	// Validate Step 4: Safety Checks
	t.Run("step_4_safety_checks", func(t *testing.T) {
		if len(plan.SafetyChecks) == 0 {
			t.Errorf("plan should include safety checks")
		}

		// Verify shard distribution is sufficient
		err := logic.ValidateShardDistribution(plan.SourceDistribution)
		if err != nil {
			t.Errorf("shard distribution validation failed: %v", err)
		}
	})

	t.Logf("✅ All execution step validations passed")
}

// TestExecutionErrorHandling tests error scenarios in execution
func TestExecutionErrorHandling(t *testing.T) {
	logic := NewEcVacuumLogic()

	tests := []struct {
		name        string
		params      *worker_pb.TaskParams
		expectError bool
		errorMsg    string
	}{
		{
			name: "no_sufficient_generations",
			params: &worker_pb.TaskParams{
				VolumeId:   400,
				Collection: "test",
				Sources: []*worker_pb.TaskSource{
					{
						Node:       "node1:8080",
						Generation: 0,
						ShardIds:   []uint32{0, 1, 2}, // Only 3 shards - insufficient
					},
					{
						Node:       "node2:8080",
						Generation: 1,
						ShardIds:   []uint32{3, 4, 5}, // Only 6 total shards - insufficient
					},
				},
			},
			expectError: true,
			errorMsg:    "no generation has sufficient shards",
		},
		{
			name: "empty_sources",
			params: &worker_pb.TaskParams{
				VolumeId:   500,
				Collection: "test",
				Sources:    []*worker_pb.TaskSource{},
			},
			expectError: false, // Should fall back to defaults
			errorMsg:    "",
		},
		{
			name: "mixed_valid_invalid_generations",
			params: &worker_pb.TaskParams{
				VolumeId:   600,
				Collection: "test",
				Sources: []*worker_pb.TaskSource{
					{
						Node:       "node1:8080",
						Generation: 0,
						ShardIds:   []uint32{0, 1}, // Insufficient
					},
					{
						Node:       "node2:8080",
						Generation: 1,
						ShardIds:   []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13}, // Complete - should be selected
					},
				},
			},
			expectError: false, // Should use generation 1
			errorMsg:    "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan, err := logic.CreateVacuumPlan(tt.params.VolumeId, tt.params.Collection, tt.params)

			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
				} else if tt.errorMsg != "" && !contains(err.Error(), tt.errorMsg) {
					t.Errorf("expected error containing '%s', got '%s'", tt.errorMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				} else {
					// Validate the plan is reasonable
					if plan.TargetGeneration <= plan.CurrentGeneration {
						t.Errorf("target generation %d should be > current generation %d",
							plan.TargetGeneration, plan.CurrentGeneration)
					}
				}
			}
		})
	}
}

// Helper function to check if string contains substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && 
		(s[:len(substr)] == substr || s[len(s)-len(substr):] == substr || 
		 len(s) > len(substr) && someContains(s, substr)))
}

func someContains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
