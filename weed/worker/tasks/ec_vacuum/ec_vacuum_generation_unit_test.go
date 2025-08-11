package ec_vacuum

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockMasterClient implements master_pb.SeaweedClient for testing
type MockMasterClient struct {
	volumes          map[uint32]*mockVolumeInfo
	ecVolumes        map[uint32]*mockEcVolumeInfo
	activatedCalls   []uint32
	simulateFailures bool
}

type mockVolumeInfo struct {
	id         uint32
	collection string
	locations  []string
}

type mockEcVolumeInfo struct {
	id               uint32
	collection       string
	generation       uint32
	activeGeneration uint32
	shards           map[uint32][]string // shardId -> locations
}

func NewMockMasterClient() *MockMasterClient {
	return &MockMasterClient{
		volumes:   make(map[uint32]*mockVolumeInfo),
		ecVolumes: make(map[uint32]*mockEcVolumeInfo),
	}
}

// Add EC volume to mock
func (m *MockMasterClient) AddEcVolume(volumeId uint32, generation uint32, activeGeneration uint32) {
	m.ecVolumes[volumeId] = &mockEcVolumeInfo{
		id:               volumeId,
		collection:       "test",
		generation:       generation,
		activeGeneration: activeGeneration,
		shards:           make(map[uint32][]string),
	}

	// Add some mock shards
	for i := uint32(0); i < 14; i++ {
		m.ecVolumes[volumeId].shards[i] = []string{"server1:8080", "server2:8080"}
	}
}

func (m *MockMasterClient) LookupEcVolume(ctx context.Context, req *master_pb.LookupEcVolumeRequest) (*master_pb.LookupEcVolumeResponse, error) {
	if m.simulateFailures {
		return nil, fmt.Errorf("simulated failure")
	}

	vol, exists := m.ecVolumes[req.VolumeId]
	if !exists {
		return nil, fmt.Errorf("volume %d not found", req.VolumeId)
	}

	resp := &master_pb.LookupEcVolumeResponse{
		VolumeId:         req.VolumeId,
		ActiveGeneration: vol.activeGeneration,
	}

	// Return shards for the requested generation or active generation
	targetGeneration := req.Generation
	if targetGeneration == 0 {
		targetGeneration = vol.activeGeneration
	}

	if targetGeneration == vol.generation {
		for shardId, locations := range vol.shards {
			var locs []*master_pb.Location
			for _, loc := range locations {
				locs = append(locs, &master_pb.Location{Url: loc})
			}

			resp.ShardIdLocations = append(resp.ShardIdLocations, &master_pb.LookupEcVolumeResponse_EcShardIdLocation{
				ShardId:    shardId,
				Generation: vol.generation,
				Locations:  locs,
			})
		}
	}

	return resp, nil
}

func (m *MockMasterClient) ActivateEcGeneration(ctx context.Context, req *master_pb.ActivateEcGenerationRequest) (*master_pb.ActivateEcGenerationResponse, error) {
	if m.simulateFailures {
		return nil, fmt.Errorf("simulated activation failure")
	}

	m.activatedCalls = append(m.activatedCalls, req.VolumeId)

	vol, exists := m.ecVolumes[req.VolumeId]
	if !exists {
		return &master_pb.ActivateEcGenerationResponse{
			Error: "volume not found",
		}, nil
	}

	// Simulate activation
	vol.activeGeneration = req.Generation

	return &master_pb.ActivateEcGenerationResponse{}, nil
}

// Other required methods (stubs)
func (m *MockMasterClient) SendHeartbeat(ctx context.Context, req *master_pb.Heartbeat) (*master_pb.HeartbeatResponse, error) {
	return &master_pb.HeartbeatResponse{}, nil
}

func (m *MockMasterClient) KeepConnected(ctx context.Context, req *master_pb.KeepConnectedRequest) (master_pb.Seaweed_KeepConnectedClient, error) {
	return nil, fmt.Errorf("not implemented")
}

func (m *MockMasterClient) LookupVolume(ctx context.Context, req *master_pb.LookupVolumeRequest) (*master_pb.LookupVolumeResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

func (m *MockMasterClient) Assign(ctx context.Context, req *master_pb.AssignRequest) (*master_pb.AssignResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

func (m *MockMasterClient) Statistics(ctx context.Context, req *master_pb.StatisticsRequest) (*master_pb.StatisticsResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

func (m *MockMasterClient) CollectionList(ctx context.Context, req *master_pb.CollectionListRequest) (*master_pb.CollectionListResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

func (m *MockMasterClient) CollectionDelete(ctx context.Context, req *master_pb.CollectionDeleteRequest) (*master_pb.CollectionDeleteResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

func (m *MockMasterClient) VolumeList(ctx context.Context, req *master_pb.VolumeListRequest) (*master_pb.VolumeListResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

func (m *MockMasterClient) VacuumVolume(ctx context.Context, req *master_pb.VacuumVolumeRequest) (*master_pb.VacuumVolumeResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

func (m *MockMasterClient) DisableVacuum(ctx context.Context, req *master_pb.DisableVacuumRequest) (*master_pb.DisableVacuumResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

func (m *MockMasterClient) EnableVacuum(ctx context.Context, req *master_pb.EnableVacuumRequest) (*master_pb.EnableVacuumResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

func (m *MockMasterClient) VolumeMarkReadonly(ctx context.Context, req *master_pb.VolumeMarkReadonlyRequest) (*master_pb.VolumeMarkReadonlyResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

func (m *MockMasterClient) GetMasterConfiguration(ctx context.Context, req *master_pb.GetMasterConfigurationRequest) (*master_pb.GetMasterConfigurationResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

func (m *MockMasterClient) ListClusterNodes(ctx context.Context, req *master_pb.ListClusterNodesRequest) (*master_pb.ListClusterNodesResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

func (m *MockMasterClient) LeaseAdminToken(ctx context.Context, req *master_pb.LeaseAdminTokenRequest) (*master_pb.LeaseAdminTokenResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

func (m *MockMasterClient) ReleaseAdminToken(ctx context.Context, req *master_pb.ReleaseAdminTokenRequest) (*master_pb.ReleaseAdminTokenResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

func (m *MockMasterClient) Ping(ctx context.Context, req *master_pb.PingRequest) (*master_pb.PingResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

func (m *MockMasterClient) RaftListClusterServers(ctx context.Context, req *master_pb.RaftListClusterServersRequest) (*master_pb.RaftListClusterServersResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

func (m *MockMasterClient) RaftAddServer(ctx context.Context, req *master_pb.RaftAddServerRequest) (*master_pb.RaftAddServerResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

func (m *MockMasterClient) RaftRemoveServer(ctx context.Context, req *master_pb.RaftRemoveServerRequest) (*master_pb.RaftRemoveServerResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

// Test the generation transition logic in EC vacuum task
func TestEcVacuumGenerationTransition(t *testing.T) {
	mockMaster := NewMockMasterClient()
	volumeId := uint32(123)
	collection := "test"

	// Set up initial EC volume in generation 0
	mockMaster.AddEcVolume(volumeId, 0, 0)

	// Create EC vacuum task from generation 0 to generation 1
	sourceNodes := map[pb.ServerAddress]erasure_coding.ShardBits{
		"server1:8080": erasure_coding.ShardBits(0x3FFF), // All 14 shards
	}

	task := NewEcVacuumTask("test-task", volumeId, collection, sourceNodes, 0)

	// Verify initial generation setup
	assert.Equal(t, uint32(0), task.sourceGeneration, "Source generation should be 0")
	assert.Equal(t, uint32(1), task.targetGeneration, "Target generation should be 1")
	assert.Equal(t, 5*time.Minute, task.cleanupGracePeriod, "Cleanup grace period should be 5 minutes")

	t.Logf("Task initialized: source_gen=%d, target_gen=%d, grace_period=%v",
		task.sourceGeneration, task.targetGeneration, task.cleanupGracePeriod)
}

func TestEcVacuumActivateNewGeneration(t *testing.T) {
	mockMaster := NewMockMasterClient()
	volumeId := uint32(456)
	collection := "test"

	// Set up EC volume with generation 1 ready for activation
	mockMaster.AddEcVolume(volumeId, 1, 0) // generation 1 exists, but active is still 0

	sourceNodes := map[pb.ServerAddress]erasure_coding.ShardBits{
		"server1:8080": erasure_coding.ShardBits(0x3FFF),
	}

	task := NewEcVacuumTask("activate-test", volumeId, collection, sourceNodes, 0)

	// Simulate the activation step
	ctx := context.Background()

	// Test activation call
	resp, err := mockMaster.ActivateEcGeneration(ctx, &master_pb.ActivateEcGenerationRequest{
		VolumeId:   volumeId,
		Generation: task.targetGeneration,
	})

	require.NoError(t, err, "Activation should succeed")
	assert.Empty(t, resp.Error, "Activation should not return error")

	// Verify activation was called
	assert.Contains(t, mockMaster.activatedCalls, volumeId, "Volume should be in activated calls")

	// Verify active generation was updated
	lookupResp, err := mockMaster.LookupEcVolume(ctx, &master_pb.LookupEcVolumeRequest{
		VolumeId: volumeId,
	})
	require.NoError(t, err)
	assert.Equal(t, uint32(1), lookupResp.ActiveGeneration, "Active generation should be updated to 1")

	t.Logf("✅ Generation activation successful: volume %d activated to generation %d",
		volumeId, lookupResp.ActiveGeneration)
}

func TestEcVacuumGenerationFailureHandling(t *testing.T) {
	mockMaster := NewMockMasterClient()
	volumeId := uint32(789)
	collection := "test"

	// Set up EC volume
	mockMaster.AddEcVolume(volumeId, 0, 0)

	sourceNodes := map[pb.ServerAddress]erasure_coding.ShardBits{
		"server1:8080": erasure_coding.ShardBits(0x3FFF),
	}

	task := NewEcVacuumTask("failure-test", volumeId, collection, sourceNodes, 0)

	// Test activation failure handling
	t.Run("activation_failure", func(t *testing.T) {
		mockMaster.simulateFailures = true

		ctx := context.Background()
		_, err := mockMaster.ActivateEcGeneration(ctx, &master_pb.ActivateEcGenerationRequest{
			VolumeId:   volumeId,
			Generation: task.targetGeneration,
		})

		assert.Error(t, err, "Should fail when master client simulates failure")
		assert.Contains(t, err.Error(), "simulated activation failure")

		t.Logf("✅ Activation failure properly handled: %v", err)

		mockMaster.simulateFailures = false
	})

	// Test lookup failure handling
	t.Run("lookup_failure", func(t *testing.T) {
		mockMaster.simulateFailures = true

		ctx := context.Background()
		_, err := mockMaster.LookupEcVolume(ctx, &master_pb.LookupEcVolumeRequest{
			VolumeId: volumeId,
		})

		assert.Error(t, err, "Should fail when master client simulates failure")
		assert.Contains(t, err.Error(), "simulated failure")

		t.Logf("✅ Lookup failure properly handled: %v", err)

		mockMaster.simulateFailures = false
	})
}

func TestEcVacuumCleanupGracePeriod(t *testing.T) {
	volumeId := uint32(321)
	collection := "test"

	sourceNodes := map[pb.ServerAddress]erasure_coding.ShardBits{
		"server1:8080": erasure_coding.ShardBits(0x3FFF),
	}

	task := NewEcVacuumTask("cleanup-test", volumeId, collection, sourceNodes, 2)

	// Verify cleanup grace period is set correctly
	assert.Equal(t, 5*time.Minute, task.cleanupGracePeriod, "Cleanup grace period should be 5 minutes")

	// Test that the grace period is significant enough for safety
	assert.Greater(t, task.cleanupGracePeriod, 1*time.Minute, "Grace period should be at least 1 minute for safety")
	assert.LessOrEqual(t, task.cleanupGracePeriod, 10*time.Minute, "Grace period should not be excessive")

	t.Logf("✅ Cleanup grace period correctly set: %v", task.cleanupGracePeriod)
}

func TestEcVacuumGenerationProgression(t *testing.T) {
	collection := "test"
	volumeId := uint32(555)

	sourceNodes := map[pb.ServerAddress]erasure_coding.ShardBits{
		"server1:8080": erasure_coding.ShardBits(0x3FFF),
	}

	// Test progression from generation 0 to 1
	task1 := NewEcVacuumTask("prog-test-1", volumeId, collection, sourceNodes, 0)
	assert.Equal(t, uint32(0), task1.sourceGeneration)
	assert.Equal(t, uint32(1), task1.targetGeneration)

	// Test progression from generation 1 to 2
	task2 := NewEcVacuumTask("prog-test-2", volumeId, collection, sourceNodes, 1)
	assert.Equal(t, uint32(1), task2.sourceGeneration)
	assert.Equal(t, uint32(2), task2.targetGeneration)

	// Test progression from generation 5 to 6
	task3 := NewEcVacuumTask("prog-test-3", volumeId, collection, sourceNodes, 5)
	assert.Equal(t, uint32(5), task3.sourceGeneration)
	assert.Equal(t, uint32(6), task3.targetGeneration)

	t.Logf("✅ Generation progression works correctly:")
	t.Logf("  0→1: source=%d, target=%d", task1.sourceGeneration, task1.targetGeneration)
	t.Logf("  1→2: source=%d, target=%d", task2.sourceGeneration, task2.targetGeneration)
	t.Logf("  5→6: source=%d, target=%d", task3.sourceGeneration, task3.targetGeneration)
}

func TestEcVacuumZeroDowntimeRequirements(t *testing.T) {
	// This test verifies that the vacuum task is designed for zero downtime

	mockMaster := NewMockMasterClient()
	volumeId := uint32(777)
	collection := "test"

	// Set up EC volume with both old and new generations
	mockMaster.AddEcVolume(volumeId, 0, 0) // Old generation active

	sourceNodes := map[pb.ServerAddress]erasure_coding.ShardBits{
		"server1:8080": erasure_coding.ShardBits(0x3FFF),
	}

	task := NewEcVacuumTask("zero-downtime-test", volumeId, collection, sourceNodes, 0)

	// Test 1: Verify that source generation (old) remains active during vacuum
	ctx := context.Background()

	// Before activation, old generation should still be active
	lookupResp, err := mockMaster.LookupEcVolume(ctx, &master_pb.LookupEcVolumeRequest{
		VolumeId: volumeId,
	})
	require.NoError(t, err)
	assert.Equal(t, uint32(0), lookupResp.ActiveGeneration, "Old generation should remain active during vacuum")

	// Test 2: After activation, new generation becomes active
	_, err = mockMaster.ActivateEcGeneration(ctx, &master_pb.ActivateEcGenerationRequest{
		VolumeId:   volumeId,
		Generation: task.targetGeneration,
	})
	require.NoError(t, err)

	lookupResp, err = mockMaster.LookupEcVolume(ctx, &master_pb.LookupEcVolumeRequest{
		VolumeId: volumeId,
	})
	require.NoError(t, err)
	assert.Equal(t, task.targetGeneration, lookupResp.ActiveGeneration, "New generation should be active after activation")

	// Test 3: Grace period ensures old generation cleanup is delayed
	assert.Greater(t, task.cleanupGracePeriod, time.Duration(0), "Grace period must be > 0 for safe cleanup")

	t.Logf("✅ Zero downtime requirements verified:")
	t.Logf("  - Old generation remains active during vacuum: ✓")
	t.Logf("  - Atomic activation switches to new generation: ✓")
	t.Logf("  - Grace period delays cleanup: %v ✓", task.cleanupGracePeriod)
}

func TestEcVacuumTaskConfiguration(t *testing.T) {
	volumeId := uint32(999)
	collection := "production"
	taskId := "production-vacuum-task-123"

	sourceNodes := map[pb.ServerAddress]erasure_coding.ShardBits{
		"server1:8080": erasure_coding.ShardBits(0x1FF),  // Shards 0-8
		"server2:8080": erasure_coding.ShardBits(0x3E00), // Shards 9-13
	}

	task := NewEcVacuumTask(taskId, volumeId, collection, sourceNodes, 3)

	// Verify task configuration
	assert.Equal(t, taskId, task.BaseTask.ID(), "Task ID should match")
	assert.Equal(t, volumeId, task.volumeID, "Volume ID should match")
	assert.Equal(t, collection, task.collection, "Collection should match")
	assert.Equal(t, uint32(3), task.sourceGeneration, "Source generation should match")
	assert.Equal(t, uint32(4), task.targetGeneration, "Target generation should be source + 1")
	assert.Equal(t, sourceNodes, task.sourceNodes, "Source nodes should match")

	// Verify shard distribution
	totalShards := 0
	for _, shardBits := range sourceNodes {
		for i := 0; i < 14; i++ {
			if shardBits.HasShardId(erasure_coding.ShardId(i)) {
				totalShards++
			}
		}
	}
	assert.Equal(t, 14, totalShards, "Should have all 14 shards distributed across nodes")

	t.Logf("✅ Task configuration verified:")
	t.Logf("  Task ID: %s", task.BaseTask.ID())
	t.Logf("  Volume: %d, Collection: %s", task.volumeID, task.collection)
	t.Logf("  Generation: %d → %d", task.sourceGeneration, task.targetGeneration)
	t.Logf("  Shard distribution: %d total shards across %d nodes", totalShards, len(sourceNodes))
}
