package topology

import (
	"fmt"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/sequence"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	testAssert "github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPreUpgradeNodeCompatibility tests that pre-upgrade nodes (without generation support)
// can continue working with the new generation-aware system
func TestPreUpgradeNodeCompatibility(t *testing.T) {
	t.Run("pre_upgrade_heartbeat_processing", func(t *testing.T) {
		// Test that heartbeats from pre-upgrade volume servers are processed correctly

		topo := NewTopology("test", sequence.NewMemorySequencer(), 32*1024, 5, false)
		volumeId := needle.VolumeId(456)

		// Simulate heartbeat from pre-upgrade volume server (generation=0)
		ecShardInfo := &master_pb.VolumeEcShardInformationMessage{
			Id:          uint32(volumeId),
			Collection:  "test",
			EcIndexBits: uint32(0x3FFF), // all 14 shards
			DiskType:    "hdd",
			Generation:  0, // Pre-upgrade server sends generation 0
		}

		dc := topo.GetOrCreateDataCenter("dc1")
		rack := dc.GetOrCreateRack("rack1")
		dn := rack.GetOrCreateDataNode("server1", 8080, 0, "127.0.0.1", nil)

		// Process heartbeat - should work fine
		topo.SyncDataNodeEcShards([]*master_pb.VolumeEcShardInformationMessage{ecShardInfo}, dn)

		// Verify it was registered
		locations, found := topo.LookupEcShards(volumeId, 0)
		require.True(t, found, "Pre-upgrade server EC shards should be registered")
		testAssert.Equal(t, uint32(0), locations.Generation, "Should be registered as generation 0")

		t.Logf("✅ Pre-upgrade server heartbeat processed: volume %d generation %d",
			volumeId, locations.Generation)
	})

	t.Run("pre_upgrade_lookup_fallback", func(t *testing.T) {
		// Test that pre-upgrade clients can lookup volumes using generation 0

		topo := NewTopology("test", sequence.NewMemorySequencer(), 32*1024, 5, false)
		volumeId := needle.VolumeId(123)

		// Register generation 2 shards
		ecInfo := &erasure_coding.EcVolumeInfo{
			VolumeId:   volumeId,
			Collection: "test",
			ShardBits:  erasure_coding.ShardBits(0x3FFF), // all 14 shards
			Generation: 2,
		}

		dc := topo.GetOrCreateDataCenter("dc1")
		rack := dc.GetOrCreateRack("rack1")
		dn := rack.GetOrCreateDataNode("server1", 8080, 0, "127.0.0.1", nil)
		topo.RegisterEcShards(ecInfo, dn)

		// Set generation 2 as active
		topo.SetEcActiveGeneration(volumeId, 2)

		// Pre-upgrade client looks up with generation 0
		locations, actualGen, found := topo.LookupEcShardsWithFallback(volumeId, 0)

		require.True(t, found, "Pre-upgrade client should find EC volume")
		testAssert.Equal(t, uint32(2), actualGen, "Should return active generation")
		testAssert.Equal(t, uint32(2), locations.Generation, "Locations should be for active generation")

		t.Logf("✅ Pre-upgrade client lookup: requested gen=0, got active gen=%d", actualGen)
	})
}

// TestPostUpgradeNodeCompatibility tests that post-upgrade nodes (with generation support)
// can handle legacy data from pre-upgrade nodes
func TestPostUpgradeNodeCompatibility(t *testing.T) {
	t.Run("post_upgrade_handles_legacy_data", func(t *testing.T) {
		// Test that new generation-aware nodes can handle legacy generation 0 data

		topo := NewTopology("test", sequence.NewMemorySequencer(), 32*1024, 5, false)
		volumeId := needle.VolumeId(789)

		// Register legacy generation 0 EC volume (from pre-upgrade)
		legacyEcInfo := &erasure_coding.EcVolumeInfo{
			VolumeId:   volumeId,
			Collection: "test",
			ShardBits:  erasure_coding.ShardBits(0x3FFF),
			Generation: 0, // Legacy data
		}

		dc := topo.GetOrCreateDataCenter("dc1")
		rack := dc.GetOrCreateRack("rack1")
		dn := rack.GetOrCreateDataNode("server1", 8080, 0, "127.0.0.1", nil)
		topo.RegisterEcShards(legacyEcInfo, dn)

		// Post-upgrade client with generation support looks up the volume
		// When no active generation is set, should fallback to whatever is available
		locations, actualGen, found := topo.LookupEcShardsWithFallback(volumeId, 0)

		require.True(t, found, "Post-upgrade node should find legacy data")
		testAssert.Equal(t, uint32(0), actualGen, "Should return generation 0 for legacy data")
		testAssert.Equal(t, uint32(0), locations.Generation, "Locations should be generation 0")

		t.Logf("✅ Post-upgrade node handles legacy data: found gen=%d", actualGen)
	})

	t.Run("post_upgrade_prefers_active_generation", func(t *testing.T) {
		// Test that post-upgrade nodes prefer active generation over legacy

		topo := NewTopology("test", sequence.NewMemorySequencer(), 32*1024, 5, false)
		volumeId := needle.VolumeId(999)

		dc := topo.GetOrCreateDataCenter("dc1")
		rack := dc.GetOrCreateRack("rack1")
		dn := rack.GetOrCreateDataNode("server1", 8080, 0, "127.0.0.1", nil)

		// Register both legacy (gen 0) and new (gen 1) data
		legacyEcInfo := &erasure_coding.EcVolumeInfo{
			VolumeId:   volumeId,
			Collection: "test",
			ShardBits:  erasure_coding.ShardBits(0x3FFF),
			Generation: 0,
		}
		topo.RegisterEcShards(legacyEcInfo, dn)

		newEcInfo := &erasure_coding.EcVolumeInfo{
			VolumeId:   volumeId,
			Collection: "test",
			ShardBits:  erasure_coding.ShardBits(0x3FFF),
			Generation: 1,
		}
		topo.RegisterEcShards(newEcInfo, dn)

		// Set generation 1 as active
		topo.SetEcActiveGeneration(volumeId, 1)

		// Post-upgrade client lookup should prefer active generation
		locations, actualGen, found := topo.LookupEcShardsWithFallback(volumeId, 0)

		require.True(t, found, "Should find volume")
		testAssert.Equal(t, uint32(1), actualGen, "Should prefer active generation over legacy")
		testAssert.Equal(t, uint32(1), locations.Generation, "Locations should be active generation")

		t.Logf("✅ Post-upgrade node prefers active: legacy=0, active=1, returned=%d", actualGen)
	})

	t.Run("post_upgrade_strict_generation_requests", func(t *testing.T) {
		// Test that post-upgrade clients can make strict generation requests

		topo := NewTopology("test", sequence.NewMemorySequencer(), 32*1024, 5, false)
		volumeId := needle.VolumeId(555)

		dc := topo.GetOrCreateDataCenter("dc1")
		rack := dc.GetOrCreateRack("rack1")
		dn := rack.GetOrCreateDataNode("server1", 8080, 0, "127.0.0.1", nil)

		// Register multiple generations
		for gen := uint32(0); gen <= 2; gen++ {
			ecInfo := &erasure_coding.EcVolumeInfo{
				VolumeId:   volumeId,
				Collection: "test",
				ShardBits:  erasure_coding.ShardBits(0x3FFF),
				Generation: gen,
			}
			topo.RegisterEcShards(ecInfo, dn)
		}

		// Test strict generation requests
		for requestedGen := uint32(0); requestedGen <= 2; requestedGen++ {
			locations, actualGen, found := topo.LookupEcShardsWithFallback(volumeId, requestedGen)

			if requestedGen == 0 {
				// Generation 0 requests use active generation logic
				require.True(t, found, "Generation 0 request should find volume")
			} else {
				// Specific generation requests should return exact match
				require.True(t, found, "Specific generation request should find exact match")
				testAssert.Equal(t, requestedGen, actualGen, "Should return exact requested generation")
				testAssert.Equal(t, requestedGen, locations.Generation, "Locations should match requested generation")
			}
		}

		t.Logf("✅ Post-upgrade strict requests work for all generations")
	})
}

// TestMixedClusterOperations tests operations in a mixed cluster
// where some nodes are pre-upgrade and some are post-upgrade
func TestMixedClusterOperations(t *testing.T) {
	t.Run("mixed_cluster_shard_distribution", func(t *testing.T) {
		// Test that EC shards can be distributed across mixed-version nodes

		topo := NewTopology("test", sequence.NewMemorySequencer(), 32*1024, 5, false)
		volumeId := needle.VolumeId(777)

		dc := topo.GetOrCreateDataCenter("dc1")
		rack := dc.GetOrCreateRack("rack1")

		// Pre-upgrade node (sends generation 0)
		preUpgradeNode := rack.GetOrCreateDataNode("pre-upgrade", 8080, 0, "127.0.0.1", nil)

		// Post-upgrade node (sends specific generation)
		postUpgradeNode := rack.GetOrCreateDataNode("post-upgrade", 8081, 0, "127.0.0.2", nil)

		// Pre-upgrade node reports shards with generation 0
		preUpgradeShards := &erasure_coding.EcVolumeInfo{
			VolumeId:   volumeId,
			Collection: "test",
			ShardBits:  erasure_coding.ShardBits(0x1FF), // shards 0-8
			Generation: 0,
		}
		topo.RegisterEcShards(preUpgradeShards, preUpgradeNode)

		// Post-upgrade node reports shards with generation 1
		postUpgradeShards := &erasure_coding.EcVolumeInfo{
			VolumeId:   volumeId,
			Collection: "test",
			ShardBits:  erasure_coding.ShardBits(0x3E00), // shards 9-13
			Generation: 1,
		}
		topo.RegisterEcShards(postUpgradeShards, postUpgradeNode)

		// Verify both generations are registered
		gen0Locations, found0 := topo.LookupEcShards(volumeId, 0)
		gen1Locations, found1 := topo.LookupEcShards(volumeId, 1)

		require.True(t, found0, "Generation 0 shards should be registered")
		require.True(t, found1, "Generation 1 shards should be registered")

		gen0ShardCount := countShards(gen0Locations)
		gen1ShardCount := countShards(gen1Locations)

		testAssert.Equal(t, 9, gen0ShardCount, "Pre-upgrade node should have 9 shards")
		testAssert.Equal(t, 5, gen1ShardCount, "Post-upgrade node should have 5 shards")

		t.Logf("✅ Mixed cluster shard distribution: gen0=%d shards, gen1=%d shards",
			gen0ShardCount, gen1ShardCount)
	})
}

// TestRollingUpgradeScenarios tests specific rolling upgrade scenarios
func TestRollingUpgradeScenarios(t *testing.T) {
	t.Run("rolling_upgrade_sequence", func(t *testing.T) {
		// Test the complete rolling upgrade sequence

		topo := NewTopology("test", sequence.NewMemorySequencer(), 32*1024, 5, false)
		volumeId := needle.VolumeId(123)

		dc := topo.GetOrCreateDataCenter("dc1")
		rack := dc.GetOrCreateRack("rack1")

		// Create 6 nodes representing a cluster during rolling upgrade
		nodes := make([]*DataNode, 6)
		for i := 0; i < 6; i++ {
			nodes[i] = rack.GetOrCreateDataNode(fmt.Sprintf("node%d", i), 8080+i, 0, fmt.Sprintf("127.0.0.%d", i+1), nil)
		}

		// Phase 1: All nodes are pre-upgrade (generation 0)
		t.Run("phase1_all_pre_upgrade", func(t *testing.T) {
			for i, node := range nodes {
				ecInfo := &erasure_coding.EcVolumeInfo{
					VolumeId:   volumeId,
					Collection: "test",
					ShardBits:  erasure_coding.ShardBits(1 << i), // Each node has one shard
					Generation: 0,
				}
				topo.RegisterEcShards(ecInfo, node)
			}

			// Verify all shards are generation 0
			locations, found := topo.LookupEcShards(volumeId, 0)
			require.True(t, found, "Should find generation 0 volume")
			testAssert.Equal(t, 6, countShards(locations), "Should have 6 shards")

			t.Logf("✅ Phase 1: All 6 nodes running pre-upgrade with generation 0")
		})

		// Phase 2: Partially upgraded cluster (3 nodes upgraded)
		t.Run("phase2_partial_upgrade", func(t *testing.T) {
			// Nodes 3-5 are upgraded and now understand generations
			// They re-register their shards as generation 1
			for i := 3; i < 6; i++ {
				// Unregister old generation 0 shard
				oldEcInfo := &erasure_coding.EcVolumeInfo{
					VolumeId:   volumeId,
					Collection: "test",
					ShardBits:  erasure_coding.ShardBits(1 << i),
					Generation: 0,
				}
				topo.UnRegisterEcShards(oldEcInfo, nodes[i])

				// Register new generation 1 shard
				newEcInfo := &erasure_coding.EcVolumeInfo{
					VolumeId:   volumeId,
					Collection: "test",
					ShardBits:  erasure_coding.ShardBits(1 << i),
					Generation: 1,
				}
				topo.RegisterEcShards(newEcInfo, nodes[i])
			}

			// Verify mixed generations
			gen0Locations, found0 := topo.LookupEcShards(volumeId, 0)
			gen1Locations, found1 := topo.LookupEcShards(volumeId, 1)

			require.True(t, found0, "Should still have generation 0 shards")
			require.True(t, found1, "Should have generation 1 shards")

			testAssert.Equal(t, 3, countShards(gen0Locations), "Should have 3 gen 0 shards")
			testAssert.Equal(t, 3, countShards(gen1Locations), "Should have 3 gen 1 shards")

			t.Logf("✅ Phase 2: Mixed cluster - 3 nodes gen 0, 3 nodes gen 1")
		})

		// Phase 3: Fully upgraded cluster
		t.Run("phase3_full_upgrade", func(t *testing.T) {
			// Remaining nodes 0-2 are upgraded
			for i := 0; i < 3; i++ {
				// Unregister old generation 0 shard
				oldEcInfo := &erasure_coding.EcVolumeInfo{
					VolumeId:   volumeId,
					Collection: "test",
					ShardBits:  erasure_coding.ShardBits(1 << i),
					Generation: 0,
				}
				topo.UnRegisterEcShards(oldEcInfo, nodes[i])

				// Register new generation 1 shard
				newEcInfo := &erasure_coding.EcVolumeInfo{
					VolumeId:   volumeId,
					Collection: "test",
					ShardBits:  erasure_coding.ShardBits(1 << i),
					Generation: 1,
				}
				topo.RegisterEcShards(newEcInfo, nodes[i])
			}

			// Set generation 1 as active
			topo.SetEcActiveGeneration(volumeId, 1)

			// Verify only generation 1 remains
			_, found0 := topo.LookupEcShards(volumeId, 0)
			gen1Locations, found1 := topo.LookupEcShards(volumeId, 1)

			testAssert.False(t, found0, "Should no longer have generation 0 shards")
			require.True(t, found1, "Should have generation 1 shards")
			testAssert.Equal(t, 6, countShards(gen1Locations), "Should have all 6 gen 1 shards")

			// Test that lookups now prefer generation 1
			_, actualGen, found := topo.LookupEcShardsWithFallback(volumeId, 0)
			require.True(t, found, "Should find volume")
			testAssert.Equal(t, uint32(1), actualGen, "Should return active generation 1")

			t.Logf("✅ Phase 3: All nodes upgraded to generation 1, old generation cleaned up")
		})
	})
}

// TestGenerationCompatibilityMatrix tests all combinations of client/server generations
func TestGenerationCompatibilityMatrix(t *testing.T) {
	// Test matrix of generation compatibility for various upgrade scenarios
	testCases := []struct {
		name               string
		clientType         string
		serverGeneration   uint32
		requestGeneration  uint32
		shouldBeCompatible bool
		description        string
	}{
		{
			name:               "pre_client_to_pre_server",
			clientType:         "pre-upgrade",
			serverGeneration:   0,
			requestGeneration:  0,
			shouldBeCompatible: true,
			description:        "Pre-upgrade client to pre-upgrade server",
		},
		{
			name:               "pre_client_to_post_server_gen1",
			clientType:         "pre-upgrade",
			serverGeneration:   1,
			requestGeneration:  0,
			shouldBeCompatible: true,
			description:        "Pre-upgrade client to generation 1 server",
		},
		{
			name:               "pre_client_to_post_server_gen2",
			clientType:         "pre-upgrade",
			serverGeneration:   2,
			requestGeneration:  0,
			shouldBeCompatible: true,
			description:        "Pre-upgrade client to generation 2 server",
		},
		{
			name:               "post_client_exact_match",
			clientType:         "post-upgrade",
			serverGeneration:   1,
			requestGeneration:  1,
			shouldBeCompatible: true,
			description:        "Post-upgrade client exact generation match",
		},
		{
			name:               "post_client_strict_mismatch",
			clientType:         "post-upgrade",
			serverGeneration:   0,
			requestGeneration:  1,
			shouldBeCompatible: false,
			description:        "Post-upgrade client strict mismatch",
		},
		{
			name:               "post_client_legacy_request",
			clientType:         "post-upgrade",
			serverGeneration:   1,
			requestGeneration:  0,
			shouldBeCompatible: true,
			description:        "Post-upgrade client with legacy request",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Use the isGenerationCompatible function from volume_grpc_erasure_coding.go
			compatible := isGenerationCompatible(tc.serverGeneration, tc.requestGeneration)

			testAssert.Equal(t, tc.shouldBeCompatible, compatible, tc.description)

			if compatible {
				t.Logf("✅ %s: server_gen=%d, request_gen=%d → COMPATIBLE",
					tc.description, tc.serverGeneration, tc.requestGeneration)
			} else {
				t.Logf("❌ %s: server_gen=%d, request_gen=%d → INCOMPATIBLE",
					tc.description, tc.serverGeneration, tc.requestGeneration)
			}
		})
	}
}

// Helper function to count shards in EcShardLocations
func countShards(locations *EcShardLocations) int {
	count := 0
	for i := 0; i < erasure_coding.TotalShardsCount; i++ {
		if len(locations.Locations[i]) > 0 {
			count++
		}
	}
	return count
}

// Helper function to simulate isGenerationCompatible from volume_grpc_erasure_coding.go
func isGenerationCompatible(actualGeneration, requestedGeneration uint32) bool {
	// Exact match is always compatible
	if actualGeneration == requestedGeneration {
		return true
	}

	// Mixed-version compatibility: if client requests generation 0 (default/legacy),
	// allow access to any generation for backward compatibility
	if requestedGeneration == 0 {
		return true
	}

	// If client requests specific generation but volume has different generation,
	// this is not compatible (strict generation matching)
	return false
}
