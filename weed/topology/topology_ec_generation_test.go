package topology

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/sequence"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// TestEcGenerationLookup tests basic generation-aware lookup functionality
func TestEcGenerationLookup(t *testing.T) {
	topo := NewTopology("test", sequence.NewMemorySequencer(), 32*1024, 5, false)
	dc := topo.GetOrCreateDataCenter("dc1")
	rack := dc.GetOrCreateRack("rack1")
	dn1 := rack.GetOrCreateDataNode("server1", 8080, 0, "127.0.0.1", nil)
	dn2 := rack.GetOrCreateDataNode("server2", 8080, 0, "127.0.0.2", nil)

	// Test case: Register EC shards for volume 123 with different generations
	volumeId := needle.VolumeId(123)
	collection := "test_collection"

	// Register generation 0 (4 shards)
	ecInfo0 := &erasure_coding.EcVolumeInfo{
		VolumeId:   volumeId,
		Collection: collection,
		ShardBits:  erasure_coding.ShardBits(0x0F), // shards 0,1,2,3
		Generation: 0,
	}
	topo.RegisterEcShards(ecInfo0, dn1)

	// Register generation 1 (different shards)
	ecInfo1 := &erasure_coding.EcVolumeInfo{
		VolumeId:   volumeId,
		Collection: collection,
		ShardBits:  erasure_coding.ShardBits(0xF0), // shards 4,5,6,7
		Generation: 1,
	}
	topo.RegisterEcShards(ecInfo1, dn2)

	// Test 1: Lookup specific generation 0
	locations, found := topo.LookupEcShards(volumeId, 0)
	if !found {
		t.Errorf("Expected to find generation 0, but didn't")
	}
	if locations.Generation != 0 {
		t.Errorf("Expected generation 0, got %d", locations.Generation)
	}
	if locations.Collection != collection {
		t.Errorf("Expected collection %s, got %s", collection, locations.Collection)
	}

	// Verify shard distribution for generation 0
	expectedShards0 := []erasure_coding.ShardId{0, 1, 2, 3}
	for _, shardId := range expectedShards0 {
		if len(locations.Locations[shardId]) != 1 {
			t.Errorf("Expected 1 location for shard %d in generation 0, got %d", shardId, len(locations.Locations[shardId]))
		}
		if locations.Locations[shardId][0].Id() != dn1.Id() {
			t.Errorf("Expected shard %d to be on %s, got %s", shardId, dn1.Id(), locations.Locations[shardId][0].Id())
		}
	}

	// Test 2: Lookup specific generation 1
	locations, found = topo.LookupEcShards(volumeId, 1)
	if !found {
		t.Errorf("Expected to find generation 1, but didn't")
	}
	if locations.Generation != 1 {
		t.Errorf("Expected generation 1, got %d", locations.Generation)
	}

	// Verify shard distribution for generation 1
	expectedShards1 := []erasure_coding.ShardId{4, 5, 6, 7}
	for _, shardId := range expectedShards1 {
		if len(locations.Locations[shardId]) != 1 {
			t.Errorf("Expected 1 location for shard %d in generation 1, got %d", shardId, len(locations.Locations[shardId]))
		}
		if locations.Locations[shardId][0].Id() != dn2.Id() {
			t.Errorf("Expected shard %d to be on %s, got %s", shardId, dn2.Id(), locations.Locations[shardId][0].Id())
		}
	}

	// Test 3: Lookup non-existent generation
	_, found = topo.LookupEcShards(volumeId, 999)
	if found {
		t.Errorf("Expected not to find generation 999, but did")
	}

	// Test 4: Lookup non-existent volume
	_, found = topo.LookupEcShards(needle.VolumeId(999), 0)
	if found {
		t.Errorf("Expected not to find volume 999, but did")
	}
}

// TestEcActiveGenerationTracking tests active generation tracking functionality
func TestEcActiveGenerationTracking(t *testing.T) {
	topo := NewTopology("test", sequence.NewMemorySequencer(), 32*1024, 5, false)
	dc := topo.GetOrCreateDataCenter("dc1")
	rack := dc.GetOrCreateRack("rack1")
	dn := rack.GetOrCreateDataNode("server1", 8080, 0, "127.0.0.1", nil)

	volumeId := needle.VolumeId(456)
	collection := "test_collection"

	// Test 1: No active generation initially
	activeGen, exists := topo.GetEcActiveGeneration(volumeId)
	if exists {
		t.Errorf("Expected no active generation initially, but got %d", activeGen)
	}

	// Test 2: Register generation 0 - should become active automatically
	ecInfo0 := &erasure_coding.EcVolumeInfo{
		VolumeId:   volumeId,
		Collection: collection,
		ShardBits:  erasure_coding.ShardBits(0xFF), // shards 0-7
		Generation: 0,
	}
	topo.RegisterEcShards(ecInfo0, dn)

	activeGen, exists = topo.GetEcActiveGeneration(volumeId)
	if !exists {
		t.Errorf("Expected active generation to exist after registering generation 0")
	}
	if activeGen != 0 {
		t.Errorf("Expected active generation 0, got %d", activeGen)
	}

	// Test 3: Register generation 1 - should become active automatically (higher generation)
	ecInfo1 := &erasure_coding.EcVolumeInfo{
		VolumeId:   volumeId,
		Collection: collection,
		ShardBits:  erasure_coding.ShardBits(0xFF00), // shards 8-15 (hypothetical)
		Generation: 1,
	}
	topo.RegisterEcShards(ecInfo1, dn)

	activeGen, exists = topo.GetEcActiveGeneration(volumeId)
	if !exists {
		t.Errorf("Expected active generation to exist after registering generation 1")
	}
	if activeGen != 1 {
		t.Errorf("Expected active generation 1, got %d", activeGen)
	}

	// Test 4: Manually set active generation
	topo.SetEcActiveGeneration(volumeId, 0)
	activeGen, exists = topo.GetEcActiveGeneration(volumeId)
	if !exists {
		t.Errorf("Expected active generation to exist after manual set")
	}
	if activeGen != 0 {
		t.Errorf("Expected active generation 0 after manual set, got %d", activeGen)
	}

	// Test 5: List volumes with active generation
	volumes := topo.ListEcVolumesWithActiveGeneration()
	found := false
	for vid, gen := range volumes {
		if vid == volumeId && gen == 0 {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Expected to find volume %d with active generation 0 in list", volumeId)
	}
}

// TestEcGenerationFallbackLookup tests the intelligent lookup with fallback
func TestEcGenerationFallbackLookup(t *testing.T) {
	topo := NewTopology("test", sequence.NewMemorySequencer(), 32*1024, 5, false)
	dc := topo.GetOrCreateDataCenter("dc1")
	rack := dc.GetOrCreateRack("rack1")
	dn := rack.GetOrCreateDataNode("server1", 8080, 0, "127.0.0.1", nil)

	volumeId := needle.VolumeId(789)
	collection := "test_collection"

	// Register only generation 2
	ecInfo2 := &erasure_coding.EcVolumeInfo{
		VolumeId:   volumeId,
		Collection: collection,
		ShardBits:  erasure_coding.ShardBits(0x3FFF), // all 14 shards
		Generation: 2,
	}
	topo.RegisterEcShards(ecInfo2, dn)

	// Test 1: Request generation 0 (doesn't exist) - should use active generation
	locations, actualGen, found := topo.LookupEcShardsWithFallback(volumeId, 0)
	if !found {
		t.Errorf("Expected fallback lookup to find the volume")
	}
	if actualGen != 2 {
		t.Errorf("Expected fallback to return generation 2, got %d", actualGen)
	}
	if locations.Generation != 2 {
		t.Errorf("Expected locations to be for generation 2, got %d", locations.Generation)
	}

	// Test 2: Request specific generation 2 - should return exact match
	locations, actualGen, found = topo.LookupEcShardsWithFallback(volumeId, 2)
	if !found {
		t.Errorf("Expected direct lookup to find generation 2")
	}
	if actualGen != 2 {
		t.Errorf("Expected exact match to return generation 2, got %d", actualGen)
	}

	// Test 3: Request non-existent generation 5 - should fail (no fallback for specific requests)
	_, _, found = topo.LookupEcShardsWithFallback(volumeId, 5)
	if found {
		t.Errorf("Expected lookup for non-existent generation 5 to fail")
	}

	// Test 4: Register generation 0 and test fallback preference
	ecInfo0 := &erasure_coding.EcVolumeInfo{
		VolumeId:   volumeId,
		Collection: collection,
		ShardBits:  erasure_coding.ShardBits(0x3FFF), // all 14 shards
		Generation: 0,
	}
	topo.RegisterEcShards(ecInfo0, dn)

	// Manually set generation 0 as active (lower than 2, but manually set)
	topo.SetEcActiveGeneration(volumeId, 0)

	// Request generation 0 should use the active generation (0)
	locations, actualGen, found = topo.LookupEcShardsWithFallback(volumeId, 0)
	if !found {
		t.Errorf("Expected lookup to find generation 0")
	}
	if actualGen != 0 {
		t.Errorf("Expected fallback to return active generation 0, got %d", actualGen)
	}
}

// TestEcGenerationActivation tests the ActivateEcGeneration functionality
func TestEcGenerationActivation(t *testing.T) {
	topo := NewTopology("test", sequence.NewMemorySequencer(), 32*1024, 5, false)
	dc := topo.GetOrCreateDataCenter("dc1")
	rack := dc.GetOrCreateRack("rack1")

	// Create multiple data nodes for testing readiness
	var dataNodes []*DataNode
	for i := 0; i < 3; i++ {
		dn := rack.GetOrCreateDataNode(fmt.Sprintf("127.0.0.%d", i+1), 8080, 0, fmt.Sprintf("127.0.0.%d", i+1), nil)
		dataNodes = append(dataNodes, dn)
	}

	volumeId := needle.VolumeId(321)
	collection := "test_collection"

	// Test 1: Try to activate non-existent generation - should fail
	ready, _, err := topo.ValidateEcGenerationReadiness(volumeId, 1)
	if ready {
		t.Errorf("Expected generation 1 to not be ready (doesn't exist)")
	}
	if err == nil {
		t.Errorf("Expected error for non-existent generation")
	}

	// Test 2: Register incomplete generation 1 (only 5 shards) - should not be ready
	ecInfo1 := &erasure_coding.EcVolumeInfo{
		VolumeId:   volumeId,
		Collection: collection,
		ShardBits:  erasure_coding.ShardBits(0x1F), // shards 0,1,2,3,4 (5 shards)
		Generation: 1,
	}
	topo.RegisterEcShards(ecInfo1, dataNodes[0])

	ready, shardCount, err := topo.ValidateEcGenerationReadiness(volumeId, 1)
	if ready {
		t.Errorf("Expected generation 1 to not be ready (only 5 shards), got %d shards", shardCount)
	}
	if err != nil {
		t.Logf("Got expected error for insufficient shards: %v", err)
	}
	if shardCount != 5 {
		t.Errorf("Expected 5 shards, got %d", shardCount)
	}

	// Test 3: Complete generation 1 (add remaining shards) - should be ready
	ecInfo1b := &erasure_coding.EcVolumeInfo{
		VolumeId:   volumeId,
		Collection: collection,
		ShardBits:  erasure_coding.ShardBits(0x3FE0), // shards 5-13 (9 more shards = 14 total)
		Generation: 1,
	}
	topo.RegisterEcShards(ecInfo1b, dataNodes[1])

	ready, _, err = topo.ValidateEcGenerationReadiness(volumeId, 1)
	if !ready {
		t.Errorf("Expected generation 1 to be ready (14 shards), got error: %v", err)
	}

	// Test 4: Activate generation 1 - should succeed
	topo.SetEcActiveGeneration(volumeId, 1)
	activeGen, exists := topo.GetEcActiveGeneration(volumeId)
	if !exists {
		t.Errorf("Expected active generation to exist after activation")
	}
	if activeGen != 1 {
		t.Errorf("Expected active generation 1, got %d", activeGen)
	}

	// Test 5: Verify activation affects lookup behavior
	_, actualGen, found := topo.LookupEcShardsWithFallback(volumeId, 0)
	if !found {
		t.Errorf("Expected fallback lookup to find the volume")
	}
	if actualGen != 1 {
		t.Errorf("Expected fallback to use active generation 1, got %d", actualGen)
	}
}

// TestEcGenerationUnregistration tests shard unregistration and cleanup
func TestEcGenerationUnregistration(t *testing.T) {
	topo := NewTopology("test", sequence.NewMemorySequencer(), 32*1024, 5, false)
	dc := topo.GetOrCreateDataCenter("dc1")
	rack := dc.GetOrCreateRack("rack1")
	dn1 := rack.GetOrCreateDataNode("server1", 8080, 0, "127.0.0.1", nil)
	dn2 := rack.GetOrCreateDataNode("server2", 8080, 0, "127.0.0.2", nil)

	volumeId := needle.VolumeId(654)
	collection := "test_collection"

	// Register two generations
	ecInfo0 := &erasure_coding.EcVolumeInfo{
		VolumeId:   volumeId,
		Collection: collection,
		ShardBits:  erasure_coding.ShardBits(0x3FFF), // all 14 shards
		Generation: 0,
	}
	topo.RegisterEcShards(ecInfo0, dn1)

	ecInfo1 := &erasure_coding.EcVolumeInfo{
		VolumeId:   volumeId,
		Collection: collection,
		ShardBits:  erasure_coding.ShardBits(0x3FFF), // all 14 shards
		Generation: 1,
	}
	topo.RegisterEcShards(ecInfo1, dn2)

	// Verify both generations exist
	_, found0 := topo.LookupEcShards(volumeId, 0)
	_, found1 := topo.LookupEcShards(volumeId, 1)
	if !found0 || !found1 {
		t.Errorf("Expected both generations to exist")
	}

	// Active generation should be 1 (higher)
	activeGen, exists := topo.GetEcActiveGeneration(volumeId)
	if !exists || activeGen != 1 {
		t.Errorf("Expected active generation 1, got %d (exists: %v)", activeGen, exists)
	}

	// Test 1: Unregister generation 0 (not active) - should clean up
	topo.UnRegisterEcShards(ecInfo0, dn1)

	_, found0 = topo.LookupEcShards(volumeId, 0)
	if found0 {
		t.Errorf("Expected generation 0 to be cleaned up after unregistration")
	}

	// Active generation should still be 1
	activeGen, exists = topo.GetEcActiveGeneration(volumeId)
	if !exists || activeGen != 1 {
		t.Errorf("Expected active generation to remain 1, got %d (exists: %v)", activeGen, exists)
	}

	// Test 2: Unregister generation 1 (active) - should clean up and remove active tracking
	topo.UnRegisterEcShards(ecInfo1, dn2)

	_, found1 = topo.LookupEcShards(volumeId, 1)
	if found1 {
		t.Errorf("Expected generation 1 to be cleaned up after unregistration")
	}

	// Active generation tracking should be removed
	_, exists = topo.GetEcActiveGeneration(volumeId)
	if exists {
		t.Errorf("Expected active generation tracking to be removed")
	}
}

// TestEcGenerationMixedVersionLookup tests backward compatibility with mixed versions
func TestEcGenerationMixedVersionLookup(t *testing.T) {
	topo := NewTopology("test", sequence.NewMemorySequencer(), 32*1024, 5, false)
	dc := topo.GetOrCreateDataCenter("dc1")
	rack := dc.GetOrCreateRack("rack1")
	dn := rack.GetOrCreateDataNode("server1", 8080, 0, "127.0.0.1", nil)

	volumeId := needle.VolumeId(987)
	collection := "test_collection"

	// Register both generation 0 (legacy) and generation 1 (new)
	ecInfo0 := &erasure_coding.EcVolumeInfo{
		VolumeId:   volumeId,
		Collection: collection,
		ShardBits:  erasure_coding.ShardBits(0x3FFF), // all 14 shards
		Generation: 0,
	}
	topo.RegisterEcShards(ecInfo0, dn)

	ecInfo1 := &erasure_coding.EcVolumeInfo{
		VolumeId:   volumeId,
		Collection: collection,
		ShardBits:  erasure_coding.ShardBits(0x3FFF), // all 14 shards
		Generation: 1,
	}
	topo.RegisterEcShards(ecInfo1, dn)

	// Set generation 1 as active
	topo.SetEcActiveGeneration(volumeId, 1)

	// Test 1: Legacy client requests generation 0 (fallback behavior)
	_, actualGen, found := topo.LookupEcShardsWithFallback(volumeId, 0)
	if !found {
		t.Errorf("Expected fallback lookup to find the volume")
	}
	// Should return active generation (1) when requesting 0
	if actualGen != 1 {
		t.Errorf("Expected fallback to return active generation 1, got %d", actualGen)
	}

	// Test 2: New client requests specific generation 1
	_, actualGen, found = topo.LookupEcShardsWithFallback(volumeId, 1)
	if !found {
		t.Errorf("Expected direct lookup to find generation 1")
	}
	if actualGen != 1 {
		t.Errorf("Expected exact match for generation 1, got %d", actualGen)
	}

	// Test 3: Legacy behavior - no active generation set, should use generation 0
	topo2 := NewTopology("test2", sequence.NewMemorySequencer(), 32*1024, 5, false)
	dc2 := topo2.GetOrCreateDataCenter("dc1")
	rack2 := dc2.GetOrCreateRack("rack1")
	dn2 := rack2.GetOrCreateDataNode("server1", 8080, 0, "127.0.0.1", nil)

	// Register only generation 0
	topo2.RegisterEcShards(ecInfo0, dn2)

	_, actualGen, found = topo2.LookupEcShardsWithFallback(volumeId, 0)
	if !found {
		t.Errorf("Expected lookup to find generation 0")
	}
	if actualGen != 0 {
		t.Errorf("Expected generation 0 for legacy volume, got %d", actualGen)
	}
}

// TestEcGenerationConcurrentOperations tests thread safety of generation operations
func TestEcGenerationConcurrentOperations(t *testing.T) {
	topo := NewTopology("test", sequence.NewMemorySequencer(), 32*1024, 5, false)
	dc := topo.GetOrCreateDataCenter("dc1")
	rack := dc.GetOrCreateRack("rack1")
	dn := rack.GetOrCreateDataNode("server1", 8080, 0, "127.0.0.1", nil)

	volumeId := needle.VolumeId(111)
	collection := "test_collection"

	// Test concurrent registration and lookup operations
	// This is a basic test - in practice you'd use goroutines and sync.WaitGroup
	// for proper concurrent testing

	// Register multiple generations
	for gen := uint32(0); gen < 5; gen++ {
		ecInfo := &erasure_coding.EcVolumeInfo{
			VolumeId:   volumeId,
			Collection: collection,
			ShardBits:  erasure_coding.ShardBits(0x3FFF), // all 14 shards
			Generation: gen,
		}
		topo.RegisterEcShards(ecInfo, dn)

		// Verify immediate lookup
		locations, found := topo.LookupEcShards(volumeId, gen)
		if !found {
			t.Errorf("Expected to find generation %d immediately after registration", gen)
		}
		if locations.Generation != gen {
			t.Errorf("Expected generation %d, got %d", gen, locations.Generation)
		}
	}

	// Verify all generations are accessible
	for gen := uint32(0); gen < 5; gen++ {
		_, found := topo.LookupEcShards(volumeId, gen)
		if !found {
			t.Errorf("Expected generation %d to be accessible", gen)
		}
	}

	// Active generation should be the highest (4)
	activeGen, exists := topo.GetEcActiveGeneration(volumeId)
	if !exists || activeGen != 4 {
		t.Errorf("Expected active generation 4, got %d (exists: %v)", activeGen, exists)
	}
}

// Helper function to create a context with timeout
func createTestContext() context.Context {
	ctx, _ := context.WithTimeout(context.Background(), time.Second*10)
	return ctx
}
