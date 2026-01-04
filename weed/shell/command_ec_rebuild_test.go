package shell

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// TestEcShardMapRegister tests that EC shards are properly registered
func TestEcShardMapRegister(t *testing.T) {
	ecShardMap := make(EcShardMap)

	// Create test nodes with EC shards
	node1 := newEcNode("dc1", "rack1", "node1", 100).
		addEcVolumeAndShardsForTest(1, "c1", []erasure_coding.ShardId{0, 1, 2, 3, 4, 5, 6})
	node2 := newEcNode("dc1", "rack1", "node2", 100).
		addEcVolumeAndShardsForTest(1, "c1", []erasure_coding.ShardId{7, 8, 9, 10, 11, 12, 13})

	ecShardMap.registerEcNode(node1, "c1")
	ecShardMap.registerEcNode(node2, "c1")

	// Verify volume 1 is registered
	locations, found := ecShardMap[needle.VolumeId(1)]
	if !found {
		t.Fatal("Expected volume 1 to be registered")
	}

	// Check shard count
	count := locations.shardCount()
	if count != erasure_coding.TotalShardsCount {
		t.Errorf("Expected %d shards, got %d", erasure_coding.TotalShardsCount, count)
	}

	// Verify shard distribution
	for i := 0; i < 7; i++ {
		if len(locations[i]) != 1 || locations[i][0].info.Id != "node1" {
			t.Errorf("Shard %d should be on node1", i)
		}
	}
	for i := 7; i < erasure_coding.TotalShardsCount; i++ {
		if len(locations[i]) != 1 || locations[i][0].info.Id != "node2" {
			t.Errorf("Shard %d should be on node2", i)
		}
	}
}

// TestEcShardMapShardCount tests shard counting
func TestEcShardMapShardCount(t *testing.T) {
	testCases := []struct {
		name          string
		shardIds      []erasure_coding.ShardId
		expectedCount int
	}{
		{"all shards", []erasure_coding.ShardId{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13}, 14},
		{"data shards only", []erasure_coding.ShardId{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, 10},
		{"parity shards only", []erasure_coding.ShardId{10, 11, 12, 13}, 4},
		{"missing some shards", []erasure_coding.ShardId{0, 1, 2, 3, 4, 5, 6, 7, 8}, 9},
		{"single shard", []erasure_coding.ShardId{0}, 1},
		{"no shards", []erasure_coding.ShardId{}, 0},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			locations := make(EcShardLocations, erasure_coding.MaxShardCount)
			for _, shardId := range tc.shardIds {
				locations[shardId] = []*EcNode{
					newEcNode("dc1", "rack1", "node1", 100),
				}
			}

			count := locations.shardCount()
			if count != tc.expectedCount {
				t.Errorf("Expected %d shards, got %d", tc.expectedCount, count)
			}
		})
	}
}

// TestRebuildEcVolumesInsufficientShards tests error handling for unrepairable volumes
func TestRebuildEcVolumesInsufficientShards(t *testing.T) {
	var logBuffer bytes.Buffer

	// Create a volume with insufficient shards (less than DataShardsCount)
	node1 := newEcNode("dc1", "rack1", "node1", 100).
		addEcVolumeAndShardsForTest(1, "c1", []erasure_coding.ShardId{0, 1, 2, 3, 4}) // Only 5 shards

	erb := &ecRebuilder{
		commandEnv: &CommandEnv{
			env:    make(map[string]string),
			noLock: true, // Bypass lock check for unit test
		},
		ewg:     NewErrorWaitGroup(DefaultMaxParallelization),
		ecNodes: []*EcNode{node1},
		writer:  &logBuffer,
	}

	erb.rebuildEcVolumes("c1")
	err := erb.ewg.Wait()

	if err == nil {
		t.Fatal("Expected error for insufficient shards, got nil")
	}
	if !strings.Contains(err.Error(), "unrepairable") {
		t.Errorf("Expected 'unrepairable' in error message, got: %s", err.Error())
	}
}

// TestRebuildEcVolumesCompleteVolume tests that complete volumes are skipped
func TestRebuildEcVolumesCompleteVolume(t *testing.T) {
	var logBuffer bytes.Buffer

	// Create a volume with all shards
	node1 := newEcNode("dc1", "rack1", "node1", 100).
		addEcVolumeAndShardsForTest(1, "c1", []erasure_coding.ShardId{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13})

	erb := &ecRebuilder{
		commandEnv: &CommandEnv{
			env:    make(map[string]string),
			noLock: true, // Bypass lock check for unit test
		},
		ewg:          NewErrorWaitGroup(DefaultMaxParallelization),
		ecNodes:      []*EcNode{node1},
		writer:       &logBuffer,
		applyChanges: false,
	}

	erb.rebuildEcVolumes("c1")
	err := erb.ewg.Wait()

	if err != nil {
		t.Fatalf("Expected no error for complete volume, got: %v", err)
	}

	// The function should return quickly without attempting rebuild
	// since the volume is already complete
}

// TestRebuildEcVolumesInsufficientSpace tests error handling for insufficient disk space
func TestRebuildEcVolumesInsufficientSpace(t *testing.T) {
	var logBuffer bytes.Buffer

	// Create a volume with missing shards but insufficient free slots
	// Node has 10 local shards, missing 4 shards (10,11,12,13), so needs 4 free slots
	// Set free slots to 3 (insufficient)
	node1 := newEcNode("dc1", "rack1", "node1", 3). // Only 3 free slots, need 4
							addEcVolumeAndShardsForTest(1, "c1", []erasure_coding.ShardId{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})

	erb := &ecRebuilder{
		commandEnv: &CommandEnv{
			env:    make(map[string]string),
			noLock: true, // Bypass lock check for unit test
		},
		ewg:          NewErrorWaitGroup(DefaultMaxParallelization),
		ecNodes:      []*EcNode{node1},
		writer:       &logBuffer,
		applyChanges: false,
	}

	erb.rebuildEcVolumes("c1")
	err := erb.ewg.Wait()

	if err == nil {
		t.Fatal("Expected error for insufficient disk space, got nil")
	}
	if !strings.Contains(err.Error(), "no node has sufficient free slots") {
		t.Errorf("Expected 'no node has sufficient free slots' in error message, got: %s", err.Error())
	}
	// Verify the enhanced error message includes diagnostic information
	if !strings.Contains(err.Error(), "need") || !strings.Contains(err.Error(), "max available") {
		t.Errorf("Expected diagnostic information in error message, got: %s", err.Error())
	}
}

// TestMultipleNodesWithShards tests rebuild with shards distributed across multiple nodes
func TestMultipleNodesWithShards(t *testing.T) {
	ecShardMap := make(EcShardMap)

	// Create 3 nodes with different shards
	node1 := newEcNode("dc1", "rack1", "node1", 100).
		addEcVolumeAndShardsForTest(1, "c1", []erasure_coding.ShardId{0, 1, 2, 3})
	node2 := newEcNode("dc1", "rack1", "node2", 100).
		addEcVolumeAndShardsForTest(1, "c1", []erasure_coding.ShardId{4, 5, 6, 7})
	node3 := newEcNode("dc1", "rack1", "node3", 100).
		addEcVolumeAndShardsForTest(1, "c1", []erasure_coding.ShardId{8, 9})

	ecShardMap.registerEcNode(node1, "c1")
	ecShardMap.registerEcNode(node2, "c1")
	ecShardMap.registerEcNode(node3, "c1")

	locations := ecShardMap[needle.VolumeId(1)]
	count := locations.shardCount()

	// We have 10 shards total, which is enough for data shards
	if count != 10 {
		t.Errorf("Expected 10 shards, got %d", count)
	}

	// Verify each shard is on the correct node
	for i := 0; i < 4; i++ {
		if len(locations[i]) != 1 || locations[i][0].info.Id != "node1" {
			t.Errorf("Shard %d should be on node1", i)
		}
	}
	for i := 4; i < 8; i++ {
		if len(locations[i]) != 1 || locations[i][0].info.Id != "node2" {
			t.Errorf("Shard %d should be on node2", i)
		}
	}
	for i := 8; i < 10; i++ {
		if len(locations[i]) != 1 || locations[i][0].info.Id != "node3" {
			t.Errorf("Shard %d should be on node3", i)
		}
	}
}

// TestDuplicateShards tests handling of duplicate shards on multiple nodes
func TestDuplicateShards(t *testing.T) {
	ecShardMap := make(EcShardMap)

	// Create 2 nodes with overlapping shards (both have shard 0)
	node1 := newEcNode("dc1", "rack1", "node1", 100).
		addEcVolumeAndShardsForTest(1, "c1", []erasure_coding.ShardId{0, 1, 2, 3})
	node2 := newEcNode("dc1", "rack1", "node2", 100).
		addEcVolumeAndShardsForTest(1, "c1", []erasure_coding.ShardId{0, 4, 5, 6}) // Duplicate shard 0

	ecShardMap.registerEcNode(node1, "c1")
	ecShardMap.registerEcNode(node2, "c1")

	locations := ecShardMap[needle.VolumeId(1)]

	// Shard 0 should be on both nodes
	if len(locations[0]) != 2 {
		t.Errorf("Expected shard 0 on 2 nodes, got %d", len(locations[0]))
	}

	// Verify both nodes are registered for shard 0
	foundNode1 := false
	foundNode2 := false
	for _, node := range locations[0] {
		if node.info.Id == "node1" {
			foundNode1 = true
		}
		if node.info.Id == "node2" {
			foundNode2 = true
		}
	}
	if !foundNode1 || !foundNode2 {
		t.Error("Both nodes should have shard 0")
	}

	// Shard count should be 7 (unique shards: 0, 1, 2, 3, 4, 5, 6)
	count := locations.shardCount()
	if count != 7 {
		t.Errorf("Expected 7 unique shards, got %d", count)
	}
}

// TestPrepareDataToRecoverTargetShardCount tests that prepareDataToRecover caps the shard reporting
func TestPrepareDataToRecoverTargetShardCount(t *testing.T) {
	var logBuffer bytes.Buffer

	// Create a node with 10 shards (0-9)
	node1 := newEcNode("dc1", "rack1", "node1", 100).
		addEcVolumeAndShardsForTest(1, "c1", []erasure_coding.ShardId{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})

	erb := &ecRebuilder{
		commandEnv: &CommandEnv{
			env:    make(map[string]string),
			noLock: true,
		},
		ecNodes: []*EcNode{node1},
		writer:  &logBuffer,
	}

	locations := make(EcShardLocations, erasure_coding.MaxShardCount)
	for i := 0; i < 10; i++ {
		locations[i] = []*EcNode{node1}
	}

	// Shards 10-13 are missing, but 14-31 should NOT be reported
	_, _, err := erb.prepareDataToRecover(node1, "c1", needle.VolumeId(1), locations)
	if err != nil {
		t.Fatalf("prepareDataToRecover failed: %v", err)
	}

	output := logBuffer.String()
	lines := strings.Split(strings.TrimSpace(output), "\n")

	// We expect "use existing shard 1.0" through "use existing shard 1.9" (10 lines)
	// and "missing shard 1.10" through "missing shard 1.13" (4 lines)
	// Total 14 lines expected. Shards 14-31 should be absent.
	expectedLineCount := 14
	if len(lines) != expectedLineCount {
		t.Errorf("Expected %d lines of output, got %d. Output:\n%s", expectedLineCount, len(lines), output)
	}

	for i := 14; i < erasure_coding.MaxShardCount; i++ {
		if strings.Contains(output, "shard 1."+strings.TrimSpace(string(rune('0'+i)))) ||
			strings.Contains(output, "shard 1."+fmt.Sprintf("%d", i)) {
			t.Errorf("Shard 1.%d should not be reported in output", i)
		}
	}
}
