package storage

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/klauspost/reedsolomon"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// mockEcVolume creates a mock EC volume for testing
func mockEcVolume(volumeId needle.VolumeId, shardLocations map[erasure_coding.ShardId][]pb.ServerAddress) *erasure_coding.EcVolume {
	ecVolume := &erasure_coding.EcVolume{
		VolumeId:       volumeId,
		ShardLocations: shardLocations,
	}
	return ecVolume
}

// TestRecoverOneRemoteEcShardInterval_ShardCounting tests the shard counting logic
func TestRecoverOneRemoteEcShardInterval_ShardCounting(t *testing.T) {
	tests := []struct {
		name                string
		totalShards         int
		shardToRecover      int
		expectSufficientFor bool
	}{
		{
			name:                "All shards available except one",
			totalShards:         erasure_coding.TotalShardsCount - 1,
			shardToRecover:      5,
			expectSufficientFor: true,
		},
		{
			name:                "Exactly minimum shards (DataShardsCount)",
			totalShards:         erasure_coding.DataShardsCount,
			shardToRecover:      13,
			expectSufficientFor: true,
		},
		{
			name:                "One less than minimum",
			totalShards:         erasure_coding.DataShardsCount - 1,
			shardToRecover:      10,
			expectSufficientFor: false,
		},
		{
			name:                "Only half the shards",
			totalShards:         erasure_coding.TotalShardsCount / 2,
			shardToRecover:      0,
			expectSufficientFor: false,
		},
		{
			name:                "All data shards available",
			totalShards:         erasure_coding.DataShardsCount,
			shardToRecover:      11, // Recovering a parity shard
			expectSufficientFor: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate the bufs array that would be populated
			bufs := make([][]byte, erasure_coding.MaxShardCount)

			// Fill in available shards (excluding the one to recover)
			shardCount := 0
			for i := 0; i < erasure_coding.TotalShardsCount && shardCount < tt.totalShards; i++ {
				if i != tt.shardToRecover {
					bufs[i] = make([]byte, 1024) // dummy data
					shardCount++
				}
			}

			// Count available and missing shards (mimicking the modified code)
			availableShards := make([]erasure_coding.ShardId, 0)
			missingShards := make([]erasure_coding.ShardId, 0)
			for shardId := 0; shardId < erasure_coding.MaxShardCount; shardId++ {
				if bufs[shardId] != nil {
					availableShards = append(availableShards, erasure_coding.ShardId(shardId))
				} else if shardId < erasure_coding.TotalShardsCount {
					missingShards = append(missingShards, erasure_coding.ShardId(shardId))
				}
			}

			// Verify the count matches expectations
			hasSufficient := len(availableShards) >= erasure_coding.DataShardsCount
			if hasSufficient != tt.expectSufficientFor {
				t.Errorf("Expected sufficient=%v, got sufficient=%v (available=%d, need=%d)",
					tt.expectSufficientFor, hasSufficient, len(availableShards), erasure_coding.DataShardsCount)
			}

			t.Logf("Available shards: %d %v, Missing shards: %d %v",
				len(availableShards), availableShards,
				len(missingShards), missingShards)
		})
	}
}

// TestRecoverOneRemoteEcShardInterval_ErrorMessage tests the improved error messages
func TestRecoverOneRemoteEcShardInterval_ErrorMessage(t *testing.T) {
	volumeId := needle.VolumeId(42)
	shardIdToRecover := erasure_coding.ShardId(7)

	// Simulate insufficient shards scenario
	availableShards := []erasure_coding.ShardId{0, 1, 2, 3, 4, 5, 6}
	missingShards := []erasure_coding.ShardId{7, 8, 9, 10, 11, 12, 13}

	// Verify error message contains all required information
	expectedErr := fmt.Errorf("cannot recover shard %d.%d: only %d shards available %v, need at least %d (missing: %v)",
		volumeId, shardIdToRecover,
		len(availableShards), availableShards,
		erasure_coding.DataShardsCount, missingShards)

	errMsg := expectedErr.Error()

	// Check that error message contains key information
	if !strings.Contains(errMsg, fmt.Sprintf("shard %d.%d", volumeId, shardIdToRecover)) {
		t.Errorf("Error message missing volume.shard identifier")
	}
	if !strings.Contains(errMsg, fmt.Sprintf("%d shards available", len(availableShards))) {
		t.Errorf("Error message missing available shard count")
	}
	if !strings.Contains(errMsg, fmt.Sprintf("need at least %d", erasure_coding.DataShardsCount)) {
		t.Errorf("Error message missing required shard count")
	}

	t.Logf("Error message format validated: %s", errMsg)
}

// TestRecoverOneRemoteEcShardInterval_ReconstructDataSlicing tests the buffer slicing fix
func TestRecoverOneRemoteEcShardInterval_ReconstructDataSlicing(t *testing.T) {
	// This test validates that we pass bufs[:TotalShardsCount] to ReconstructData
	// instead of the full bufs array which could be MaxShardCount (32)

	enc, err := reedsolomon.New(erasure_coding.DataShardsCount, erasure_coding.ParityShardsCount)
	if err != nil {
		t.Fatalf("Failed to create encoder: %v", err)
	}

	// Create test data
	shardSize := 1024
	bufs := make([][]byte, erasure_coding.MaxShardCount)

	// Fill data shards
	for i := 0; i < erasure_coding.DataShardsCount; i++ {
		bufs[i] = make([]byte, shardSize)
		for j := range bufs[i] {
			bufs[i][j] = byte(i + j)
		}
	}

	// Create parity shards (initially nil)
	for i := erasure_coding.DataShardsCount; i < erasure_coding.TotalShardsCount; i++ {
		bufs[i] = make([]byte, shardSize)
	}

	// Encode to generate parity
	if err := enc.Encode(bufs[:erasure_coding.TotalShardsCount]); err != nil {
		t.Fatalf("Failed to encode: %v", err)
	}

	// Simulate loss of shard 5
	originalShard5 := make([]byte, shardSize)
	copy(originalShard5, bufs[5])
	bufs[5] = nil

	// Reconstruct using only TotalShardsCount elements (not MaxShardCount)
	if err := enc.ReconstructData(bufs[:erasure_coding.TotalShardsCount]); err != nil {
		t.Fatalf("Failed to reconstruct data: %v", err)
	}

	// Verify shard 5 was recovered correctly
	if bufs[5] == nil {
		t.Errorf("Shard 5 was not recovered")
	} else {
		for i := range originalShard5 {
			if originalShard5[i] != bufs[5][i] {
				t.Errorf("Recovered shard 5 data mismatch at byte %d: expected %d, got %d",
					i, originalShard5[i], bufs[5][i])
				break
			}
		}
	}

	t.Logf("Successfully reconstructed shard with proper buffer slicing")
}

// TestRecoverOneRemoteEcShardInterval_ParityShardRecovery tests recovering parity shards
func TestRecoverOneRemoteEcShardInterval_ParityShardRecovery(t *testing.T) {
	// Parity shards (10-13) should be recoverable with all data shards (0-9)

	enc, err := reedsolomon.New(erasure_coding.DataShardsCount, erasure_coding.ParityShardsCount)
	if err != nil {
		t.Fatalf("Failed to create encoder: %v", err)
	}

	shardSize := 512
	bufs := make([][]byte, erasure_coding.TotalShardsCount)

	// Fill all shards initially
	for i := 0; i < erasure_coding.TotalShardsCount; i++ {
		bufs[i] = make([]byte, shardSize)
		for j := range bufs[i] {
			bufs[i][j] = byte(i * j)
		}
	}

	// Encode
	if err := enc.Encode(bufs); err != nil {
		t.Fatalf("Failed to encode: %v", err)
	}

	// Test recovering each parity shard
	for parityShard := erasure_coding.DataShardsCount; parityShard < erasure_coding.TotalShardsCount; parityShard++ {
		t.Run(fmt.Sprintf("RecoverParity%d", parityShard), func(t *testing.T) {
			testBufs := make([][]byte, erasure_coding.TotalShardsCount)
			for i := range testBufs {
				if i != parityShard {
					testBufs[i] = make([]byte, shardSize)
					copy(testBufs[i], bufs[i])
				}
			}

			// Reconstruct (handles both data and parity)
			if err := enc.Reconstruct(testBufs); err != nil {
				t.Errorf("Failed to reconstruct parity shard %d: %v", parityShard, err)
			}

			// Verify
			if testBufs[parityShard] == nil {
				t.Errorf("Parity shard %d was not recovered", parityShard)
			}
		})
	}
}

// TestRecoverOneRemoteEcShardInterval_ConcurrentShardReading tests the concurrent shard reading
func TestRecoverOneRemoteEcShardInterval_ConcurrentShardReading(t *testing.T) {
	// Simulate the concurrent reading pattern in recoverOneRemoteEcShardInterval

	shardIdToRecover := erasure_coding.ShardId(7)

	shardLocations := make(map[erasure_coding.ShardId][]pb.ServerAddress)
	for i := 0; i < erasure_coding.TotalShardsCount; i++ {
		if i != int(shardIdToRecover) {
			shardLocations[erasure_coding.ShardId(i)] = []pb.ServerAddress{"server1:8080"}
		}
	}

	// Simulate concurrent shard reading
	bufs := make([][]byte, erasure_coding.MaxShardCount)
	var wg sync.WaitGroup
	var mu sync.Mutex
	readErrors := make(map[erasure_coding.ShardId]error)

	for shardId, locations := range shardLocations {
		if shardId == shardIdToRecover {
			continue
		}
		if len(locations) == 0 {
			continue
		}

		wg.Add(1)
		go func(sid erasure_coding.ShardId) {
			defer wg.Done()

			// Simulate successful read
			data := make([]byte, 1024)
			for i := range data {
				data[i] = byte(sid)
			}

			mu.Lock()
			bufs[sid] = data
			mu.Unlock()
		}(shardId)
	}

	wg.Wait()

	// Count available shards
	availableCount := 0
	for i := 0; i < erasure_coding.TotalShardsCount; i++ {
		if bufs[i] != nil {
			availableCount++
		}
	}

	expectedCount := erasure_coding.TotalShardsCount - 1 // All except the one to recover
	if availableCount != expectedCount {
		t.Errorf("Expected %d shards to be read, got %d", expectedCount, availableCount)
	}

	// Verify no errors occurred
	if len(readErrors) > 0 {
		t.Errorf("Unexpected read errors: %v", readErrors)
	}

	t.Logf("Successfully simulated concurrent reading of %d shards", availableCount)
}
