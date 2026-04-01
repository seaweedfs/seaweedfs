package shell

import (
	"bytes"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle_map"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func TestBuildUnionFromMultipleIndexDatabases(t *testing.T) {
	// Test that we can correctly identify missing entries between replicas

	// Create mock index databases representing different replicas
	replicaA := needle_map.NewMemDb()
	replicaB := needle_map.NewMemDb()
	replicaC := needle_map.NewMemDb()
	defer replicaA.Close()
	defer replicaB.Close()
	defer replicaC.Close()

	// Replica A has entries 1, 2, 3, 4, 5
	replicaA.Set(types.NeedleId(1), types.Offset{}, types.Size(100))
	replicaA.Set(types.NeedleId(2), types.Offset{}, types.Size(100))
	replicaA.Set(types.NeedleId(3), types.Offset{}, types.Size(100))
	replicaA.Set(types.NeedleId(4), types.Offset{}, types.Size(100))
	replicaA.Set(types.NeedleId(5), types.Offset{}, types.Size(100))

	// Replica B has entries 1, 2, 3, 6, 7 (missing 4, 5 from A, has unique 6, 7)
	replicaB.Set(types.NeedleId(1), types.Offset{}, types.Size(100))
	replicaB.Set(types.NeedleId(2), types.Offset{}, types.Size(100))
	replicaB.Set(types.NeedleId(3), types.Offset{}, types.Size(100))
	replicaB.Set(types.NeedleId(6), types.Offset{}, types.Size(100))
	replicaB.Set(types.NeedleId(7), types.Offset{}, types.Size(100))

	// Replica C has entries 1, 2, 8 (minimal overlap, has unique 8)
	replicaC.Set(types.NeedleId(1), types.Offset{}, types.Size(100))
	replicaC.Set(types.NeedleId(2), types.Offset{}, types.Size(100))
	replicaC.Set(types.NeedleId(8), types.Offset{}, types.Size(100))

	// Test: Find entries in B that are missing from A
	var missingFromA []types.NeedleId
	replicaB.AscendingVisit(func(nv needle_map.NeedleValue) error {
		if _, found := replicaA.Get(nv.Key); !found {
			missingFromA = append(missingFromA, nv.Key)
		}
		return nil
	})

	if len(missingFromA) != 2 {
		t.Errorf("Expected 2 entries missing from A (6, 7), got %d: %v", len(missingFromA), missingFromA)
	}

	// Test: Find entries in C that are missing from A
	var missingFromAinC []types.NeedleId
	replicaC.AscendingVisit(func(nv needle_map.NeedleValue) error {
		if _, found := replicaA.Get(nv.Key); !found {
			missingFromAinC = append(missingFromAinC, nv.Key)
		}
		return nil
	})

	if len(missingFromAinC) != 1 {
		t.Errorf("Expected 1 entry missing from A in C (8), got %d: %v", len(missingFromAinC), missingFromAinC)
	}

	// Simulate building union: add missing entries to A
	for _, id := range missingFromA {
		replicaA.Set(id, types.Offset{}, types.Size(100))
	}
	for _, id := range missingFromAinC {
		replicaA.Set(id, types.Offset{}, types.Size(100))
	}

	// Verify A now has all 8 unique entries
	count := 0
	replicaA.AscendingVisit(func(nv needle_map.NeedleValue) error {
		count++
		return nil
	})

	if count != 8 {
		t.Errorf("Expected union to have 8 entries, got %d", count)
	}
}

func TestFindLargestReplica(t *testing.T) {
	// Test that we correctly identify the replica with the most entries

	type replicaInfo struct {
		url       string
		fileCount uint64
	}

	testCases := []struct {
		name     string
		replicas []replicaInfo
		expected string
	}{
		{
			name: "single replica",
			replicas: []replicaInfo{
				{"server1:8080", 100},
			},
			expected: "server1:8080",
		},
		{
			name: "first is largest",
			replicas: []replicaInfo{
				{"server1:8080", 100},
				{"server2:8080", 50},
				{"server3:8080", 75},
			},
			expected: "server1:8080",
		},
		{
			name: "last is largest",
			replicas: []replicaInfo{
				{"server1:8080", 50},
				{"server2:8080", 75},
				{"server3:8080", 100},
			},
			expected: "server3:8080",
		},
		{
			name: "middle is largest",
			replicas: []replicaInfo{
				{"server1:8080", 50},
				{"server2:8080", 100},
				{"server3:8080", 75},
			},
			expected: "server2:8080",
		},
		{
			name: "all equal - pick first",
			replicas: []replicaInfo{
				{"server1:8080", 100},
				{"server2:8080", 100},
				{"server3:8080", 100},
			},
			expected: "server1:8080",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Find the largest
			bestIdx := 0
			var bestCount uint64 = 0
			for i, r := range tc.replicas {
				if i == 0 || r.fileCount > bestCount {
					bestIdx = i
					bestCount = r.fileCount
				}
			}

			if tc.replicas[bestIdx].url != tc.expected {
				t.Errorf("Expected %s, got %s", tc.expected, tc.replicas[bestIdx].url)
			}
		})
	}
}

func TestDeletedEntriesAreSkipped(t *testing.T) {
	// Test that deleted entries are not copied during sync

	replicaA := needle_map.NewMemDb()
	replicaB := needle_map.NewMemDb()
	defer replicaA.Close()
	defer replicaB.Close()

	// Replica A has entries 1, 2, 3 (all valid)
	replicaA.Set(types.NeedleId(1), types.Offset{}, types.Size(100))
	replicaA.Set(types.NeedleId(2), types.Offset{}, types.Size(100))
	replicaA.Set(types.NeedleId(3), types.Offset{}, types.Size(100))

	// Replica B has entry 4 valid, entry 5 deleted
	replicaB.Set(types.NeedleId(4), types.Offset{}, types.Size(100))
	replicaB.Set(types.NeedleId(5), types.Offset{}, types.Size(-1)) // Deleted (negative size)

	// Find non-deleted entries in B missing from A
	var missingFromA []types.NeedleId
	replicaB.AscendingVisit(func(nv needle_map.NeedleValue) error {
		if nv.Size.IsDeleted() {
			return nil // Skip deleted
		}
		if _, found := replicaA.Get(nv.Key); !found {
			missingFromA = append(missingFromA, nv.Key)
		}
		return nil
	})

	if len(missingFromA) != 1 {
		t.Errorf("Expected 1 non-deleted entry missing (4), got %d: %v", len(missingFromA), missingFromA)
	}

	if len(missingFromA) > 0 && missingFromA[0] != types.NeedleId(4) {
		t.Errorf("Expected missing entry to be 4, got %d", missingFromA[0])
	}
}

func TestReplicaUnionBuilder_EmptyLocations(t *testing.T) {
	// Test handling of empty locations slice
	builder := &replicaUnionBuilder{
		writer: &bytes.Buffer{},
		vid:    1,
	}

	_, count, err := builder.buildUnionReplica(nil, "")
	if err == nil {
		t.Error("Expected error for empty locations")
	}
	if count != 0 {
		t.Errorf("Expected 0 synced, got %d", count)
	}
}

func TestAvoidDuplicateCopies(t *testing.T) {
	// Test that when building union, we don't copy the same entry multiple times
	// by updating the best replica's in-memory index after each copy

	bestDB := needle_map.NewMemDb()
	defer bestDB.Close()

	// Best replica has entries 1, 2
	bestDB.Set(types.NeedleId(1), types.Offset{}, types.Size(100))
	bestDB.Set(types.NeedleId(2), types.Offset{}, types.Size(100))

	// Simulate two other replicas both having entry 3
	otherReplicas := [][]types.NeedleId{
		{3, 4}, // Replica B has 3, 4
		{3, 5}, // Replica C has 3, 5
	}

	copiedEntries := make(map[types.NeedleId]int) // Track how many times each entry is "copied"

	for _, otherEntries := range otherReplicas {
		for _, id := range otherEntries {
			if _, found := bestDB.Get(id); !found {
				// Would copy this entry
				copiedEntries[id]++
				// Add to bestDB to prevent duplicate copies
				bestDB.Set(id, types.Offset{}, types.Size(100))
			}
		}
	}

	// Entry 3 should only be copied once (from first replica that has it)
	if copiedEntries[types.NeedleId(3)] != 1 {
		t.Errorf("Entry 3 should be copied exactly once, got %d", copiedEntries[types.NeedleId(3)])
	}

	// Entry 4 should be copied once
	if copiedEntries[types.NeedleId(4)] != 1 {
		t.Errorf("Entry 4 should be copied exactly once, got %d", copiedEntries[types.NeedleId(4)])
	}

	// Entry 5 should be copied once
	if copiedEntries[types.NeedleId(5)] != 1 {
		t.Errorf("Entry 5 should be copied exactly once, got %d", copiedEntries[types.NeedleId(5)])
	}

	// Best should now have 5 entries total
	count := 0
	bestDB.AscendingVisit(func(nv needle_map.NeedleValue) error {
		count++
		return nil
	})
	if count != 5 {
		t.Errorf("Expected 5 entries in union, got %d", count)
	}
}
