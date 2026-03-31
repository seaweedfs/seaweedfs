package lock_manager

import (
	"fmt"
	"math"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/stretchr/testify/assert"
)

func TestHashRing_BasicOperations(t *testing.T) {
	hr := NewHashRing(50)

	// Empty ring
	p, b := hr.GetPrimaryAndBackup("key1")
	assert.Equal(t, pb.ServerAddress(""), p)
	assert.Equal(t, pb.ServerAddress(""), b)

	// Single server — no backup
	hr.AddServer("filer1:8888")
	p, b = hr.GetPrimaryAndBackup("key1")
	assert.Equal(t, pb.ServerAddress("filer1:8888"), p)
	assert.Equal(t, pb.ServerAddress(""), b)

	// Two servers — backup is the other server
	hr.AddServer("filer2:8888")
	p, b = hr.GetPrimaryAndBackup("key1")
	assert.NotEqual(t, p, b)
	assert.NotEmpty(t, b)

	// Three servers
	hr.AddServer("filer3:8888")
	p, b = hr.GetPrimaryAndBackup("key1")
	assert.NotEqual(t, p, b)
	assert.NotEmpty(t, b)

	// Remove server
	hr.RemoveServer("filer2:8888")
	assert.Equal(t, 2, hr.ServerCount())
}

func TestHashRing_DuplicateAddRemove(t *testing.T) {
	hr := NewHashRing(50)

	hr.AddServer("filer1:8888")
	hr.AddServer("filer1:8888") // duplicate
	assert.Equal(t, 1, hr.ServerCount())

	hr.RemoveServer("filer1:8888")
	assert.Equal(t, 0, hr.ServerCount())

	hr.RemoveServer("filer1:8888") // remove non-existent
	assert.Equal(t, 0, hr.ServerCount())
}

func TestHashRing_SetServers(t *testing.T) {
	hr := NewHashRing(50)

	hr.SetServers([]pb.ServerAddress{"a:1", "b:2", "c:3"})
	assert.Equal(t, 3, hr.ServerCount())

	servers := hr.GetServers()
	assert.Equal(t, 3, len(servers))

	// SetServers replaces
	hr.SetServers([]pb.ServerAddress{"x:1", "y:2"})
	assert.Equal(t, 2, hr.ServerCount())
}

func TestHashRing_ConsistencyOnRemoval(t *testing.T) {
	// The key property of consistent hashing: when a server is removed,
	// only keys that mapped to the removed server change.
	hr := NewHashRing(50)
	servers := []pb.ServerAddress{"filer1:8888", "filer2:8888", "filer3:8888"}
	hr.SetServers(servers)

	numKeys := 1000
	// Record where each key maps before removal
	before := make(map[string]pb.ServerAddress, numKeys)
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("lock-key-%d", i)
		before[key] = hr.GetPrimary(key)
	}

	// Remove filer2
	hr.RemoveServer("filer2:8888")

	moved := 0
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("lock-key-%d", i)
		after := hr.GetPrimary(key)
		if before[key] != after {
			// Only keys from filer2 should move
			assert.Equal(t, pb.ServerAddress("filer2:8888"), before[key],
				"key %s moved from %s to %s, but it wasn't on the removed server", key, before[key], after)
			moved++
		}
	}
	// Roughly 1/3 of keys should move (those that were on filer2)
	t.Logf("Keys that moved: %d / %d", moved, numKeys)
	assert.Greater(t, moved, 0, "some keys should have moved")
	assert.Less(t, moved, numKeys, "not all keys should move")
}

func TestHashRing_BackupIsSuccessor(t *testing.T) {
	// After removing primary, the backup should become the new primary
	hr := NewHashRing(50)
	servers := []pb.ServerAddress{"filer1:8888", "filer2:8888", "filer3:8888"}
	hr.SetServers(servers)

	// For each key, verify that removing the primary makes the backup the new primary
	promoted := 0
	total := 500
	for i := 0; i < total; i++ {
		key := fmt.Sprintf("test-lock-%d", i)
		primary, backup := hr.GetPrimaryAndBackup(key)
		assert.NotEqual(t, primary, backup)

		// Temporarily remove primary
		hr.RemoveServer(primary)
		newPrimary := hr.GetPrimary(key)
		if newPrimary == backup {
			promoted++
		}
		// Restore
		hr.AddServer(primary)
	}
	// The backup should become new primary for all keys
	assert.Equal(t, total, promoted,
		"backup should become new primary for all keys when primary is removed")
}

func TestHashRing_Distribution(t *testing.T) {
	hr := NewHashRing(50)
	servers := []pb.ServerAddress{"filer1:8888", "filer2:8888", "filer3:8888"}
	hr.SetServers(servers)

	counts := make(map[pb.ServerAddress]int)
	numKeys := 3000
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("dist-key-%d", i)
		p := hr.GetPrimary(key)
		counts[p]++
	}

	expected := float64(numKeys) / float64(len(servers))
	for server, count := range counts {
		deviation := math.Abs(float64(count)-expected) / expected
		t.Logf("Server %s: %d keys (%.1f%% deviation)", server, count, deviation*100)
		// Allow up to 40% deviation with 50 vnodes and 3 servers
		assert.Less(t, deviation, 0.40,
			"server %s has too many or too few keys: %d (expected ~%d)", server, count, int(expected))
	}
}

func TestHashRing_GetPrimary(t *testing.T) {
	hr := NewHashRing(50)

	// Empty ring
	assert.Equal(t, pb.ServerAddress(""), hr.GetPrimary("key"))

	hr.SetServers([]pb.ServerAddress{"a:1", "b:2"})

	// Deterministic: same key always maps to same server
	p1 := hr.GetPrimary("mykey")
	p2 := hr.GetPrimary("mykey")
	assert.Equal(t, p1, p2)

	// GetPrimary matches the primary from GetPrimaryAndBackup
	primary, _ := hr.GetPrimaryAndBackup("mykey")
	assert.Equal(t, primary, hr.GetPrimary("mykey"))
}
