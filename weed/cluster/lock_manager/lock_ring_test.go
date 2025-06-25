package lock_manager

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/stretchr/testify/assert"
)

func TestAddServer(t *testing.T) {
	r := NewLockRing(100 * time.Millisecond)

	// Add servers
	r.AddServer("localhost:8080")
	r.AddServer("localhost:8081")
	r.AddServer("localhost:8082")
	r.AddServer("localhost:8083")
	r.AddServer("localhost:8084")

	// Verify all servers are present
	servers := r.GetSnapshot()
	assert.Equal(t, 5, len(servers))
	assert.Contains(t, servers, pb.ServerAddress("localhost:8080"))
	assert.Contains(t, servers, pb.ServerAddress("localhost:8081"))
	assert.Contains(t, servers, pb.ServerAddress("localhost:8082"))
	assert.Contains(t, servers, pb.ServerAddress("localhost:8083"))
	assert.Contains(t, servers, pb.ServerAddress("localhost:8084"))

	// Remove servers
	r.RemoveServer("localhost:8084")
	r.RemoveServer("localhost:8082")
	r.RemoveServer("localhost:8080")

	// Wait for all cleanup operations to complete
	r.WaitForCleanup()

	// Verify only 2 servers remain (localhost:8081 and localhost:8083)
	servers = r.GetSnapshot()
	assert.Equal(t, 2, len(servers))
	assert.Contains(t, servers, pb.ServerAddress("localhost:8081"))
	assert.Contains(t, servers, pb.ServerAddress("localhost:8083"))

	// Verify cleanup has happened - wait for snapshot interval and check snapshots are compacted
	time.Sleep(110 * time.Millisecond)
	r.WaitForCleanup()

	// Verify snapshot history is cleaned up properly (should have at most 2 snapshots after compaction)
	snapshotCount := r.GetSnapshotCount()
	assert.LessOrEqual(t, snapshotCount, 2, "Snapshot history should be compacted")
}

func TestLockRing(t *testing.T) {
	r := NewLockRing(100 * time.Millisecond)

	// Test initial snapshot
	r.SetSnapshot([]pb.ServerAddress{"localhost:8080", "localhost:8081"})
	assert.Equal(t, 1, r.GetSnapshotCount())
	servers := r.GetSnapshot()
	assert.Equal(t, 2, len(servers))
	assert.Contains(t, servers, pb.ServerAddress("localhost:8080"))
	assert.Contains(t, servers, pb.ServerAddress("localhost:8081"))

	// Add another server
	r.SetSnapshot([]pb.ServerAddress{"localhost:8080", "localhost:8081", "localhost:8082"})
	assert.Equal(t, 2, r.GetSnapshotCount())
	servers = r.GetSnapshot()
	assert.Equal(t, 3, len(servers))
	assert.Contains(t, servers, pb.ServerAddress("localhost:8082"))

	// Wait for cleanup interval and add another server
	time.Sleep(110 * time.Millisecond)
	r.WaitForCleanup()
	r.SetSnapshot([]pb.ServerAddress{"localhost:8080", "localhost:8081", "localhost:8082", "localhost:8083"})
	assert.LessOrEqual(t, r.GetSnapshotCount(), 3)
	servers = r.GetSnapshot()
	assert.Equal(t, 4, len(servers))
	assert.Contains(t, servers, pb.ServerAddress("localhost:8083"))

	// Wait for cleanup and verify compaction
	time.Sleep(110 * time.Millisecond)
	r.WaitForCleanup()
	assert.LessOrEqual(t, r.GetSnapshotCount(), 2, "Snapshots should be compacted")

	// Add final server
	r.SetSnapshot([]pb.ServerAddress{"localhost:8080", "localhost:8081", "localhost:8082", "localhost:8083", "localhost:8084"})
	servers = r.GetSnapshot()
	assert.Equal(t, 5, len(servers))
	assert.Contains(t, servers, pb.ServerAddress("localhost:8084"))
	assert.LessOrEqual(t, r.GetSnapshotCount(), 3)
}
