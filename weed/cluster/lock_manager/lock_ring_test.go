package lock_manager

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/stretchr/testify/assert"
)

func TestLockRing_SetSnapshot(t *testing.T) {
	r := NewLockRing(100 * time.Millisecond)

	// Set 5 servers
	r.SetSnapshot([]pb.ServerAddress{
		"localhost:8080", "localhost:8081", "localhost:8082",
		"localhost:8083", "localhost:8084",
	}, 1)

	servers := r.GetSnapshot()
	assert.Equal(t, 5, len(servers))

	// Replace with 2 servers
	r.SetSnapshot([]pb.ServerAddress{"localhost:8081", "localhost:8083"}, 2)

	r.WaitForCleanup()
	servers = r.GetSnapshot()
	assert.Equal(t, 2, len(servers))
	assert.Contains(t, servers, pb.ServerAddress("localhost:8081"))
	assert.Contains(t, servers, pb.ServerAddress("localhost:8083"))

	// Verify compaction
	time.Sleep(110 * time.Millisecond)
	r.WaitForCleanup()
	assert.LessOrEqual(t, r.GetSnapshotCount(), 2)
}

func TestLockRing_SnapshotCompaction(t *testing.T) {
	r := NewLockRing(100 * time.Millisecond)

	r.SetSnapshot([]pb.ServerAddress{"localhost:8080", "localhost:8081"}, 1)
	assert.Equal(t, 1, r.GetSnapshotCount())

	r.SetSnapshot([]pb.ServerAddress{"localhost:8080", "localhost:8081", "localhost:8082"}, 2)
	assert.Equal(t, 2, r.GetSnapshotCount())

	// Wait for compaction
	time.Sleep(110 * time.Millisecond)
	r.WaitForCleanup()

	r.SetSnapshot([]pb.ServerAddress{"localhost:8080", "localhost:8081", "localhost:8082", "localhost:8083"}, 3)
	assert.LessOrEqual(t, r.GetSnapshotCount(), 3)
	servers := r.GetSnapshot()
	assert.Equal(t, 4, len(servers))

	time.Sleep(110 * time.Millisecond)
	r.WaitForCleanup()
	assert.LessOrEqual(t, r.GetSnapshotCount(), 2, "Snapshots should be compacted")

	r.SetSnapshot([]pb.ServerAddress{
		"localhost:8080", "localhost:8081", "localhost:8082",
		"localhost:8083", "localhost:8084",
	}, 4)
	servers = r.GetSnapshot()
	assert.Equal(t, 5, len(servers))
}

func TestLockRing_VersionRejectsStale(t *testing.T) {
	r := NewLockRing(100 * time.Millisecond)

	// Apply version 3
	ok := r.SetSnapshot([]pb.ServerAddress{"a:1", "b:2", "c:3"}, 3)
	assert.True(t, ok)
	assert.Equal(t, int64(3), r.Version())
	assert.Equal(t, 3, len(r.GetSnapshot()))

	// Stale version 2 — should be rejected
	ok = r.SetSnapshot([]pb.ServerAddress{"x:1"}, 2)
	assert.False(t, ok)
	assert.Equal(t, int64(3), r.Version())
	assert.Equal(t, 3, len(r.GetSnapshot()), "stale update should not change the ring")

	// Same version 3 — accepted (SetSnapshot accepts version >= current, state-changing)
	ok = r.SetSnapshot([]pb.ServerAddress{"a:1", "b:2"}, 3)
	assert.True(t, ok)
	assert.Equal(t, 2, len(r.GetSnapshot()))

	// Newer version 5 — should be accepted
	ok = r.SetSnapshot([]pb.ServerAddress{"d:1", "e:2", "f:3", "g:4"}, 5)
	assert.True(t, ok)
	assert.Equal(t, int64(5), r.Version())
	assert.Equal(t, 4, len(r.GetSnapshot()))

	// Version 0 always accepted (bootstrap)
	ok = r.SetSnapshot([]pb.ServerAddress{"z:1"}, 0)
	assert.True(t, ok)
	assert.Equal(t, 1, len(r.GetSnapshot()))
}
