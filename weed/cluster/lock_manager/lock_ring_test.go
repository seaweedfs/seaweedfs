package lock_manager

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/stretchr/testify/assert"
)

func TestAddServer(t *testing.T) {
	r := NewLockRing(100 * time.Millisecond)
	r.AddServer("localhost:8080")
	assert.Equal(t, 1, len(r.snapshots))
	r.AddServer("localhost:8081")
	r.AddServer("localhost:8082")
	r.AddServer("localhost:8083")
	r.AddServer("localhost:8084")
	r.RemoveServer("localhost:8084")
	r.RemoveServer("localhost:8082")
	r.RemoveServer("localhost:8080")

	assert.Equal(t, 8, len(r.snapshots))

	// Wait for all cleanup operations to complete instead of using time.Sleep
	time.Sleep(110 * time.Millisecond) // Still need to wait for the cleanup interval
	r.WaitForCleanup()                 // Ensure all cleanup goroutines have finished

	assert.Equal(t, 2, len(r.snapshots))

}

func TestLockRing(t *testing.T) {
	r := NewLockRing(100 * time.Millisecond)
	r.SetSnapshot([]pb.ServerAddress{"localhost:8080", "localhost:8081"})
	assert.Equal(t, 1, len(r.snapshots))
	r.SetSnapshot([]pb.ServerAddress{"localhost:8080", "localhost:8081", "localhost:8082"})
	assert.Equal(t, 2, len(r.snapshots))
	time.Sleep(110 * time.Millisecond)
	r.SetSnapshot([]pb.ServerAddress{"localhost:8080", "localhost:8081", "localhost:8082", "localhost:8083"})
	assert.Equal(t, 3, len(r.snapshots))
	time.Sleep(110 * time.Millisecond)
	assert.Equal(t, 2, len(r.snapshots))
	r.SetSnapshot([]pb.ServerAddress{"localhost:8080", "localhost:8081", "localhost:8082", "localhost:8083", "localhost:8084"})
	assert.Equal(t, 3, len(r.snapshots))
}
