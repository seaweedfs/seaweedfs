package cluster

import (
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLockRingManager_BatchesRapidChanges(t *testing.T) {
	var mu sync.Mutex
	var broadcasts []*master_pb.LockRingUpdate

	lrm := NewLockRingManager(func(resp *master_pb.KeepConnectedResponse) {
		mu.Lock()
		if resp.LockRingUpdate != nil {
			broadcasts = append(broadcasts, resp.LockRingUpdate)
		}
		mu.Unlock()
	})
	lrm.stabilizeDelay = 100 * time.Millisecond

	group := FilerGroupName("default")

	// Add 3 servers in rapid succession
	lrm.AddServer(group, "filer1:8888")
	lrm.AddServer(group, "filer2:8888")
	lrm.AddServer(group, "filer3:8888")

	// No broadcast should have happened yet (timer hasn't fired)
	mu.Lock()
	assert.Equal(t, 0, len(broadcasts), "should not broadcast before stabilization delay")
	mu.Unlock()

	// Wait for stabilization
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	require.Equal(t, 1, len(broadcasts), "should batch into a single broadcast")
	assert.Equal(t, 3, len(broadcasts[0].Servers), "should include all 3 servers")
	assert.Greater(t, broadcasts[0].Version, int64(0))
	mu.Unlock()
}

func TestLockRingManager_DropAndJoinBatched(t *testing.T) {
	var mu sync.Mutex
	var broadcasts []*master_pb.LockRingUpdate

	lrm := NewLockRingManager(func(resp *master_pb.KeepConnectedResponse) {
		mu.Lock()
		if resp.LockRingUpdate != nil {
			broadcasts = append(broadcasts, resp.LockRingUpdate)
		}
		mu.Unlock()
	})
	lrm.stabilizeDelay = 100 * time.Millisecond

	group := FilerGroupName("default")

	// Set up initial state
	lrm.AddServer(group, "filer1:8888")
	lrm.AddServer(group, "filer2:8888")
	lrm.AddServer(group, "filer3:8888")
	lrm.FlushPending(group)

	mu.Lock()
	broadcasts = nil // reset
	mu.Unlock()

	// Simulate drop + join in rapid succession
	lrm.RemoveServer(group, "filer3:8888")
	lrm.AddServer(group, "filer4:8888")

	// Should not have broadcast yet
	mu.Lock()
	assert.Equal(t, 0, len(broadcasts))
	mu.Unlock()

	// Wait for stabilization
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	require.Equal(t, 1, len(broadcasts), "drop+join should be batched into single broadcast")
	servers := broadcasts[0].Servers
	assert.Equal(t, 3, len(servers), "should have filer1, filer2, filer4")
	// filer3 should be gone, filer4 should be present
	serverSet := make(map[string]bool)
	for _, s := range servers {
		serverSet[s] = true
	}
	assert.False(t, serverSet["filer3:8888"], "filer3 should be removed")
	assert.True(t, serverSet["filer4:8888"], "filer4 should be added")
	mu.Unlock()
}

func TestLockRingManager_VersionIncrements(t *testing.T) {
	var mu sync.Mutex
	var broadcasts []*master_pb.LockRingUpdate

	lrm := NewLockRingManager(func(resp *master_pb.KeepConnectedResponse) {
		mu.Lock()
		if resp.LockRingUpdate != nil {
			broadcasts = append(broadcasts, resp.LockRingUpdate)
		}
		mu.Unlock()
	})
	lrm.stabilizeDelay = 50 * time.Millisecond

	group := FilerGroupName("default")

	lrm.AddServer(group, "filer1:8888")
	time.Sleep(100 * time.Millisecond)

	lrm.AddServer(group, "filer2:8888")
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	require.Equal(t, 2, len(broadcasts))
	assert.Greater(t, broadcasts[0].Version, int64(0), "version should be positive")
	assert.Greater(t, broadcasts[1].Version, broadcasts[0].Version, "versions should be monotonically increasing")
	mu.Unlock()
}

func TestLockRingManager_FlushPending(t *testing.T) {
	var mu sync.Mutex
	var broadcasts []*master_pb.LockRingUpdate

	lrm := NewLockRingManager(func(resp *master_pb.KeepConnectedResponse) {
		mu.Lock()
		if resp.LockRingUpdate != nil {
			broadcasts = append(broadcasts, resp.LockRingUpdate)
		}
		mu.Unlock()
	})
	lrm.stabilizeDelay = 10 * time.Second // long delay

	group := FilerGroupName("default")

	lrm.AddServer(group, "filer1:8888")
	lrm.AddServer(group, "filer2:8888")

	// Flush immediately
	lrm.FlushPending(group)

	mu.Lock()
	require.Equal(t, 1, len(broadcasts))
	assert.Equal(t, 2, len(broadcasts[0].Servers))
	mu.Unlock()
}

func TestLockRingManager_MultipleGroups(t *testing.T) {
	var mu sync.Mutex
	broadcastsByGroup := make(map[string][]*master_pb.LockRingUpdate)

	lrm := NewLockRingManager(func(resp *master_pb.KeepConnectedResponse) {
		mu.Lock()
		if resp.LockRingUpdate != nil {
			broadcastsByGroup[resp.LockRingUpdate.FilerGroup] = append(
				broadcastsByGroup[resp.LockRingUpdate.FilerGroup], resp.LockRingUpdate)
		}
		mu.Unlock()
	})
	lrm.stabilizeDelay = 50 * time.Millisecond

	lrm.AddServer("group1", "filer1:8888")
	lrm.AddServer("group2", "filer2:8888")

	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	assert.Equal(t, 1, len(broadcastsByGroup["group1"]))
	assert.Equal(t, 1, len(broadcastsByGroup["group2"]))
	assert.Equal(t, []string{"filer1:8888"}, broadcastsByGroup["group1"][0].Servers)
	assert.Equal(t, []string{"filer2:8888"}, broadcastsByGroup["group2"][0].Servers)
	mu.Unlock()
}

func TestLockRingManager_GetServers(t *testing.T) {
	lrm := NewLockRingManager(nil)

	group := FilerGroupName("default")
	lrm.AddServer(group, "filer1:8888")
	lrm.AddServer(group, "filer2:8888")

	servers := lrm.GetServers(group)
	assert.Equal(t, 2, len(servers))

	// Contains both
	serverSet := make(map[string]bool)
	for _, s := range servers {
		serverSet[s] = true
	}
	assert.True(t, serverSet["filer1:8888"])
	assert.True(t, serverSet["filer2:8888"])

	// Remove one
	lrm.RemoveServer(group, "filer1:8888")
	servers = lrm.GetServers(group)
	assert.Equal(t, 1, len(servers))
	assert.Equal(t, "filer2:8888", servers[0])
}

func TestLockRingManager_NoBroadcastWithoutFn(t *testing.T) {
	// No panic when broadcastFn is nil
	lrm := NewLockRingManager(nil)
	lrm.stabilizeDelay = 10 * time.Millisecond

	lrm.AddServer("default", pb.ServerAddress("filer1:8888"))
	time.Sleep(50 * time.Millisecond) // should not panic
}
