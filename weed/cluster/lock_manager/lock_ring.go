package lock_manager

import (
	"sort"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
)

type LockRingSnapshot struct {
	servers []pb.ServerAddress
	ts      time.Time
}

type LockRing struct {
	sync.RWMutex
	snapshots        []*LockRingSnapshot
	candidateServers map[pb.ServerAddress]struct{}
	lastUpdateTime   time.Time
	lastCompactTime  time.Time
	snapshotInterval time.Duration
	onTakeSnapshot   func(snapshot []pb.ServerAddress)
	cleanupWg        sync.WaitGroup
	Ring             *HashRing // consistent hash ring
}

func NewLockRing(snapshotInterval time.Duration) *LockRing {
	return &LockRing{
		snapshotInterval: snapshotInterval,
		candidateServers: make(map[pb.ServerAddress]struct{}),
		Ring:             NewHashRing(DefaultVnodeCount),
	}
}

func (r *LockRing) SetTakeSnapshotCallback(onTakeSnapshot func(snapshot []pb.ServerAddress)) {
	r.Lock()
	defer r.Unlock()
	r.onTakeSnapshot = onTakeSnapshot
}

// AddServer adds a server to the ring
// if the previous snapshot passed the snapshot interval, create a new snapshot
func (r *LockRing) AddServer(server pb.ServerAddress) {
	glog.V(0).Infof("add server %v", server)
	r.Lock()

	if _, found := r.candidateServers[server]; found {
		glog.V(0).Infof("add server: already exists %v", server)
		r.Unlock()
		return
	}
	r.lastUpdateTime = time.Now()
	r.candidateServers[server] = struct{}{}
	r.Unlock()

	r.Ring.AddServer(server)
	r.takeSnapshotWithDelayedCompaction()
}

func (r *LockRing) RemoveServer(server pb.ServerAddress) {
	glog.V(0).Infof("remove server %v", server)

	r.Lock()

	if _, found := r.candidateServers[server]; !found {
		r.Unlock()
		return
	}
	r.lastUpdateTime = time.Now()
	delete(r.candidateServers, server)
	r.Unlock()

	r.Ring.RemoveServer(server)
	r.takeSnapshotWithDelayedCompaction()
}

func (r *LockRing) SetSnapshot(servers []pb.ServerAddress) {

	sort.Slice(servers, func(i, j int) bool {
		return servers[i] < servers[j]
	})

	r.Lock()
	r.lastUpdateTime = time.Now()
	// init candidateServers
	for _, server := range servers {
		r.candidateServers[server] = struct{}{}
	}
	r.Unlock()

	r.Ring.SetServers(servers)
	r.addOneSnapshot(servers)

	r.cleanupWg.Add(1)
	go func() {
		defer r.cleanupWg.Done()
		<-time.After(r.snapshotInterval)
		r.compactSnapshots()
	}()
}

func (r *LockRing) takeSnapshotWithDelayedCompaction() {
	r.doTakeSnapshot()

	r.cleanupWg.Add(1)
	go func() {
		defer r.cleanupWg.Done()
		<-time.After(r.snapshotInterval)
		r.compactSnapshots()
	}()
}

func (r *LockRing) doTakeSnapshot() {
	servers := r.getSortedServers()

	r.addOneSnapshot(servers)
}

func (r *LockRing) addOneSnapshot(servers []pb.ServerAddress) {
	r.Lock()
	defer r.Unlock()

	ts := time.Now()
	t := &LockRingSnapshot{
		servers: servers,
		ts:      ts,
	}
	r.snapshots = append(r.snapshots, t)
	for i := len(r.snapshots) - 2; i >= 0; i-- {
		r.snapshots[i+1] = r.snapshots[i]
	}
	r.snapshots[0] = t

	if r.onTakeSnapshot != nil {
		r.onTakeSnapshot(t.servers)
	}
}

func (r *LockRing) compactSnapshots() {
	r.Lock()
	defer r.Unlock()

	// Always attempt compaction when called, regardless of lastCompactTime
	// This ensures proper cleanup even with multiple concurrent compaction requests

	ts := time.Now()
	// remove old snapshots
	recentSnapshotIndex := 1
	for ; recentSnapshotIndex < len(r.snapshots); recentSnapshotIndex++ {
		if ts.Sub(r.snapshots[recentSnapshotIndex].ts) > r.snapshotInterval {
			break
		}
	}
	// keep the one that has been running for a while
	if recentSnapshotIndex+1 <= len(r.snapshots) {
		r.snapshots = r.snapshots[:recentSnapshotIndex+1]
	}
	r.lastCompactTime = ts
}

func (r *LockRing) getSortedServers() []pb.ServerAddress {
	sortedServers := make([]pb.ServerAddress, 0, len(r.candidateServers))
	for server := range r.candidateServers {
		sortedServers = append(sortedServers, server)
	}
	sort.Slice(sortedServers, func(i, j int) bool {
		return sortedServers[i] < sortedServers[j]
	})
	return sortedServers
}

func (r *LockRing) GetSnapshot() (servers []pb.ServerAddress) {
	r.RLock()
	defer r.RUnlock()

	if len(r.snapshots) == 0 {
		return
	}
	return r.snapshots[0].servers
}

// WaitForCleanup waits for all pending cleanup operations to complete
// This is useful for testing to ensure deterministic behavior
func (r *LockRing) WaitForCleanup() {
	r.cleanupWg.Wait()
}

// GetSnapshotCount safely returns the number of snapshots for testing
func (r *LockRing) GetSnapshotCount() int {
	r.RLock()
	defer r.RUnlock()
	return len(r.snapshots)
}

// GetPrimaryAndBackup returns the primary and backup servers for a key
// using the consistent hash ring.
func (r *LockRing) GetPrimaryAndBackup(key string) (primary, backup pb.ServerAddress) {
	return r.Ring.GetPrimaryAndBackup(key)
}

// GetPrimary returns the primary server for a key using the consistent hash ring.
func (r *LockRing) GetPrimary(key string) pb.ServerAddress {
	return r.Ring.GetPrimary(key)
}

// hashKeyToServer is kept for backward compatibility but now uses consistent hashing.
func hashKeyToServer(key string, servers []pb.ServerAddress) pb.ServerAddress {
	if len(servers) == 0 {
		return ""
	}
	// Build a temporary ring for the given servers
	// This is used by SelectNotOwnedLocks and CalculateTargetServer
	// which pass an explicit server list
	ring := NewHashRing(DefaultVnodeCount)
	ring.SetServers(servers)
	return ring.GetPrimary(key)
}
