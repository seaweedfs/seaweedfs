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
	lastCompactTime  time.Time
	snapshotInterval time.Duration
	onTakeSnapshot   func(snapshot []pb.ServerAddress)
	cleanupWg        sync.WaitGroup
	Ring             *HashRing // consistent hash ring
	version          int64    // monotonic version from master, rejects stale updates
}

func NewLockRing(snapshotInterval time.Duration) *LockRing {
	return &LockRing{
		snapshotInterval: snapshotInterval,
		Ring:             NewHashRing(DefaultVnodeCount),
	}
}

func (r *LockRing) SetTakeSnapshotCallback(onTakeSnapshot func(snapshot []pb.ServerAddress)) {
	r.Lock()
	defer r.Unlock()
	r.onTakeSnapshot = onTakeSnapshot
}

// SetSnapshot replaces the ring with a new server list from the master.
// The version must be >= the current version, otherwise the update is rejected
// (protects against reordered messages). Version 0 is always accepted (bootstrap).
func (r *LockRing) SetSnapshot(servers []pb.ServerAddress, version int64) bool {

	sort.Slice(servers, func(i, j int) bool {
		return servers[i] < servers[j]
	})

	r.Lock()
	if version > 0 && version < r.version {
		glog.V(0).Infof("LockRing: rejecting stale update v%d (current v%d)", version, r.version)
		r.Unlock()
		return false
	}
	r.version = version
	r.Unlock()

	r.Ring.SetServers(servers)
	r.addOneSnapshot(servers)

	r.cleanupWg.Add(1)
	go func() {
		defer r.cleanupWg.Done()
		<-time.After(r.snapshotInterval)
		r.compactSnapshots()
	}()
	return true
}

// Version returns the current ring version.
func (r *LockRing) Version() int64 {
	r.RLock()
	defer r.RUnlock()
	return r.version
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

	ts := time.Now()
	recentSnapshotIndex := 1
	for ; recentSnapshotIndex < len(r.snapshots); recentSnapshotIndex++ {
		if ts.Sub(r.snapshots[recentSnapshotIndex].ts) > r.snapshotInterval {
			break
		}
	}
	if recentSnapshotIndex+1 <= len(r.snapshots) {
		r.snapshots = r.snapshots[:recentSnapshotIndex+1]
	}
	r.lastCompactTime = ts
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

// hashKeyToServer uses a temporary consistent hash ring for the given server list.
func hashKeyToServer(key string, servers []pb.ServerAddress) pb.ServerAddress {
	if len(servers) == 0 {
		return ""
	}
	ring := NewHashRing(DefaultVnodeCount)
	ring.SetServers(servers)
	return ring.GetPrimary(key)
}
