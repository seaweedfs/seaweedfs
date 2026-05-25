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
	ring    *HashRing // prebuilt ring for servers, so PriorOwner need not rebuild per call
}

type LockRing struct {
	sync.RWMutex
	snapshots        []*LockRingSnapshot
	lastCompactTime  time.Time
	snapshotInterval time.Duration
	onTakeSnapshot   func(snapshot []pb.ServerAddress)
	cleanupWg        sync.WaitGroup
	Ring             *HashRing // consistent hash ring
	version          int64     // monotonic version from master, rejects stale updates
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
	// Update the ring while holding the lock so version and ring state
	// are always consistent — prevents a concurrent SetSnapshot from
	// seeing the new version but applying its servers to the old ring.
	r.Ring.SetServers(servers)
	// Append the snapshot under the same lock as the ring update so a concurrent
	// PriorOwner always sees snapshots[0] matching r.Ring (and snapshots[1] as the
	// true prior); otherwise it could pair a new ring with a stale prior snapshot.
	r.addOneSnapshotLocked(servers)
	r.Unlock()

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

// addOneSnapshotLocked appends a new snapshot (newest at index 0). The caller
// must hold r.Lock(), so the ring update and snapshot append are one atomic step.
func (r *LockRing) addOneSnapshotLocked(servers []pb.ServerAddress) {
	ts := time.Now()
	ring := NewHashRing(DefaultVnodeCount)
	ring.SetServers(servers)
	t := &LockRingSnapshot{
		servers: servers,
		ts:      ts,
		ring:    ring,
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

// PriorOwner returns the key's owner from the previous ring snapshot, but only
// while the ring changed within the last snapshotInterval and that owner differs
// from the current primary. This is the cooling-off window in which the previous
// owner may still hold locks the new owner has not yet rebuilt — a caller can
// consult it before granting so a fresh owner does not double-grant during a
// rebalance. Returns "" outside the window or when ownership did not move. It
// uses the snapshot's prebuilt ring, so it does not rebuild a hash ring per call.
func (r *LockRing) PriorOwner(key string) pb.ServerAddress {
	r.RLock()
	defer r.RUnlock()
	if len(r.snapshots) < 2 {
		return ""
	}
	if time.Since(r.snapshots[0].ts) > r.snapshotInterval {
		return ""
	}
	current := r.Ring.GetPrimary(key)
	var prior pb.ServerAddress
	if pr := r.snapshots[1].ring; pr != nil {
		prior = pr.GetPrimary(key)
	} else {
		prior = hashKeyToServer(key, r.snapshots[1].servers)
	}
	if prior != "" && prior != current {
		return prior
	}
	return ""
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
