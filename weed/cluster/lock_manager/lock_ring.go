package lock_manager

import (
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"sort"
	"sync"
	"time"
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
}

func NewLockRing(snapshotInterval time.Duration, onTakeSnapshot func(snapshot []pb.ServerAddress)) *LockRing {
	return &LockRing{
		snapshotInterval: snapshotInterval,
		candidateServers: make(map[pb.ServerAddress]struct{}),
		onTakeSnapshot:   onTakeSnapshot,
	}
}

// AddServer adds a server to the ring
// if the previous snapshot passed the snapshot interval, create a new snapshot
func (r *LockRing) AddServer(server pb.ServerAddress) {
	r.Lock()

	if _, found := r.candidateServers[server]; found {
		r.Unlock()
		return
	}
	r.lastUpdateTime = time.Now()
	r.candidateServers[server] = struct{}{}
	r.Unlock()

	r.takeSnapshotWithDelayedCompaction()
}

func (r *LockRing) RemoveServer(server pb.ServerAddress) {
	r.Lock()

	if _, found := r.candidateServers[server]; !found {
		r.Unlock()
		return
	}
	r.lastUpdateTime = time.Now()
	delete(r.candidateServers, server)
	r.Unlock()

	r.takeSnapshotWithDelayedCompaction()
}

func (r *LockRing) SetSnapshot(servers []pb.ServerAddress) {

	sort.Slice(servers, func(i, j int) bool {
		return servers[i] < servers[j]
	})

	r.lastUpdateTime = time.Now()

	r.addOneSnapshot(servers)

	go func() {
		<-time.After(r.snapshotInterval)
		r.compactSnapshots()
	}()
}

func (r *LockRing) takeSnapshotWithDelayedCompaction() {
	r.doTakeSnapshot()

	go func() {
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

	if r.lastCompactTime.After(r.lastUpdateTime) {
		return
	}

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
