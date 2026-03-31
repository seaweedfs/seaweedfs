package cluster

import (
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

const LockRingStabilizationInterval = 1 * time.Second

// LockRingManager tracks filer membership for the distributed lock ring.
// It batches rapid topology changes (e.g., node drop + join) with a
// stabilization timer, then broadcasts the complete member list atomically
// so filers receive a single consistent ring update instead of multiple
// intermediate states.
type LockRingManager struct {
	mu              sync.Mutex
	members         map[FilerGroupName]map[pb.ServerAddress]struct{}
	version         map[FilerGroupName]int64
	pendingTimer    map[FilerGroupName]*time.Timer
	broadcastFn     func(resp *master_pb.KeepConnectedResponse)
	stabilizeDelay  time.Duration
}

func NewLockRingManager(broadcastFn func(resp *master_pb.KeepConnectedResponse)) *LockRingManager {
	return &LockRingManager{
		members:        make(map[FilerGroupName]map[pb.ServerAddress]struct{}),
		version:        make(map[FilerGroupName]int64),
		pendingTimer:   make(map[FilerGroupName]*time.Timer),
		broadcastFn:    broadcastFn,
		stabilizeDelay: LockRingStabilizationInterval,
	}
}

// AddServer records a filer joining and schedules a batched broadcast.
func (lrm *LockRingManager) AddServer(filerGroup FilerGroupName, address pb.ServerAddress) {
	lrm.mu.Lock()
	defer lrm.mu.Unlock()

	if _, ok := lrm.members[filerGroup]; !ok {
		lrm.members[filerGroup] = make(map[pb.ServerAddress]struct{})
	}
	lrm.members[filerGroup][address] = struct{}{}
	lrm.scheduleBroadcast(filerGroup)
}

// RemoveServer records a filer leaving and schedules a batched broadcast.
func (lrm *LockRingManager) RemoveServer(filerGroup FilerGroupName, address pb.ServerAddress) {
	lrm.mu.Lock()
	defer lrm.mu.Unlock()

	if members, ok := lrm.members[filerGroup]; ok {
		delete(members, address)
	}
	lrm.scheduleBroadcast(filerGroup)
}

// GetServers returns the current member list for a filer group.
func (lrm *LockRingManager) GetServers(filerGroup FilerGroupName) []string {
	lrm.mu.Lock()
	defer lrm.mu.Unlock()

	members, ok := lrm.members[filerGroup]
	if !ok {
		return nil
	}
	servers := make([]string, 0, len(members))
	for addr := range members {
		servers = append(servers, string(addr))
	}
	return servers
}

// GetVersion returns the current version for a filer group.
func (lrm *LockRingManager) GetVersion(filerGroup FilerGroupName) int64 {
	lrm.mu.Lock()
	defer lrm.mu.Unlock()
	return lrm.version[filerGroup]
}

// scheduleBroadcast resets the stabilization timer. If another change arrives
// before the timer fires, the timer resets, batching the changes.
// Caller must hold lrm.mu.
func (lrm *LockRingManager) scheduleBroadcast(filerGroup FilerGroupName) {
	if timer, ok := lrm.pendingTimer[filerGroup]; ok {
		if !timer.Stop() {
			// Timer already fired, callback is running or queued.
			// It will pick up the latest state from lrm.members, so
			// just schedule a new one for any further changes.
		}
	}
	lrm.pendingTimer[filerGroup] = time.AfterFunc(lrm.stabilizeDelay, func() {
		lrm.doBroadcast(filerGroup)
	})
}

func (lrm *LockRingManager) doBroadcast(filerGroup FilerGroupName) {
	lrm.mu.Lock()
	// Use wall-clock nanoseconds so the version survives master restarts
	// without persistence — a restarted master produces a version greater
	// than any pre-restart value (assuming clocks don't jump backward).
	version := time.Now().UnixNano()
	lrm.version[filerGroup] = version
	servers := make([]string, 0)
	if members, ok := lrm.members[filerGroup]; ok {
		for addr := range members {
			servers = append(servers, string(addr))
		}
	}
	delete(lrm.pendingTimer, filerGroup)
	lrm.mu.Unlock()

	glog.V(0).Infof("LockRing: broadcasting ring update for group %q version %d: %v", filerGroup, version, servers)

	if lrm.broadcastFn != nil {
		lrm.broadcastFn(&master_pb.KeepConnectedResponse{
			LockRingUpdate: &master_pb.LockRingUpdate{
				FilerGroup: string(filerGroup),
				Servers:    servers,
				Version:    version,
			},
		})
	}
}

// FlushPending fires any pending timer immediately (for testing or shutdown).
func (lrm *LockRingManager) FlushPending(filerGroup FilerGroupName) {
	lrm.mu.Lock()
	if timer, ok := lrm.pendingTimer[filerGroup]; ok {
		if timer.Stop() {
			// Timer was pending — we stopped it, so we broadcast now
			delete(lrm.pendingTimer, filerGroup)
			lrm.mu.Unlock()
			lrm.doBroadcast(filerGroup)
		} else {
			// Timer already fired, callback is running — let it finish
			lrm.mu.Unlock()
		}
	} else {
		lrm.mu.Unlock()
	}
}
