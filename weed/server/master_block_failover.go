package weed_server

import (
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// pendingRebuild records a volume that needs rebuild when a dead VS reconnects.
type pendingRebuild struct {
	VolumeName string
	OldPath    string // path on dead server
	NewPrimary string // promoted replica server
	Epoch      uint64
}

// blockFailoverState holds failover and rebuild state on the master.
type blockFailoverState struct {
	mu              sync.Mutex
	pendingRebuilds map[string][]pendingRebuild // dead server addr -> pending rebuilds
	// R2-F2: Track deferred promotion timers so they can be cancelled on reconnect.
	deferredTimers map[string][]*time.Timer // dead server addr -> pending timers
}

func newBlockFailoverState() *blockFailoverState {
	return &blockFailoverState{
		pendingRebuilds: make(map[string][]pendingRebuild),
		deferredTimers:  make(map[string][]*time.Timer),
	}
}

// failoverBlockVolumes is called when a volume server disconnects.
// It checks each block volume on that server and:
// - If dead server is primary: promote best replica (if lease expired).
// - If dead server hosts a replica: remove from replica list, record pending rebuild.
func (ms *MasterServer) failoverBlockVolumes(deadServer string) {
	if ms.blockRegistry == nil {
		return
	}
	ms.blockRegistry.FailoversTotal.Add(1)
	entries := ms.blockRegistry.ListByServer(deadServer)
	now := time.Now()
	for _, entry := range entries {
		// Case 1: Dead server is the primary.
		if entry.VolumeServer == deadServer &&
			blockvol.RoleFromWire(entry.Role) == blockvol.RolePrimary {

			if !entry.HasReplica() {
				glog.Warningf("failover: %q has no replica, cannot promote", entry.Name)
				continue
			}
			// F2: Wait for lease expiry before promoting.
			leaseExpiry := entry.LastLeaseGrant.Add(entry.LeaseTTL)
			if now.Before(leaseExpiry) {
				delay := leaseExpiry.Sub(now)
				glog.V(0).Infof("failover: %q lease expires in %v, deferring promotion", entry.Name, delay)
				volumeName := entry.Name
				timer := time.AfterFunc(delay, func() {
					ms.promoteReplica(volumeName)
				})
				ms.blockFailover.mu.Lock()
				ms.blockFailover.deferredTimers[deadServer] = append(
					ms.blockFailover.deferredTimers[deadServer], timer)
				ms.blockFailover.mu.Unlock()
				continue
			}
			// Lease already expired — promote immediately.
			ms.promoteReplica(entry.Name)
			continue
		}

		// Case 2: Dead server hosts a replica (not the primary).
		if entry.VolumeServer != deadServer {
			ri := entry.ReplicaByServer(deadServer)
			if ri != nil {
				replicaPath := ri.Path
				// Remove dead replica from registry.
				if err := ms.blockRegistry.RemoveReplica(entry.Name, deadServer); err != nil {
					glog.Warningf("failover: RemoveReplica %q on %s: %v", entry.Name, deadServer, err)
					continue
				}
				// Record pending rebuild for when dead server reconnects.
				ms.recordPendingRebuild(deadServer, pendingRebuild{
					VolumeName: entry.Name,
					OldPath:    replicaPath,
					NewPrimary: entry.VolumeServer, // current primary (unchanged)
					Epoch:      entry.Epoch,
				})
				glog.V(0).Infof("failover: removed dead replica %s for %q, pending rebuild",
					deadServer, entry.Name)
			}
		}
	}
}

// promoteReplica promotes the best replica to primary for the named volume,
// enqueues an assignment for the new primary, and records a pending rebuild.
func (ms *MasterServer) promoteReplica(volumeName string) {
	entry, ok := ms.blockRegistry.Lookup(volumeName)
	if !ok {
		return
	}
	if !entry.HasReplica() {
		return
	}

	oldPrimary := entry.VolumeServer
	oldPath := entry.Path

	// CP8-2: Use PromoteBestReplica (picks by health score, tie-break by WALHeadLSN).
	newEpoch, err := ms.blockRegistry.PromoteBestReplica(volumeName)
	if err != nil {
		glog.Warningf("failover: PromoteBestReplica %q: %v", volumeName, err)
		return
	}

	// Re-read entry after promotion.
	entry, ok = ms.blockRegistry.Lookup(volumeName)
	if !ok {
		return
	}

	// Build assignment for new primary. Include ReplicaAddrs for remaining replicas.
	leaseTTLMs := blockvol.LeaseTTLToWire(30 * time.Second)
	assignment := blockvol.BlockVolumeAssignment{
		Path:       entry.Path,
		Epoch:      newEpoch,
		Role:       blockvol.RoleToWire(blockvol.RolePrimary),
		LeaseTtlMs: leaseTTLMs,
	}
	for _, ri := range entry.Replicas {
		assignment.ReplicaAddrs = append(assignment.ReplicaAddrs, blockvol.ReplicaAddr{
			DataAddr: ri.DataAddr,
			CtrlAddr: ri.CtrlAddr,
		})
	}
	// Backward compat: also set scalar fields if exactly 1 replica.
	if len(entry.Replicas) == 1 {
		assignment.ReplicaDataAddr = entry.Replicas[0].DataAddr
		assignment.ReplicaCtrlAddr = entry.Replicas[0].CtrlAddr
	}
	ms.blockAssignmentQueue.Enqueue(entry.VolumeServer, assignment)

	// Record pending rebuild for when dead server reconnects.
	ms.recordPendingRebuild(oldPrimary, pendingRebuild{
		VolumeName: volumeName,
		OldPath:    oldPath,
		NewPrimary: entry.VolumeServer,
		Epoch:      newEpoch,
	})

	ms.blockRegistry.PromotionsTotal.Add(1)
	glog.V(0).Infof("failover: promoted replica for %q: new primary=%s epoch=%d (old primary=%s)",
		volumeName, entry.VolumeServer, newEpoch, oldPrimary)
}

// recordPendingRebuild stores a pending rebuild for a dead server.
func (ms *MasterServer) recordPendingRebuild(deadServer string, rb pendingRebuild) {
	if ms.blockFailover == nil {
		return
	}
	ms.blockFailover.mu.Lock()
	defer ms.blockFailover.mu.Unlock()
	ms.blockFailover.pendingRebuilds[deadServer] = append(ms.blockFailover.pendingRebuilds[deadServer], rb)
}

// drainPendingRebuilds returns and clears pending rebuilds for a server.
func (ms *MasterServer) drainPendingRebuilds(server string) []pendingRebuild {
	if ms.blockFailover == nil {
		return nil
	}
	ms.blockFailover.mu.Lock()
	defer ms.blockFailover.mu.Unlock()
	rebuilds := ms.blockFailover.pendingRebuilds[server]
	delete(ms.blockFailover.pendingRebuilds, server)
	return rebuilds
}

// cancelDeferredTimers stops all deferred promotion timers for a server (R2-F2).
// Called when a VS reconnects before its lease-deferred timers fire, preventing split-brain.
func (ms *MasterServer) cancelDeferredTimers(server string) {
	if ms.blockFailover == nil {
		return
	}
	ms.blockFailover.mu.Lock()
	timers := ms.blockFailover.deferredTimers[server]
	delete(ms.blockFailover.deferredTimers, server)
	ms.blockFailover.mu.Unlock()
	for _, t := range timers {
		t.Stop()
	}
	if len(timers) > 0 {
		glog.V(0).Infof("failover: cancelled %d deferred promotion timers for reconnected %s", len(timers), server)
	}
}

// recoverBlockVolumes is called when a previously dead VS reconnects.
// It cancels any deferred promotion timers (R2-F2), drains pending rebuilds,
// and enqueues rebuild assignments.
func (ms *MasterServer) recoverBlockVolumes(reconnectedServer string) {
	// R2-F2: Cancel deferred promotion timers for this server to prevent split-brain.
	ms.cancelDeferredTimers(reconnectedServer)

	rebuilds := ms.drainPendingRebuilds(reconnectedServer)
	if len(rebuilds) == 0 {
		return
	}

	for _, rb := range rebuilds {
		entry, ok := ms.blockRegistry.Lookup(rb.VolumeName)
		if !ok {
			glog.V(0).Infof("rebuild: volume %q deleted while %s was down, skipping", rb.VolumeName, reconnectedServer)
			continue
		}

		// Update registry: reconnected server becomes a replica (via AddReplica for RF≥2 support).
		ms.blockRegistry.AddReplica(rb.VolumeName, ReplicaInfo{
			Server: reconnectedServer,
			Path:   rb.OldPath,
		})

		// Enqueue rebuild assignment for the reconnected server.
		ms.blockAssignmentQueue.Enqueue(reconnectedServer, blockvol.BlockVolumeAssignment{
			Path:        rb.OldPath,
			Epoch:       entry.Epoch,
			Role:        blockvol.RoleToWire(blockvol.RoleRebuilding),
			RebuildAddr: entry.RebuildListenAddr,
		})

		ms.blockRegistry.RebuildsTotal.Add(1)
		glog.V(0).Infof("rebuild: enqueued rebuild for %q on %s (epoch=%d, rebuildAddr=%s)",
			rb.VolumeName, reconnectedServer, entry.Epoch, entry.RebuildListenAddr)
	}
}
