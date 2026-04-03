package weed_server

import (
	"context"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// pendingRebuild records a volume that needs rebuild when a dead VS reconnects.
type pendingRebuild struct {
	VolumeName      string
	OldPath         string // path on dead server
	NewPrimary      string // promoted replica server
	Epoch           uint64
	ReplicaDataAddr string // CP13-8: saved from before death for catch-up-first recovery
	ReplicaCtrlAddr string // CP13-8: saved from before death for catch-up-first recovery
}

// deferredPromotion tracks a deferred promotion timer with its volume context.
type deferredPromotion struct {
	Timer          *time.Timer
	VolumeName     string
	CurrentPrimary string // current (stale) primary that will be replaced
	AffectedServer string // dead server addr
}

// blockFailoverState holds failover and rebuild state on the master.
type blockFailoverState struct {
	mu              sync.Mutex
	pendingRebuilds map[string][]pendingRebuild       // dead server addr -> pending rebuilds
	deferredTimers  map[string][]deferredPromotion     // dead server addr -> pending deferred promotions
}

// FailoverVolumeState is one volume's failover diagnosis entry.
type FailoverVolumeState struct {
	VolumeName       string
	CurrentPrimary   string
	AffectedServer   string // dead server that triggered the failover/rebuild
	DeferredPromotion bool  // true if a deferred promotion timer is pending
	PendingRebuild   bool  // true if a rebuild is pending for this volume
	Reason           string // "lease_wait", "rebuild_pending", or ""
}

// FailoverDiagnostic is a bounded read-only snapshot of failover state
// for operator-visible diagnosis. P3 diagnosability surface.
//
// Volume-oriented: each entry describes one volume's failover state.
// Aggregate counts are derived from the volume list.
type FailoverDiagnostic struct {
	Volumes              []FailoverVolumeState
	PendingRebuildCount  map[string]int // dead server → count of pending rebuilds
	DeferredPromotionCount map[string]int // dead server → count of deferred promotion timers
}

func (fs *blockFailoverState) DiagnosticSnapshot() FailoverDiagnostic {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	diag := FailoverDiagnostic{
		PendingRebuildCount:    make(map[string]int),
		DeferredPromotionCount: make(map[string]int),
	}
	for server, rebuilds := range fs.pendingRebuilds {
		diag.PendingRebuildCount[server] = len(rebuilds)
		for _, rb := range rebuilds {
			diag.Volumes = append(diag.Volumes, FailoverVolumeState{
				VolumeName:     rb.VolumeName,
				CurrentPrimary: rb.NewPrimary,
				AffectedServer: server,
				PendingRebuild: true,
				Reason:         "rebuild_pending",
			})
		}
	}
	for server, promos := range fs.deferredTimers {
		diag.DeferredPromotionCount[server] = len(promos)
		for _, dp := range promos {
			diag.Volumes = append(diag.Volumes, FailoverVolumeState{
				VolumeName:        dp.VolumeName,
				CurrentPrimary:    dp.CurrentPrimary,
				AffectedServer:    dp.AffectedServer,
				DeferredPromotion: true,
				Reason:            "lease_wait",
			})
		}
	}
	return diag
}

// PublicationDiagnostic is a bounded read-only snapshot comparing the
// operator-visible publication (LookupBlockVolume response) against the
// registry authority for one volume. P3 diagnosability surface for S2.
type PublicationDiagnostic struct {
	VolumeName           string
	LookupVolumeServer   string // what LookupBlockVolume returns
	LookupIscsiAddr      string
	AuthorityVolumeServer string // registry entry (source of truth)
	AuthorityIscsiAddr   string
	Coherent             bool   // true if lookup == authority
	Reason               string // "" if coherent, otherwise why they diverge
}

// PublicationDiagnosticFor returns a PublicationDiagnostic for the named volume.
// It performs two independent reads:
//   - Lookup side: calls LookupBlockVolume (the actual gRPC method)
//   - Authority side: reads the registry directly
//
// Then compares the two. If they diverge, Coherent=false with a Reason.
func (ms *MasterServer) PublicationDiagnosticFor(volumeName string) (PublicationDiagnostic, bool) {
	if ms.blockRegistry == nil {
		return PublicationDiagnostic{}, false
	}

	// Read 1: the operator-visible publication surface.
	lookupResp, err := ms.LookupBlockVolume(context.Background(), &master_pb.LookupBlockVolumeRequest{Name: volumeName})
	if err != nil {
		return PublicationDiagnostic{}, false
	}

	// Read 2: the registry authority (separate read).
	entry, ok := ms.blockRegistry.Lookup(volumeName)
	if !ok {
		return PublicationDiagnostic{}, false
	}

	diag := PublicationDiagnostic{
		VolumeName:            volumeName,
		LookupVolumeServer:    lookupResp.VolumeServer,
		LookupIscsiAddr:       lookupResp.IscsiAddr,
		AuthorityVolumeServer: entry.VolumeServer,
		AuthorityIscsiAddr:    entry.ISCSIAddr,
	}

	// Compare the two reads.
	vsMatch := diag.LookupVolumeServer == diag.AuthorityVolumeServer
	iscsiMatch := diag.LookupIscsiAddr == diag.AuthorityIscsiAddr
	diag.Coherent = vsMatch && iscsiMatch
	if !diag.Coherent {
		if !vsMatch {
			diag.Reason = "volume_server_mismatch"
		} else {
			diag.Reason = "iscsi_addr_mismatch"
		}
	}

	return diag, true
}

func newBlockFailoverState() *blockFailoverState {
	return &blockFailoverState{
		pendingRebuilds: make(map[string][]pendingRebuild),
		deferredTimers:  make(map[string][]deferredPromotion),
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
				capturedEpoch := entry.Epoch // T3: capture epoch for stale-timer validation
				capturedDeadServer := deadServer // capture for closure
				timer := time.AfterFunc(delay, func() {
					// Clean up the deferred entry regardless of outcome.
					ms.removeFiredDeferredPromotion(capturedDeadServer, volumeName)

					// T3: Re-validate before acting — prevent stale timer on recreated/changed volume.
					current, ok := ms.blockRegistry.Lookup(volumeName)
					if !ok {
						glog.V(0).Infof("failover: deferred promotion for %q skipped (volume deleted)", volumeName)
						return
					}
					if current.Epoch != capturedEpoch {
						glog.V(0).Infof("failover: deferred promotion for %q skipped (epoch changed %d -> %d)",
							volumeName, capturedEpoch, current.Epoch)
						return
					}
					ms.promoteReplica(volumeName)
				})
				ms.blockFailover.mu.Lock()
				ms.blockFailover.deferredTimers[deadServer] = append(
					ms.blockFailover.deferredTimers[deadServer], deferredPromotion{
						Timer:          timer,
						VolumeName:     volumeName,
						CurrentPrimary: entry.VolumeServer,
						AffectedServer: deadServer,
					})
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
				replicaDataAddr := ri.DataAddr // CP13-8: save before removal
				replicaCtrlAddr := ri.CtrlAddr
				// Remove dead replica from registry.
				if err := ms.blockRegistry.RemoveReplica(entry.Name, deadServer); err != nil {
					glog.Warningf("failover: RemoveReplica %q on %s: %v", entry.Name, deadServer, err)
					continue
				}
				// Record pending rebuild for when dead server reconnects.
				ms.recordPendingRebuild(deadServer, pendingRebuild{
					VolumeName:      entry.Name,
					OldPath:         replicaPath,
					NewPrimary:      entry.VolumeServer,
					Epoch:           entry.Epoch,
					ReplicaDataAddr: replicaDataAddr,
					ReplicaCtrlAddr: replicaCtrlAddr,
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

	ms.finalizePromotion(volumeName, oldPrimary, oldPath, newEpoch)
}

// finalizePromotion performs post-registry promotion steps:
// enqueue assignment for new primary, record pending rebuild for old primary, bump metrics.
// Called by both promoteReplica (auto) and blockVolumePromoteHandler (manual).
func (ms *MasterServer) finalizePromotion(volumeName, oldPrimary, oldPath string, newEpoch uint64) {
	// Re-read entry after promotion.
	entry, ok := ms.blockRegistry.Lookup(volumeName)
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
			ServerID: ri.Server, // V2: stable identity
		})
	}
	// Backward compat: also set scalar fields if exactly 1 replica.
	if len(entry.Replicas) == 1 {
		assignment.ReplicaDataAddr = entry.Replicas[0].DataAddr
		assignment.ReplicaCtrlAddr = entry.Replicas[0].CtrlAddr
		assignment.ReplicaServerID = entry.Replicas[0].Server // V2: stable identity
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
	promos := ms.blockFailover.deferredTimers[server]
	delete(ms.blockFailover.deferredTimers, server)
	ms.blockFailover.mu.Unlock()
	for _, dp := range promos {
		dp.Timer.Stop()
	}
	if len(promos) > 0 {
		glog.V(0).Infof("failover: cancelled %d deferred promotion timers for reconnected %s", len(promos), server)
	}
}

// removeFiredDeferredPromotion removes a single deferred promotion entry after
// its timer has fired (whether it promoted or was skipped). This keeps
// FailoverDiagnostic accurate: once the timer fires, the volume is no longer
// in lease-wait state.
func (ms *MasterServer) removeFiredDeferredPromotion(server, volumeName string) {
	if ms.blockFailover == nil {
		return
	}
	ms.blockFailover.mu.Lock()
	defer ms.blockFailover.mu.Unlock()
	promos := ms.blockFailover.deferredTimers[server]
	for i, dp := range promos {
		if dp.VolumeName == volumeName {
			ms.blockFailover.deferredTimers[server] = append(promos[:i], promos[i+1:]...)
			if len(ms.blockFailover.deferredTimers[server]) == 0 {
				delete(ms.blockFailover.deferredTimers, server)
			}
			return
		}
	}
}

// recoverBlockVolumes is called when a previously dead VS reconnects.
// It cancels any deferred promotion timers (R2-F2), drains pending rebuilds,
// enqueues rebuild assignments, and checks for orphaned primaries (T2/B-06).
func (ms *MasterServer) recoverBlockVolumes(reconnectedServer string) {
	// R2-F2: Cancel deferred promotion timers for this server to prevent split-brain.
	ms.cancelDeferredTimers(reconnectedServer)

	// T2 (B-06): Check for orphaned primaries — volumes where the reconnecting
	// server is a replica but the primary is dead/disconnected.
	ms.reevaluateOrphanedPrimaries(reconnectedServer)

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

		// CP13-8: Use replica addresses saved before death for catch-up-first recovery.
		// These are deterministic (derived from volume path hash in ReplicationPorts),
		// so they should be the same after VS restart. If the VS somehow gets different
		// ports (e.g., port conflict), the catch-up attempt will fail at the TCP level
		// and fall through to the shipper's NeedsRebuild → master rebuild path.
		// This is an optimization, not a source of truth — the master remains the
		// authority for topology/assignment changes.
		dataAddr := rb.ReplicaDataAddr
		ctrlAddr := rb.ReplicaCtrlAddr

		// Update registry: reconnected server becomes a replica.
		ms.blockRegistry.AddReplica(rb.VolumeName, ReplicaInfo{
			Server:   reconnectedServer,
			Path:     rb.OldPath,
			DataAddr: dataAddr,
			CtrlAddr: ctrlAddr,
		})

		// CP13-8: Try catch-up first (Replica assignment), fall back to rebuild.
		// If the replica can catch up from the primary's retained WAL, this is
		// much faster than a full rebuild. The shipper's reconnect handshake
		// (CP13-5) determines whether catch-up or rebuild is actually needed.
		// If catch-up fails, the shipper marks NeedsRebuild, and the master
		// sends a Rebuilding assignment on the next heartbeat cycle.
		if dataAddr != "" {
			leaseTTLMs := blockvol.LeaseTTLToWire(30 * time.Second)
			// Send Replica assignment to the reconnected server.
			ms.blockAssignmentQueue.Enqueue(reconnectedServer, blockvol.BlockVolumeAssignment{
				Path:            rb.OldPath,
				Epoch:           entry.Epoch,
				Role:            blockvol.RoleToWire(blockvol.RoleReplica),
				LeaseTtlMs:      leaseTTLMs,
				ReplicaDataAddr: dataAddr,
				ReplicaCtrlAddr: ctrlAddr,
			})
			// Also re-send Primary assignment so the primary gets fresh replica addresses.
			primaryAssignment := blockvol.BlockVolumeAssignment{
				Path:       entry.Path,
				Epoch:      entry.Epoch,
				Role:       blockvol.RoleToWire(blockvol.RolePrimary),
				LeaseTtlMs: leaseTTLMs,
			}
			// Include all replica addresses with stable identity.
			for _, ri := range entry.Replicas {
				primaryAssignment.ReplicaAddrs = append(primaryAssignment.ReplicaAddrs, blockvol.ReplicaAddr{
					DataAddr: ri.DataAddr,
					CtrlAddr: ri.CtrlAddr,
					ServerID: ri.Server, // V2: stable identity
				})
			}
			if len(entry.Replicas) == 1 {
				primaryAssignment.ReplicaDataAddr = entry.Replicas[0].DataAddr
				primaryAssignment.ReplicaCtrlAddr = entry.Replicas[0].CtrlAddr
				primaryAssignment.ReplicaServerID = entry.Replicas[0].Server // V2
			}
			ms.blockAssignmentQueue.Enqueue(entry.VolumeServer, primaryAssignment)

			glog.V(0).Infof("recover: enqueued catch-up (Replica) for %q on %s (epoch=%d, data=%s) + Primary refresh on %s",
				rb.VolumeName, reconnectedServer, entry.Epoch, dataAddr, entry.VolumeServer)
			continue
		}

		// Fallback: no known addresses — use rebuild path.
		rebuildAddr := entry.RebuildListenAddr
		if rebuildAddr == "" {
			glog.Warningf("rebuild: %q RebuildListenAddr is empty (new primary %s may not have heartbeated yet), "+
				"queuing rebuild anyway — VS should retry on empty addr", rb.VolumeName, entry.VolumeServer)
		}

		ms.blockAssignmentQueue.Enqueue(reconnectedServer, blockvol.BlockVolumeAssignment{
			Path:        rb.OldPath,
			Epoch:       entry.Epoch,
			Role:        blockvol.RoleToWire(blockvol.RoleRebuilding),
			RebuildAddr: rebuildAddr,
		})

		ms.blockRegistry.RebuildsTotal.Add(1)
		glog.V(0).Infof("rebuild: enqueued rebuild for %q on %s (epoch=%d, rebuildAddr=%s)",
			rb.VolumeName, reconnectedServer, entry.Epoch, rebuildAddr)
	}
}

// reevaluateOrphanedPrimaries checks if the given server is a replica for any
// volumes whose primary is dead (not block-capable). If so, promotes the best
// available replica — but only after the old primary's lease has expired, to
// refreshPrimaryForAddrChange sends a fresh Primary assignment when a replica's
// receiver address changed (e.g., restart with port conflict). This ensures the
// primary's shipper gets the new address without waiting for the next heartbeat cycle.
func (ms *MasterServer) refreshPrimaryForAddrChange(ac ReplicaAddrChange) {
	entry, ok := ms.blockRegistry.Lookup(ac.VolumeName)
	if !ok {
		return
	}
	leaseTTLMs := blockvol.LeaseTTLToWire(30 * time.Second)
	assignment := blockvol.BlockVolumeAssignment{
		Path:       entry.Path,
		Epoch:      entry.Epoch,
		Role:       blockvol.RoleToWire(blockvol.RolePrimary),
		LeaseTtlMs: leaseTTLMs,
	}
	for _, ri := range entry.Replicas {
		assignment.ReplicaAddrs = append(assignment.ReplicaAddrs, blockvol.ReplicaAddr{
			DataAddr: ri.DataAddr,
			CtrlAddr: ri.CtrlAddr,
			ServerID: ri.Server, // V2: stable identity
		})
	}
	if len(entry.Replicas) == 1 {
		assignment.ReplicaDataAddr = entry.Replicas[0].DataAddr
		assignment.ReplicaCtrlAddr = entry.Replicas[0].CtrlAddr
		assignment.ReplicaServerID = entry.Replicas[0].Server // V2
	}
	// Use current registry primary (not stale ac.PrimaryServer) in case
	// failover happened between address-change detection and this refresh.
	currentPrimary := entry.VolumeServer
	ms.blockAssignmentQueue.Enqueue(currentPrimary, assignment)
	glog.V(0).Infof("recover: replica addr changed for %q (data: %s→%s, ctrl: %s→%s), refreshed Primary on %s",
		ac.VolumeName, ac.OldDataAddr, ac.NewDataAddr, ac.OldCtrlAddr, ac.NewCtrlAddr, currentPrimary)
}

// enqueuePrimaryRefresh sends a fresh Primary assignment with replica addresses.
// CP13-8A: called when a replica re-registers after promote so the new primary
// gets shipper configuration for the re-registered replica.
func (ms *MasterServer) enqueuePrimaryRefresh(entry BlockVolumeEntry) {
	leaseTTLMs := blockvol.LeaseTTLToWire(30 * time.Second)
	assignment := blockvol.BlockVolumeAssignment{
		Path:       entry.Path,
		Epoch:      entry.Epoch,
		Role:       blockvol.RoleToWire(blockvol.RolePrimary),
		LeaseTtlMs: leaseTTLMs,
	}
	for _, ri := range entry.Replicas {
		assignment.ReplicaAddrs = append(assignment.ReplicaAddrs, blockvol.ReplicaAddr{
			DataAddr: ri.DataAddr,
			CtrlAddr: ri.CtrlAddr,
			ServerID: ri.Server,
		})
	}
	if len(entry.Replicas) == 1 {
		assignment.ReplicaDataAddr = entry.Replicas[0].DataAddr
		assignment.ReplicaCtrlAddr = entry.Replicas[0].CtrlAddr
		assignment.ReplicaServerID = entry.Replicas[0].Server
	}
	ms.blockAssignmentQueue.Enqueue(entry.VolumeServer, assignment)
	glog.V(0).Infof("CP13-8A: enqueued Primary refresh for %q on %s with %d replica(s)",
		entry.Name, entry.VolumeServer, len(entry.Replicas))
}

// maintain the same split-brain protection as failoverBlockVolumes().
// This fixes B-06 (orphaned primary after replica re-register)
// and partially B-08 (fast reconnect skips failover window).
func (ms *MasterServer) reevaluateOrphanedPrimaries(server string) {
	if ms.blockRegistry == nil {
		return
	}
	orphaned := ms.blockRegistry.VolumesWithDeadPrimary(server)
	now := time.Now()
	for _, volumeName := range orphaned {
		entry, ok := ms.blockRegistry.Lookup(volumeName)
		if !ok {
			continue
		}

		// Respect lease expiry — same gate as failoverBlockVolumes().
		leaseExpiry := entry.LastLeaseGrant.Add(entry.LeaseTTL)
		if now.Before(leaseExpiry) {
			delay := leaseExpiry.Sub(now)
			glog.V(0).Infof("failover: orphaned primary for %q (replica %s alive, primary dead) "+
				"but lease expires in %v, deferring promotion", volumeName, server, delay)
			capturedEpoch := entry.Epoch
			deadPrimary := entry.VolumeServer
			timer := time.AfterFunc(delay, func() {
				// Clean up the deferred entry regardless of outcome.
				ms.removeFiredDeferredPromotion(deadPrimary, volumeName)

				current, ok := ms.blockRegistry.Lookup(volumeName)
				if !ok {
					return
				}
				if current.Epoch != capturedEpoch {
					glog.V(0).Infof("failover: deferred orphan promotion for %q skipped (epoch changed %d -> %d)",
						volumeName, capturedEpoch, current.Epoch)
					return
				}
				ms.promoteReplica(volumeName)
			})
			ms.blockFailover.mu.Lock()
			ms.blockFailover.deferredTimers[deadPrimary] = append(
				ms.blockFailover.deferredTimers[deadPrimary], deferredPromotion{
					Timer:          timer,
					VolumeName:     volumeName,
					CurrentPrimary: deadPrimary,
					AffectedServer: deadPrimary,
				})
			ms.blockFailover.mu.Unlock()
			continue
		}

		glog.V(0).Infof("failover: orphaned primary detected for %q (replica %s alive, primary dead, lease expired), promoting",
			volumeName, server)
		ms.promoteReplica(volumeName)
	}
}
