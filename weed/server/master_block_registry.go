package weed_server

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// VolumeStatus tracks the lifecycle of a block volume entry.
type VolumeStatus int

const (
	StatusPending VolumeStatus = iota // Created via RPC, not yet confirmed by heartbeat
	StatusActive                      // Confirmed by heartbeat from volume server
)

// ReplicaInfo tracks one replica of a block volume (CP8-2).
type ReplicaInfo struct {
	Server           string    // replica VS address
	Path             string    // file path on replica VS
	ISCSIAddr        string    // iSCSI target address
	IQN              string    // iSCSI qualified name
	NvmeAddr         string    // NVMe/TCP target address (ip:port), empty if NVMe disabled
	NQN              string    // NVMe subsystem NQN, empty if NVMe disabled
	DataAddr         string    // WAL receiver data listen addr
	CtrlAddr         string    // WAL receiver ctrl listen addr
	Ready            bool      // receiver/publish readiness confirmed by replica heartbeat
	HasExplicitReady bool      // whether replica readiness was carried explicitly on heartbeat
	HealthScore      float64   // from heartbeat (0.0-1.0)
	WALHeadLSN       uint64    // from heartbeat
	WALLag           uint64    // computed: primary.WALHeadLSN - replica.WALHeadLSN
	LastHeartbeat    time.Time // last heartbeat received from this replica
	Role             uint32    // replica role (RoleReplica, RoleRebuilding, etc.)
}

const (
	// DefaultPromotionLSNTolerance is the max WAL LSN lag allowed for promotion eligibility.
	// Configurable per-registry via SetPromotionLSNTolerance.
	DefaultPromotionLSNTolerance uint64 = 100
)

// BlockVolumeEntry tracks one block volume across the cluster.
type BlockVolumeEntry struct {
	Name             string
	VolumeServer     string // volume server address (ip:port or grpc addr)
	Path             string // file path on volume server
	IQN              string
	ISCSIAddr        string
	NvmeAddr         string // NVMe/TCP target address (ip:port), empty if NVMe disabled
	NQN              string // NVMe subsystem NQN, empty if NVMe disabled
	SizeBytes        uint64
	ReplicaPlacement string // SeaweedFS placement string: "000", "001", "010", "100"
	Epoch            uint64
	Role             uint32
	Status           VolumeStatus

	// Deprecated scalar replica fields (CP6-3). Use Replicas[] for new code.
	ReplicaServer     string
	ReplicaPath       string
	ReplicaISCSIAddr  string
	ReplicaIQN        string
	ReplicaDataAddr   string
	ReplicaCtrlAddr   string
	RebuildListenAddr string // rebuild server listen addr on primary

	// CP8-2: Multi-replica support.
	ReplicaFactor            int           // 2 or 3 (default 2)
	Replicas                 []ReplicaInfo // one per replica (RF-1 entries)
	HealthScore              float64       // primary health score from heartbeat
	ReplicaReady             bool          // all configured replicas are ready for publication
	ReplicaDegraded          bool          // aggregate: transport degraded OR not ready
	TransportDegraded        bool          // primary reports degraded replicas
	NeedsRebuild             bool          // explicit primary heartbeat needs_rebuild truth when present
	HasNeedsRebuild          bool          // whether the current primary heartbeat carried explicit needs_rebuild truth
	PublishHealthy           bool          // explicit primary heartbeat publish_healthy truth when present
	HasPublishHealthy        bool          // whether the current primary heartbeat carried explicit publish_healthy truth
	HeartbeatVolumeMode      string        // explicit primary heartbeat outward volume_mode truth when present
	HasHeartbeatVolumeMode   bool          // whether the current primary heartbeat carried explicit outward volume_mode truth
	HeartbeatVolumeReason    string        // explicit primary heartbeat outward volume_mode_reason truth when present
	HasHeartbeatVolumeReason bool          // whether the current primary heartbeat carried explicit outward volume_mode_reason truth
	EngineProjectionMode     string        // T1: pure V2 engine-derived local projection mode (distinct from HeartbeatVolumeMode)
	HasEngineProjectionMode  bool          // whether the current primary heartbeat carried explicit engine_projection_mode
	PendingPrimaryHeartbeat  bool          // promotion selected a new primary, but it has not yet heartbeated as primary on the new epoch

	// CP13-9: Normalized volume mode for external surfaces.
	// Computed by recomputeReplicaState from the current entry state.
	VolumeMode string // "allocated_only", "bootstrap_pending", "publish_healthy", "degraded", "needs_rebuild"
	WALHeadLSN uint64 // primary WAL head LSN from heartbeat

	// T5: Master-owned cluster-level replication health judgment.
	// Distinct from EngineProjectionMode (VS-local) and VolumeMode (legacy).
	// Computed from multi-replica facts: replica LSN lag, heartbeat freshness,
	// role state, and recovery phase. Monotonic: worst replica state dominates.
	ClusterReplicationMode string // "keepup", "catching_up", "degraded", "needs_rebuild", "no_replicas"

	// CP8-3-1: Durability mode.
	DurabilityMode string // "best_effort", "sync_all", "sync_quorum"

	// CP11B-1: Provisioning preset (control-plane metadata only).
	Preset string // "database", "general", "throughput", or ""

	// Lease tracking for failover (CP6-3 F2).
	LastLeaseGrant time.Time
	LeaseTTL       time.Duration

	// CP11A-2: Coordinated expand tracking.
	ExpandInProgress  bool
	ExpandFailed      bool // true = primary committed but replica(s) failed; size suppressed
	PendingExpandSize uint64
	ExpandEpoch       uint64

	// CP13-8A: Set by reconcileOnRestart/upsertServerAsReplica when a replica
	// is added after promote. The heartbeat handler uses this to enqueue an
	// updated Primary assignment with the new replica's addresses.
	NeedsPrimaryRefresh bool
}

// HasReplica returns true if this volume has any replica (checks both new and deprecated fields).
func (e *BlockVolumeEntry) HasReplica() bool {
	return len(e.Replicas) > 0 || e.ReplicaServer != ""
}

// AllReplicasReady returns true when every configured replica has reported
// publish readiness via heartbeat. Volumes without replicas are vacuously ready.
func (e *BlockVolumeEntry) AllReplicasReady() bool {
	if len(e.Replicas) == 0 {
		return true
	}
	for _, ri := range e.Replicas {
		if !ri.Ready {
			return false
		}
	}
	return true
}

// FirstReplica returns the first replica info, or nil if none.
func (e *BlockVolumeEntry) FirstReplica() *ReplicaInfo {
	if len(e.Replicas) > 0 {
		return &e.Replicas[0]
	}
	return nil
}

// ReplicaByServer returns the replica hosted on the given server, or nil.
func (e *BlockVolumeEntry) ReplicaByServer(server string) *ReplicaInfo {
	for i := range e.Replicas {
		if e.Replicas[i].Server == server {
			return &e.Replicas[i]
		}
	}
	return nil
}

func (e *BlockVolumeEntry) recomputeReplicaState() {
	e.ReplicaReady = e.AllReplicasReady()
	if !e.HasReplica() {
		e.ReplicaDegraded = e.TransportDegraded
	} else {
		e.ReplicaDegraded = e.TransportDegraded || !e.ReplicaReady
	}

	// CP13-9: compute normalized VolumeMode for external surfaces.
	e.VolumeMode = e.computeVolumeMode()

	// T5: compute cluster-level replication mode from multi-replica facts.
	e.ClusterReplicationMode = e.computeClusterReplicationMode()
}

// computeClusterReplicationMode returns the master-owned cluster-level
// replication health judgment for the RF2 set. This is distinct from
// EngineProjectionMode (VS-local engine truth) and VolumeMode (legacy).
//
// Mode meanings:
//   - "no_replicas": RF=1 or no replicas configured (not an RF2 judgment)
//   - "keepup": all replicas healthy, LSN within tolerance, heartbeat fresh
//   - "catching_up": at least one replica is behind but recoverable
//   - "degraded": at least one replica has barrier failure or impaired state
//   - "needs_rebuild": at least one replica has unrecoverable gap
//
// Monotonic: worst replica state dominates the cluster mode.
func (e *BlockVolumeEntry) computeClusterReplicationMode() string {
	rf := e.ReplicaFactor
	if rf == 0 {
		rf = 1
	}
	// RF=1: no replication configured — not an RF2 judgment.
	if rf <= 1 && len(e.Replicas) == 0 {
		return "no_replicas"
	}
	// RF>1 but no replicas registered: the set is degraded (missing replica).
	if len(e.Replicas) == 0 {
		return "degraded"
	}

	worst := "keepup"

	// Incorporate master-observed transport degradation signal.
	if e.TransportDegraded {
		worst = worseClusterMode(worst, "degraded")
	}

	for _, ri := range e.Replicas {
		replicaMode := evaluateReplicaHealth(ri, e.WALHeadLSN)
		worst = worseClusterMode(worst, replicaMode)
	}
	return worst
}

// evaluateReplicaHealth returns the cluster-level health classification for
// one replica. Does NOT use EngineProjectionMode — computes from registry-
// observed facts only (heartbeat freshness, LSN lag, role).
func evaluateReplicaHealth(ri ReplicaInfo, primaryWALHeadLSN uint64) string {
	// Rebuilding role is needs_rebuild.
	if blockvol.RoleFromWire(ri.Role) == blockvol.RoleRebuilding {
		return "needs_rebuild"
	}

	// Stale heartbeat (>60s) is degraded.
	if !ri.LastHeartbeat.IsZero() && time.Since(ri.LastHeartbeat) > 60*time.Second {
		return "degraded"
	}

	// No heartbeat ever received — catching up.
	if ri.LastHeartbeat.IsZero() {
		return "catching_up"
	}

	// LSN lag check: if primary has progress and replica is behind.
	if primaryWALHeadLSN > 0 && ri.WALHeadLSN < primaryWALHeadLSN {
		lag := primaryWALHeadLSN - ri.WALHeadLSN
		if lag > 1000 {
			return "needs_rebuild"
		}
		if lag > 0 {
			return "catching_up"
		}
	}

	// Not ready is catching_up.
	if !ri.Ready {
		return "catching_up"
	}

	return "keepup"
}

// worseClusterMode returns the more severe of two cluster modes.
// Severity order: needs_rebuild > degraded > catching_up > keepup.
func worseClusterMode(a, b string) string {
	if clusterModeRank(b) > clusterModeRank(a) {
		return b
	}
	return a
}

func clusterModeRank(mode string) int {
	switch mode {
	case "keepup":
		return 0
	case "catching_up":
		return 1
	case "degraded":
		return 2
	case "needs_rebuild":
		return 3
	default:
		return 0
	}
}

// computeVolumeMode returns the normalized mode string for CP13-9.
//
// Mode meanings:
//   - "allocated_only": volume exists but has no replicas configured (RF=1 or pre-assignment)
//   - "bootstrap_pending": replicas configured but not yet confirmed ready (first-write bootstrap)
//   - "publish_healthy": all replicas ready, no transport degradation
//   - "degraded": replication was healthy but is now impaired (transient)
//   - "needs_rebuild": one or more replicas have unrecoverable gap
func (e *BlockVolumeEntry) computeVolumeMode() string {
	if e.HasHeartbeatVolumeMode && validHeartbeatVolumeMode(e.HeartbeatVolumeMode) {
		return e.HeartbeatVolumeMode
	}

	rf := e.ReplicaFactor
	if rf == 0 {
		rf = 1
	}

	// RF=1: no replication — "allocated_only" if standalone.
	if rf <= 1 && !e.HasReplica() {
		return "allocated_only"
	}

	// Has replica config but no replicas registered yet.
	if rf > 1 && len(e.Replicas) == 0 {
		return "bootstrap_pending"
	}

	// Prefer explicit primary heartbeat needs_rebuild truth when present.
	if e.HasNeedsRebuild {
		if e.NeedsRebuild {
			return "needs_rebuild"
		}
	} else {
		// Backward-compatible fallback: older paths may only surface needs_rebuild
		// through replica-side role heuristics.
		for _, ri := range e.Replicas {
			if blockvol.RoleFromWire(ri.Role) == blockvol.RoleRebuilding {
				return "needs_rebuild"
			}
		}
	}

	// A registry-driven promotion has selected a winner, but outward publication
	// should not read as complete until that winner heartbeats as the new primary.
	if e.PendingPrimaryHeartbeat {
		return "bootstrap_pending"
	}

	// Prefer explicit primary heartbeat publish_healthy truth when present.
	if e.HasPublishHealthy && e.PublishHealthy {
		return "publish_healthy"
	}

	// Replicas exist but not all ready.
	if !e.ReplicaReady {
		return "bootstrap_pending"
	}

	// All ready but transport degraded.
	if e.TransportDegraded {
		return "degraded"
	}

	if e.HasPublishHealthy {
		return "bootstrap_pending"
	}

	return "publish_healthy"
}

// BestReplicaForPromotion returns the best replica for promotion, or nil if none eligible.
// Criteria: highest HealthScore, tie-break by highest WALHeadLSN, then first in list.
func (e *BlockVolumeEntry) BestReplicaForPromotion() *ReplicaInfo {
	if len(e.Replicas) == 0 {
		return nil
	}
	best := 0
	for i := 1; i < len(e.Replicas); i++ {
		if e.Replicas[i].HealthScore > e.Replicas[best].HealthScore {
			best = i
		} else if e.Replicas[i].HealthScore == e.Replicas[best].HealthScore &&
			e.Replicas[i].WALHeadLSN > e.Replicas[best].WALHeadLSN {
			best = i
		}
	}
	return &e.Replicas[best]
}

// BlockVolumeRegistry is the in-memory registry of block volumes.
// Rebuilt from heartbeats on master restart (no persistence).
type BlockVolumeRegistry struct {
	mu           sync.RWMutex
	volumes      map[string]*BlockVolumeEntry // keyed by name
	byServer     map[string]map[string]bool   // server -> set of volume names
	blockServers map[string]*blockServerInfo  // servers known to support block volumes

	// Promotion eligibility: max WAL LSN lag for replica to be promotable.
	promotionLSNTolerance uint64

	// inflight guards concurrent CreateBlockVolume for the same name.
	inflight sync.Map // name -> *inflightEntry

	// Metrics (CP8-4).
	PromotionsTotal atomic.Uint64
	FailoversTotal  atomic.Uint64
	RebuildsTotal   atomic.Uint64
}

type inflightEntry struct{}

// blockServerInfo tracks server-level capabilities reported via heartbeat.
type blockServerInfo struct {
	NvmeAddr       string // NVMe/TCP listen address; empty if NVMe disabled
	DiskType       string // reported via heartbeat (future)
	AvailableBytes uint64 // reported via heartbeat (future)
}

// PlacementCandidateInfo is the registry's view of a placement candidate.
// Used by the placement planner — the single bridge point between registry
// and the pure evaluateBlockPlacement() function.
type PlacementCandidateInfo struct {
	Address        string
	VolumeCount    int
	DiskType       string // empty = unknown/any
	AvailableBytes uint64 // 0 = unknown
	NvmeCapable    bool
}

// NewBlockVolumeRegistry creates an empty registry.
func NewBlockVolumeRegistry() *BlockVolumeRegistry {
	return &BlockVolumeRegistry{
		volumes:               make(map[string]*BlockVolumeEntry),
		byServer:              make(map[string]map[string]bool),
		blockServers:          make(map[string]*blockServerInfo),
		promotionLSNTolerance: DefaultPromotionLSNTolerance,
	}
}

// Register adds an entry to the registry.
// Returns error if a volume with the same name already exists.
func (r *BlockVolumeRegistry) Register(entry *BlockVolumeEntry) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.volumes[entry.Name]; ok {
		return fmt.Errorf("block volume %q already registered", entry.Name)
	}
	entry.recomputeReplicaState()
	r.volumes[entry.Name] = entry
	r.addToServer(entry.VolumeServer, entry.Name)
	// Also index replica servers so ListByServer finds them.
	for _, ri := range entry.Replicas {
		r.addToServer(ri.Server, entry.Name)
	}
	return nil
}

// Unregister removes and returns the entry. Returns nil if not found.
func (r *BlockVolumeRegistry) Unregister(name string) *BlockVolumeEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.volumes[name]
	if !ok {
		return nil
	}
	delete(r.volumes, name)
	r.removeFromServer(entry.VolumeServer, name)
	for _, ri := range entry.Replicas {
		r.removeFromServer(ri.Server, name)
	}
	return entry
}

// AcquireExpandInflight tries to acquire an expand lock for the named volume
// and records the pending expand metadata on the entry.
// Returns false if an expand is already in flight or failed (requires ClearExpandFailed first).
func (r *BlockVolumeRegistry) AcquireExpandInflight(name string, pendingSize, expandEpoch uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.volumes[name]
	if !ok {
		return false
	}
	if entry.ExpandInProgress || entry.ExpandFailed {
		return false
	}
	entry.ExpandInProgress = true
	entry.PendingExpandSize = pendingSize
	entry.ExpandEpoch = expandEpoch
	return true
}

// ReleaseExpandInflight clears all expand tracking fields for the named volume.
// Only call on clean success or clean cancel (all nodes rolled back).
func (r *BlockVolumeRegistry) ReleaseExpandInflight(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.volumes[name]
	if !ok {
		return
	}
	entry.ExpandInProgress = false
	entry.ExpandFailed = false
	entry.PendingExpandSize = 0
	entry.ExpandEpoch = 0
}

// MarkExpandFailed transitions the entry from in-progress to failed.
// ExpandInProgress stays true so heartbeat continues to suppress size updates.
// The entry remains locked until ClearExpandFailed is called (manual reconciliation).
func (r *BlockVolumeRegistry) MarkExpandFailed(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.volumes[name]
	if !ok {
		return
	}
	entry.ExpandFailed = true
	// Keep ExpandInProgress=true, PendingExpandSize, ExpandEpoch — all needed for diagnosis.
}

// ClearExpandFailed resets the expand-failed state so a new expand can be attempted.
// Called by an operator or automated reconciliation after the inconsistency is resolved
// (e.g., failed replica rebuilt or manually expanded).
func (r *BlockVolumeRegistry) ClearExpandFailed(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.volumes[name]
	if !ok {
		return
	}
	entry.ExpandInProgress = false
	entry.ExpandFailed = false
	entry.PendingExpandSize = 0
	entry.ExpandEpoch = 0
}

// UpdateSize updates the size of a registered volume.
// Called only after a successful VS expand to keep registry in sync.
func (r *BlockVolumeRegistry) UpdateSize(name string, newSizeBytes uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.volumes[name]
	if !ok {
		return fmt.Errorf("block volume %q not found in registry", name)
	}
	entry.SizeBytes = newSizeBytes
	return nil
}

// clone returns a deep copy of the entry. The Replicas slice is copied
// so the caller cannot mutate registry state through the returned value.
func (e *BlockVolumeEntry) clone() BlockVolumeEntry {
	c := *e
	if len(e.Replicas) > 0 {
		c.Replicas = make([]ReplicaInfo, len(e.Replicas))
		copy(c.Replicas, e.Replicas)
	}
	return c
}

// Lookup returns a copy of the entry for the given name.
// The returned value is safe to read without holding any lock.
// To mutate registry state, use UpdateEntry instead.
func (r *BlockVolumeRegistry) Lookup(name string) (BlockVolumeEntry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	e, ok := r.volumes[name]
	if !ok {
		return BlockVolumeEntry{}, false
	}
	return e.clone(), ok
}

// UpdateEntry calls fn with the internal entry under write lock.
// Use this for any mutation that must be visible to the registry.
func (r *BlockVolumeRegistry) UpdateEntry(name string, fn func(*BlockVolumeEntry)) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.volumes[name]
	if !ok {
		return fmt.Errorf("block volume %q not found", name)
	}
	fn(e)
	return nil
}

// ListByServer returns copies of all entries hosted on the given server.
func (r *BlockVolumeRegistry) ListByServer(server string) []BlockVolumeEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names, ok := r.byServer[server]
	if !ok {
		return nil
	}
	entries := make([]BlockVolumeEntry, 0, len(names))
	for name := range names {
		if e, ok := r.volumes[name]; ok {
			entries = append(entries, e.clone())
		}
	}
	return entries
}

// UpdateFullHeartbeat reconciles the registry from a full heartbeat.
// Called on the first heartbeat from a volume server.
// Marks reported volumes as Active, removes entries for this server
// that are not reported (stale).
// ReplicaAddrChange records a replica whose advertised address changed,
// requiring a Primary assignment refresh so the shipper gets the new address.
// Detected only in the full heartbeat path (UpdateFullHeartbeat). Delta
// heartbeats do not carry replica addresses and cannot trigger this.
type ReplicaAddrChange struct {
	VolumeName    string
	PrimaryServer string
	OldDataAddr   string
	OldCtrlAddr   string
	NewDataAddr   string
	NewCtrlAddr   string
}

// HeartbeatResult holds the side effects from UpdateFullHeartbeat that the
// caller (heartbeat handler) must process.
type HeartbeatResult struct {
	AddrChanges          []ReplicaAddrChange
	PrimaryRefreshNeeded []BlockVolumeEntry // CP13-8A: entries needing primary assignment refresh
}

func (r *BlockVolumeRegistry) UpdateFullHeartbeat(server string, infos []*master_pb.BlockVolumeInfoMessage, nvmeAddr string) HeartbeatResult {
	return r.UpdateFullHeartbeatWithInventoryAuthority(server, infos, nvmeAddr, true)
}

func (r *BlockVolumeRegistry) UpdateFullHeartbeatWithInventoryAuthority(server string, infos []*master_pb.BlockVolumeInfoMessage, nvmeAddr string, blockInventoryAuthoritative bool) HeartbeatResult {
	var result HeartbeatResult
	r.mu.Lock()
	defer r.mu.Unlock()

	// Mark server as block-capable and record server-level NVMe capability.
	r.blockServers[server] = &blockServerInfo{NvmeAddr: nvmeAddr}

	// Build set of reported paths.
	reported := make(map[string]*master_pb.BlockVolumeInfoMessage, len(infos))
	for _, info := range infos {
		reported[info.Path] = info
	}

	// Find entries for this server that are NOT reported -> reconcile.
	if blockInventoryAuthoritative {
		if names, ok := r.byServer[server]; ok {
			for name := range names {
				entry := r.volumes[name]
				if entry == nil {
					continue
				}
				if entry.VolumeServer == server {
					// Server is the primary: check if primary path is reported.
					if _, found := reported[entry.Path]; !found {
						// B-10: Do not delete entries with a coordinated expand in flight.
						// The primary may have restarted mid-expand; deleting the entry
						// would orphan the volume and strand the expand coordinator.
						if entry.ExpandInProgress {
							glog.Warningf("block registry: skipping stale-cleanup for %q (ExpandInProgress=true, server=%s)",
								name, server)
							continue
						}
						delete(r.volumes, name)
						delete(names, name)
						// Also clean up replica entries from byServer.
						for _, ri := range entry.Replicas {
							r.removeFromServer(ri.Server, name)
						}
					}
				} else {
					// Server is a replica: check if replica path is reported.
					ri := entry.ReplicaByServer(server)
					if ri == nil {
						// No replica record — stale byServer index, just clean up.
						delete(names, name)
						continue
					}
					if _, found := reported[ri.Path]; !found {
						// Replica path not reported — remove this replica, NOT the whole volume.
						r.removeReplicaLocked(entry, server, name)
						delete(names, name)
						glog.V(0).Infof("block registry: removed stale replica %s for %q (path %s not in heartbeat)",
							server, name, ri.Path)
					}
				}
			}
		}
	}

	// Update or add entries for reported volumes.
	for _, info := range infos {
		// Find existing entry: search byServer index for matching path (primary or replica).
		var existing *BlockVolumeEntry
		var existingName string
		if names, ok := r.byServer[server]; ok {
			for vname := range names {
				if e := r.volumes[vname]; e != nil {
					if e.VolumeServer == server && e.Path == info.Path {
						existing = e
						existingName = vname
						break
					}
					if ri := e.ReplicaByServer(server); ri != nil && ri.Path == info.Path {
						existing = e
						existingName = vname
						break
					}
				}
			}
		}
		// Also try lookup by name derived from path (handles post-restart).
		if existing == nil {
			name := nameFromPath(info.Path)
			if name != "" {
				if e, ok := r.volumes[name]; ok {
					existing = e
					existingName = name
				}
			}
		}

		if existing != nil {
			isPrimary := existing.VolumeServer == server
			isReplica := existing.ReplicaByServer(server) != nil

			if isPrimary {
				r.applyPrimaryHeartbeatObservation(existing, info)
			} else if isReplica {
				r.applyReplicaHeartbeatObservation(existing, server, existingName, info, &result)
			} else {
				// Server reports a volume that exists but has no record of this server.
				// This happens after master restart. Use epoch-based reconciliation
				// to determine if the new server should be primary or replica.
				r.reconcileOnRestart(existingName, existing, server, info)
			}
		} else {
			// Auto-register volumes reported by heartbeat but not in registry.
			// This recovers state after master restart.
			name := nameFromPath(info.Path)
			if name == "" {
				continue
			}
			// Skip auto-register if a create is in progress for this volume.
			// Without this gate, the replica VS heartbeat can race ahead of
			// CreateBlockVolume.Register and create a bare entry that lacks
			// replica info, causing the real Register to hit "already registered"
			// and fall back to the incomplete auto-registered entry.
			if r.IsInflight(name) {
				continue
			}
			existing, dup := r.volumes[name]
			if !dup {
				entry := &BlockVolumeEntry{
					Name:                     name,
					VolumeServer:             server,
					Path:                     info.Path,
					SizeBytes:                info.VolumeSize,
					Epoch:                    info.Epoch,
					Role:                     info.Role,
					Status:                   StatusActive,
					LastLeaseGrant:           time.Now(),
					LeaseTTL:                 30 * time.Second,
					HealthScore:              info.HealthScore,
					TransportDegraded:        info.ReplicaDegraded,
					NeedsRebuild:             false,
					HasNeedsRebuild:          false,
					PublishHealthy:           false,
					HasPublishHealthy:        false,
					HeartbeatVolumeMode:      "",
					HasHeartbeatVolumeMode:   false,
					HeartbeatVolumeReason:    "",
					HasHeartbeatVolumeReason: false,
					WALHeadLSN:               info.WalHeadLsn,
					DurabilityMode:           info.DurabilityMode,
				}
				entry.NeedsRebuild, entry.HasNeedsRebuild = primaryNeedsRebuildObservedFromHeartbeat(info)
				entry.PublishHealthy, entry.HasPublishHealthy = primaryPublishHealthyObservedFromHeartbeat(info)
				entry.HeartbeatVolumeMode, entry.HasHeartbeatVolumeMode = primaryVolumeModeObservedFromHeartbeat(info)
				entry.HeartbeatVolumeReason, entry.HasHeartbeatVolumeReason = primaryVolumeReasonObservedFromHeartbeat(info)
				entry.EngineProjectionMode, entry.HasEngineProjectionMode = primaryEngineProjectionModeObservedFromHeartbeat(info)
				if info.ReplicaDataAddr != "" {
					entry.ReplicaDataAddr = info.ReplicaDataAddr
				}
				if info.ReplicaCtrlAddr != "" {
					entry.ReplicaCtrlAddr = info.ReplicaCtrlAddr
				}
				entry.NvmeAddr = info.NvmeAddr
				entry.NQN = info.Nqn
				entry.recomputeReplicaState()
				r.volumes[name] = entry
				r.addToServer(server, name)
				glog.V(0).Infof("block registry: auto-registered %q from heartbeat (server=%s, path=%s, size=%d)",
					name, server, info.Path, info.VolumeSize)
			} else {
				// Reconcile: a second server reports the same volume during restart reconstruction.
				r.reconcileOnRestart(name, existing, server, info)
			}
		}
	}
	// CP13-8A: collect entries that need primary refresh and clear the flag.
	for _, e := range r.volumes {
		if e.NeedsPrimaryRefresh {
			e.NeedsPrimaryRefresh = false
			result.PrimaryRefreshNeeded = append(result.PrimaryRefreshNeeded, e.clone())
		}
	}
	return result
}

// applyPrimaryHeartbeatObservation consumes one primary-side heartbeat into the
// registry entry. Caller must hold r.mu.
func (r *BlockVolumeRegistry) applyPrimaryHeartbeatObservation(existing *BlockVolumeEntry, info *master_pb.BlockVolumeInfoMessage) {
	// CP11A-2: skip size update during coordinated expand.
	if !existing.ExpandInProgress {
		existing.SizeBytes = info.VolumeSize
	}
	existing.Epoch = info.Epoch
	existing.Role = info.Role
	existing.Status = StatusActive
	existing.LastLeaseGrant = time.Now()
	newPrimaryTurnover := existing.PendingPrimaryHeartbeat
	existing.PendingPrimaryHeartbeat = false
	existing.HealthScore = info.HealthScore
	existing.TransportDegraded = info.ReplicaDegraded
	applyExplicitPrimaryTruthFromHeartbeat(existing, info, true)
	// T1: on the first heartbeat from a newly promoted primary, clear stale
	// EngineProjectionMode if the new primary does not emit the field. A new
	// primary that omits the field must not inherit the old primary's
	// V2-local projection — that would create synthetic master-side truth.
	if newPrimaryTurnover {
		if _, ok := primaryEngineProjectionModeObservedFromHeartbeat(info); !ok {
			existing.EngineProjectionMode = ""
			existing.HasEngineProjectionMode = false
		}
	}
	existing.WALHeadLSN = info.WalHeadLsn
	// F3: only update DurabilityMode when non-empty (prevents older VS from clearing strict mode).
	if info.DurabilityMode != "" {
		existing.DurabilityMode = info.DurabilityMode
	}
	// F5: update replica addresses from heartbeat info.
	if info.ReplicaDataAddr != "" {
		existing.ReplicaDataAddr = info.ReplicaDataAddr
	}
	if info.ReplicaCtrlAddr != "" {
		existing.ReplicaCtrlAddr = info.ReplicaCtrlAddr
	}
	// NVMe publication: update NVMe fields from heartbeat.
	// Required for master restart reconstruction and NVMe enable/disable.
	existing.NvmeAddr = info.NvmeAddr
	existing.NQN = info.Nqn
	// Sync first replica's data addrs to Replicas[].
	if info.ReplicaDataAddr != "" && len(existing.Replicas) > 0 {
		existing.Replicas[0].DataAddr = info.ReplicaDataAddr
		existing.Replicas[0].CtrlAddr = info.ReplicaCtrlAddr
	}
	existing.recomputeReplicaState()
}

// applyReplicaHeartbeatObservation consumes one replica-side heartbeat into the
// registry entry. Caller must hold r.mu.
func (r *BlockVolumeRegistry) applyReplicaHeartbeatObservation(existing *BlockVolumeEntry, server, existingName string, info *master_pb.BlockVolumeInfoMessage, result *HeartbeatResult) {
	for i := range existing.Replicas {
		if existing.Replicas[i].Server != server {
			continue
		}
		existing.Replicas[i].Path = info.Path
		existing.Replicas[i].WALHeadLSN = info.WalHeadLsn
		existing.Replicas[i].HealthScore = info.HealthScore
		existing.Replicas[i].LastHeartbeat = time.Now()
		// Keep role as RoleReplica — the VS may report a stale
		// primary role if it hasn't received its demotion assignment yet.
		// The registry's decision (lower epoch = replica) is authoritative.
		existing.Replicas[i].Role = blockvol.RoleToWire(blockvol.RoleReplica)
		existing.Replicas[i].NvmeAddr = info.NvmeAddr
		existing.Replicas[i].NQN = info.Nqn
		applyReplicaReadyFromHeartbeat(&existing.Replicas[i], info, true)
		if existing.WALHeadLSN > info.WalHeadLsn {
			existing.Replicas[i].WALLag = existing.WALHeadLSN - info.WalHeadLsn
		} else {
			existing.Replicas[i].WALLag = 0
		}
		// CP13-8: detect address change on replica restart.
		// If either the data or control address changed, the primary's
		// shipper has a stale endpoint. Queue a Primary refresh.
		if info.ReplicaDataAddr != "" || info.ReplicaCtrlAddr != "" {
			oldData := existing.Replicas[i].DataAddr
			oldCtrl := existing.Replicas[i].CtrlAddr
			dataChanged := info.ReplicaDataAddr != "" && oldData != "" && oldData != info.ReplicaDataAddr
			ctrlChanged := info.ReplicaCtrlAddr != "" && oldCtrl != "" && oldCtrl != info.ReplicaCtrlAddr
			dataBecameKnown := oldData == "" && info.ReplicaDataAddr != ""
			ctrlBecameKnown := oldCtrl == "" && info.ReplicaCtrlAddr != ""
			if dataChanged || ctrlChanged {
				result.AddrChanges = append(result.AddrChanges, ReplicaAddrChange{
					VolumeName:    existingName,
					PrimaryServer: existing.VolumeServer,
					OldDataAddr:   oldData,
					OldCtrlAddr:   oldCtrl,
					NewDataAddr:   info.ReplicaDataAddr,
					NewCtrlAddr:   info.ReplicaCtrlAddr,
				})
			}
			if info.ReplicaDataAddr != "" {
				existing.Replicas[i].DataAddr = info.ReplicaDataAddr
			}
			if info.ReplicaCtrlAddr != "" {
				existing.Replicas[i].CtrlAddr = info.ReplicaCtrlAddr
			}
			if dataBecameKnown || ctrlBecameKnown {
				existing.NeedsPrimaryRefresh = true
			}
		}
		if len(existing.Replicas) > 0 && existing.Replicas[0].Server == server {
			existing.ReplicaServer = existing.Replicas[0].Server
			existing.ReplicaPath = existing.Replicas[0].Path
			existing.ReplicaISCSIAddr = existing.Replicas[0].ISCSIAddr
			existing.ReplicaIQN = existing.Replicas[0].IQN
			existing.ReplicaDataAddr = existing.Replicas[0].DataAddr
			existing.ReplicaCtrlAddr = existing.Replicas[0].CtrlAddr
		}
		break
	}
	existing.recomputeReplicaState()
}

func applyReplicaReadyFromHeartbeat(replica *ReplicaInfo, info *master_pb.BlockVolumeInfoMessage, preserveWhenAbsent bool) {
	if replica == nil || info == nil {
		return
	}
	if info.ReplicaReady != nil {
		replica.Ready = info.GetReplicaReady()
		replica.HasExplicitReady = true
		return
	}
	if preserveWhenAbsent && replica.HasExplicitReady {
		return
	}
	replica.Ready = info.ReplicaDataAddr != "" && info.ReplicaCtrlAddr != ""
	replica.HasExplicitReady = false
}

func replicaReadyObservedFromHeartbeat(info *master_pb.BlockVolumeInfoMessage) bool {
	if info == nil {
		return false
	}
	if info.ReplicaReady != nil {
		return info.GetReplicaReady()
	}
	return info.ReplicaDataAddr != "" && info.ReplicaCtrlAddr != ""
}

func primaryNeedsRebuildObservedFromHeartbeat(info *master_pb.BlockVolumeInfoMessage) (bool, bool) {
	if info == nil {
		return false, false
	}
	if info.NeedsRebuild != nil {
		return info.GetNeedsRebuild(), true
	}
	return false, false
}

func primaryPublishHealthyObservedFromHeartbeat(info *master_pb.BlockVolumeInfoMessage) (bool, bool) {
	if info == nil {
		return false, false
	}
	if info.PublishHealthy != nil {
		return info.GetPublishHealthy(), true
	}
	return false, false
}

func primaryVolumeModeObservedFromHeartbeat(info *master_pb.BlockVolumeInfoMessage) (string, bool) {
	if info == nil {
		return "", false
	}
	if info.VolumeMode != nil {
		return info.GetVolumeMode(), true
	}
	return "", false
}

func primaryVolumeReasonObservedFromHeartbeat(info *master_pb.BlockVolumeInfoMessage) (string, bool) {
	if info == nil {
		return "", false
	}
	if info.VolumeModeReason != nil {
		return info.GetVolumeModeReason(), true
	}
	return "", false
}

func primaryEngineProjectionModeObservedFromHeartbeat(info *master_pb.BlockVolumeInfoMessage) (string, bool) {
	if info == nil {
		return "", false
	}
	if info.EngineProjectionMode != nil {
		return info.GetEngineProjectionMode(), true
	}
	return "", false
}

func applyExplicitPrimaryTruthFromHeartbeat(existing *BlockVolumeEntry, info *master_pb.BlockVolumeInfoMessage, preserveWhenAbsent bool) {
	if existing == nil || info == nil {
		return
	}
	if needsRebuild, ok := primaryNeedsRebuildObservedFromHeartbeat(info); ok {
		existing.NeedsRebuild = needsRebuild
		existing.HasNeedsRebuild = true
	} else if !preserveWhenAbsent {
		existing.NeedsRebuild = false
		existing.HasNeedsRebuild = false
	}
	if publishHealthy, ok := primaryPublishHealthyObservedFromHeartbeat(info); ok {
		existing.PublishHealthy = publishHealthy
		existing.HasPublishHealthy = true
	} else if !preserveWhenAbsent {
		existing.PublishHealthy = false
		existing.HasPublishHealthy = false
	}
	if mode, ok := primaryVolumeModeObservedFromHeartbeat(info); ok {
		existing.HeartbeatVolumeMode = mode
		existing.HasHeartbeatVolumeMode = true
	} else if !preserveWhenAbsent {
		existing.HeartbeatVolumeMode = ""
		existing.HasHeartbeatVolumeMode = false
	}
	if reason, ok := primaryVolumeReasonObservedFromHeartbeat(info); ok {
		existing.HeartbeatVolumeReason = reason
		existing.HasHeartbeatVolumeReason = true
	} else if !preserveWhenAbsent {
		existing.HeartbeatVolumeReason = ""
		existing.HasHeartbeatVolumeReason = false
	}
	if epm, ok := primaryEngineProjectionModeObservedFromHeartbeat(info); ok {
		existing.EngineProjectionMode = epm
		existing.HasEngineProjectionMode = true
	} else if !preserveWhenAbsent {
		existing.EngineProjectionMode = ""
		existing.HasEngineProjectionMode = false
	}
}

func validHeartbeatVolumeMode(mode string) bool {
	switch mode {
	case "allocated_only", "bootstrap_pending", "publish_healthy", "degraded", "needs_rebuild":
		return true
	default:
		return false
	}
}

// reconcileOnRestart handles the case where a second server reports a volume
// name that already exists in the registry during master restart reconstruction.
// Uses epoch-based tie-breaking to determine who is the real primary.
//
// Rules:
//  1. Higher epoch wins as primary — the old entry becomes a replica.
//  2. Same epoch, both claim primary — higher WALHeadLSN wins (heuristic).
//     A warning is logged because this is an ambiguous case.
//  3. Lower epoch — new server is added as replica.
//
// Caller must hold r.mu (write lock).
func (r *BlockVolumeRegistry) reconcileOnRestart(name string, existing *BlockVolumeEntry, newServer string, info *master_pb.BlockVolumeInfoMessage) {
	newEpoch := info.Epoch
	oldEpoch := existing.Epoch

	if newEpoch > oldEpoch {
		// New server has a higher epoch — it is the authoritative primary.
		// Demote the current entry to a replica.
		glog.V(0).Infof("block registry: reconcile %q: new server %s epoch %d > existing %s epoch %d, promoting new",
			name, newServer, newEpoch, existing.VolumeServer, oldEpoch)
		r.demoteExistingToReplica(name, existing, newServer, info)
		return
	}

	if newEpoch < oldEpoch {
		// New server has a lower epoch — add as replica.
		glog.V(0).Infof("block registry: reconcile %q: new server %s epoch %d < existing %s epoch %d, adding as replica",
			name, newServer, newEpoch, existing.VolumeServer, oldEpoch)
		r.upsertServerAsReplica(name, existing, newServer, info)
		return
	}

	// Same epoch — trust reported roles first.
	existingIsPrimary := existing.Role == blockvol.RoleToWire(blockvol.RolePrimary)
	newIsPrimary := info.Role == blockvol.RoleToWire(blockvol.RolePrimary)

	if existingIsPrimary && !newIsPrimary {
		// Existing claims primary, new claims replica — trust roles.
		glog.V(0).Infof("block registry: reconcile %q: same epoch %d, existing %s is primary, new %s is replica — keeping existing",
			name, newEpoch, existing.VolumeServer, newServer)
		r.upsertServerAsReplica(name, existing, newServer, info)
		return
	}
	if !existingIsPrimary && newIsPrimary {
		// New claims primary, existing is not — trust roles.
		glog.V(0).Infof("block registry: reconcile %q: same epoch %d, new %s claims primary, existing %s does not — promoting new",
			name, newEpoch, newServer, existing.VolumeServer)
		r.demoteExistingToReplica(name, existing, newServer, info)
		return
	}

	// Both claim primary or both claim replica — ambiguous. Use WALHeadLSN as heuristic.
	if newIsPrimary {
		// Both claim primary.
		if info.WalHeadLsn > existing.WALHeadLSN {
			glog.Warningf("block registry: reconcile %q: AMBIGUOUS same epoch %d, both primary — new %s LSN %d > existing %s LSN %d, promoting new (heuristic)",
				name, newEpoch, newServer, info.WalHeadLsn, existing.VolumeServer, existing.WALHeadLSN)
			r.demoteExistingToReplica(name, existing, newServer, info)
		} else {
			glog.Warningf("block registry: reconcile %q: AMBIGUOUS same epoch %d, both primary — existing %s LSN %d >= new %s LSN %d, keeping existing (heuristic)",
				name, newEpoch, existing.VolumeServer, existing.WALHeadLSN, newServer, info.WalHeadLsn)
			r.upsertServerAsReplica(name, existing, newServer, info)
		}
	} else {
		// Both claim replica — no primary known. Keep existing, log ambiguity.
		glog.Warningf("block registry: reconcile %q: AMBIGUOUS same epoch %d, neither claims primary — keeping existing %s, adding new %s as replica",
			name, newEpoch, existing.VolumeServer, newServer)
		r.upsertServerAsReplica(name, existing, newServer, info)
	}
}

// demoteExistingToReplica swaps the primary: new server becomes primary,
// old primary becomes a replica. Called during restart reconciliation.
// Caller must hold r.mu.
func (r *BlockVolumeRegistry) demoteExistingToReplica(name string, existing *BlockVolumeEntry, newServer string, info *master_pb.BlockVolumeInfoMessage) {
	oldServer := existing.VolumeServer
	oldPath := existing.Path

	// Save old primary as replica.
	oldReplica := ReplicaInfo{
		Server:        oldServer,
		Path:          oldPath,
		ISCSIAddr:     existing.ISCSIAddr,
		IQN:           existing.IQN,
		NvmeAddr:      existing.NvmeAddr,
		NQN:           existing.NQN,
		HealthScore:   existing.HealthScore,
		WALHeadLSN:    existing.WALHeadLSN,
		LastHeartbeat: existing.LastLeaseGrant,
		Role:          blockvol.RoleToWire(blockvol.RoleReplica),
	}

	// Update entry to reflect new primary.
	existing.VolumeServer = newServer
	existing.Path = info.Path
	existing.Epoch = info.Epoch
	existing.Role = info.Role
	existing.HealthScore = info.HealthScore
	existing.WALHeadLSN = info.WalHeadLsn
	existing.LastLeaseGrant = time.Now()
	if info.DurabilityMode != "" {
		existing.DurabilityMode = info.DurabilityMode
	}
	existing.NvmeAddr = info.NvmeAddr
	existing.NQN = info.Nqn
	applyExplicitPrimaryTruthFromHeartbeat(existing, info, false)

	// Add old primary as replica.
	existing.Replicas = append(existing.Replicas, oldReplica)
	r.addToServer(newServer, name)

	// Sync deprecated scalar fields.
	if len(existing.Replicas) == 1 {
		existing.ReplicaServer = oldReplica.Server
		existing.ReplicaPath = oldReplica.Path
	}
	existing.recomputeReplicaState()
}

// upsertServerAsReplica adds or updates the server as a replica for the existing entry.
// If the server already exists in Replicas[], its fields are updated instead of appending
// a duplicate. This prevents duplicate replica entries during restart/replay windows.
//
// The role is always set to RoleReplica regardless of what the heartbeat claims.
// A server added here has a lower epoch than the current primary — it IS a replica
// by definition. Without this override, a demoted primary that hasn't received its
// new assignment yet reports Role=primary in its heartbeat, causing the promotion
// gate (evaluatePromotionLocked Gate 3) to reject it with "wrong_role" and blocking
// automatic failover.
// Caller must hold r.mu.
func (r *BlockVolumeRegistry) upsertServerAsReplica(name string, existing *BlockVolumeEntry, newServer string, info *master_pb.BlockVolumeInfoMessage) {
	replicaRole := blockvol.RoleToWire(blockvol.RoleReplica)

	// Check for existing replica entry for this server.
	for i := range existing.Replicas {
		if existing.Replicas[i].Server == newServer {
			// Update in place — force RoleReplica regardless of heartbeat claim.
			oldData := existing.Replicas[i].DataAddr
			oldCtrl := existing.Replicas[i].CtrlAddr
			existing.Replicas[i].Path = info.Path
			if info.ReplicaDataAddr != "" {
				existing.Replicas[i].DataAddr = info.ReplicaDataAddr
			}
			if info.ReplicaCtrlAddr != "" {
				existing.Replicas[i].CtrlAddr = info.ReplicaCtrlAddr
			}
			existing.Replicas[i].HealthScore = info.HealthScore
			existing.Replicas[i].WALHeadLSN = info.WalHeadLsn
			existing.Replicas[i].LastHeartbeat = time.Now()
			existing.Replicas[i].Role = replicaRole
			existing.Replicas[i].NvmeAddr = info.NvmeAddr
			existing.Replicas[i].NQN = info.Nqn
			applyReplicaReadyFromHeartbeat(&existing.Replicas[i], info, true)
			if len(existing.Replicas) > 0 && existing.Replicas[0].Server == newServer {
				existing.ReplicaServer = existing.Replicas[0].Server
				existing.ReplicaPath = existing.Replicas[0].Path
				existing.ReplicaISCSIAddr = existing.Replicas[0].ISCSIAddr
				existing.ReplicaIQN = existing.Replicas[0].IQN
				existing.ReplicaDataAddr = existing.Replicas[0].DataAddr
				existing.ReplicaCtrlAddr = existing.Replicas[0].CtrlAddr
			}
			if (oldData == "" && existing.Replicas[i].DataAddr != "") || (oldCtrl == "" && existing.Replicas[i].CtrlAddr != "") {
				existing.NeedsPrimaryRefresh = true
			}
			return
		}
	}
	// New replica — append with forced RoleReplica.
	// CP13-8A: populate DataAddr/CtrlAddr from heartbeat so the master can
	// enqueue a Primary assignment with replica addresses for the shipper.
	ri := ReplicaInfo{
		Server:        newServer,
		Path:          info.Path,
		DataAddr:      info.ReplicaDataAddr,
		CtrlAddr:      info.ReplicaCtrlAddr,
		HealthScore:   info.HealthScore,
		WALHeadLSN:    info.WalHeadLsn,
		LastHeartbeat: time.Now(),
		Role:          replicaRole,
		NvmeAddr:      info.NvmeAddr,
		NQN:           info.Nqn,
	}
	applyReplicaReadyFromHeartbeat(&ri, info, false)
	existing.Replicas = append(existing.Replicas, ri)
	r.addToServer(newServer, name)
	if len(existing.Replicas) == 1 {
		existing.ReplicaServer = ri.Server
		existing.ReplicaPath = ri.Path
	}
	// CP13-8A: mark that a primary refresh is needed so the caller
	// can enqueue an updated Primary assignment with replica addresses.
	existing.NeedsPrimaryRefresh = true
}

// DrainPrimaryRefreshNeeded returns entries that need a primary assignment
// refresh (e.g., after a replica re-registers post-promote) and clears the flag.
// Caller must hold no lock (this acquires the write lock).
func (r *BlockVolumeRegistry) DrainPrimaryRefreshNeeded() []BlockVolumeEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	var result []BlockVolumeEntry
	for _, e := range r.volumes {
		if e.NeedsPrimaryRefresh {
			e.NeedsPrimaryRefresh = false
			result = append(result, e.clone())
		}
	}
	return result
}

// UpdateDeltaHeartbeat processes incremental new/deleted block volumes.
// Called on subsequent heartbeats (not the first).
func (r *BlockVolumeRegistry) UpdateDeltaHeartbeat(server string, added []*master_pb.BlockVolumeShortInfoMessage, removed []*master_pb.BlockVolumeShortInfoMessage) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Remove deleted volumes.
	for _, rm := range removed {
		if names, ok := r.byServer[server]; ok {
			for name := range names {
				if e := r.volumes[name]; e != nil && e.Path == rm.Path {
					delete(r.volumes, name)
					delete(names, name)
					break
				}
			}
		}
	}

	// Mark newly appeared volumes as active (if they exist in registry).
	for _, add := range added {
		if names, ok := r.byServer[server]; ok {
			for name := range names {
				if e := r.volumes[name]; e != nil && e.Path == add.Path {
					e.Status = StatusActive
					break
				}
			}
		}
	}
}

// PickServer returns the server address with the fewest block volumes.
// servers is the list of online volume server addresses.
// Returns error if no servers available.
func (r *BlockVolumeRegistry) PickServer(servers []string) (string, error) {
	if len(servers) == 0 {
		return "", fmt.Errorf("no block volume servers available")
	}
	r.mu.RLock()
	defer r.mu.RUnlock()

	best := servers[0]
	bestCount := r.countForServer(best)
	for _, s := range servers[1:] {
		c := r.countForServer(s)
		if c < bestCount {
			best = s
			bestCount = c
		}
	}
	return best, nil
}

// AcquireInflight tries to acquire a per-name create lock.
// Returns true if acquired (caller must call ReleaseInflight when done).
// Returns false if another create is already in progress for this name.
func (r *BlockVolumeRegistry) AcquireInflight(name string) bool {
	_, loaded := r.inflight.LoadOrStore(name, &inflightEntry{})
	return !loaded // true = we stored it (acquired), false = already existed
}

// ReleaseInflight releases the per-name create lock.
func (r *BlockVolumeRegistry) ReleaseInflight(name string) {
	r.inflight.Delete(name)
}

// IsInflight returns true if a create is in progress for the given volume name.
func (r *BlockVolumeRegistry) IsInflight(name string) bool {
	_, ok := r.inflight.Load(name)
	return ok
}

// countForServer returns the number of volumes on the given server.
// Caller must hold at least RLock.
func (r *BlockVolumeRegistry) countForServer(server string) int {
	if names, ok := r.byServer[server]; ok {
		return len(names)
	}
	return 0
}

func (r *BlockVolumeRegistry) addToServer(server, name string) {
	if r.byServer[server] == nil {
		r.byServer[server] = make(map[string]bool)
	}
	r.byServer[server][name] = true
}

func (r *BlockVolumeRegistry) removeFromServer(server, name string) {
	if names, ok := r.byServer[server]; ok {
		delete(names, name)
		if len(names) == 0 {
			delete(r.byServer, server)
		}
	}
}

// removeReplicaLocked removes a replica from an entry by server address.
// Caller must hold r.mu. Also syncs deprecated scalar fields.
func (r *BlockVolumeRegistry) removeReplicaLocked(entry *BlockVolumeEntry, server, name string) {
	newReplicas := make([]ReplicaInfo, 0, len(entry.Replicas))
	for _, ri := range entry.Replicas {
		if ri.Server == server {
			continue
		}
		newReplicas = append(newReplicas, ri)
	}
	entry.Replicas = newReplicas
	// Sync deprecated scalar fields.
	if len(entry.Replicas) > 0 {
		r0 := &entry.Replicas[0]
		entry.ReplicaServer = r0.Server
		entry.ReplicaPath = r0.Path
		entry.ReplicaISCSIAddr = r0.ISCSIAddr
		entry.ReplicaIQN = r0.IQN
		entry.ReplicaDataAddr = r0.DataAddr
		entry.ReplicaCtrlAddr = r0.CtrlAddr
	} else {
		entry.ReplicaServer = ""
		entry.ReplicaPath = ""
		entry.ReplicaISCSIAddr = ""
		entry.ReplicaIQN = ""
		entry.ReplicaDataAddr = ""
		entry.ReplicaCtrlAddr = ""
	}
	entry.recomputeReplicaState()
}

// SetReplica sets replica info for a registered volume.
// Deprecated: use AddReplica for new code. This method syncs both scalar and Replicas[].
func (r *BlockVolumeRegistry) SetReplica(name, server, path, iscsiAddr, iqn string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.volumes[name]
	if !ok {
		return fmt.Errorf("block volume %q not found", name)
	}
	// Remove old replica from byServer index before replacing.
	if entry.ReplicaServer != "" && entry.ReplicaServer != server {
		r.removeFromServer(entry.ReplicaServer, name)
	}
	entry.ReplicaServer = server
	entry.ReplicaPath = path
	entry.ReplicaISCSIAddr = iscsiAddr
	entry.ReplicaIQN = iqn
	r.addToServer(server, name)

	// CP8-2: also sync to Replicas[].
	info := ReplicaInfo{Server: server, Path: path, ISCSIAddr: iscsiAddr, IQN: iqn}
	replaced := false
	for i := range entry.Replicas {
		if entry.Replicas[i].Server == server {
			// Preserve existing health/LSN data.
			info.HealthScore = entry.Replicas[i].HealthScore
			info.WALHeadLSN = entry.Replicas[i].WALHeadLSN
			info.DataAddr = entry.Replicas[i].DataAddr
			info.CtrlAddr = entry.Replicas[i].CtrlAddr
			info.Ready = entry.Replicas[i].Ready
			entry.Replicas[i] = info
			replaced = true
			break
		}
	}
	if !replaced {
		entry.Replicas = append(entry.Replicas, info)
	}
	entry.recomputeReplicaState()
	return nil
}

// ClearReplica removes all replica info for a registered volume.
// Deprecated: use RemoveReplica for new code. This method clears both scalar and Replicas[].
func (r *BlockVolumeRegistry) ClearReplica(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.volumes[name]
	if !ok {
		return fmt.Errorf("block volume %q not found", name)
	}
	if entry.ReplicaServer != "" {
		r.removeFromServer(entry.ReplicaServer, name)
	}
	// Remove all replicas from byServer index.
	for _, ri := range entry.Replicas {
		if ri.Server != entry.ReplicaServer {
			r.removeFromServer(ri.Server, name)
		}
	}
	entry.ReplicaServer = ""
	entry.ReplicaPath = ""
	entry.ReplicaISCSIAddr = ""
	entry.ReplicaIQN = ""
	entry.ReplicaDataAddr = ""
	entry.ReplicaCtrlAddr = ""
	entry.Replicas = nil
	entry.recomputeReplicaState()
	return nil
}

// SwapPrimaryReplica promotes the replica to primary and clears the old replica.
// The old primary becomes the new replica (if it reconnects, rebuild will handle it).
// Epoch is atomically computed as entry.Epoch+1 inside the lock (R2-F5).
// Returns the new epoch for use in assignment messages.
func (r *BlockVolumeRegistry) SwapPrimaryReplica(name string) (uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.volumes[name]
	if !ok {
		return 0, fmt.Errorf("block volume %q not found", name)
	}
	if entry.ReplicaServer == "" {
		return 0, fmt.Errorf("block volume %q has no replica", name)
	}

	// Remove old primary from byServer index.
	r.removeFromServer(entry.VolumeServer, name)

	oldPrimaryServer := entry.VolumeServer
	oldPrimaryPath := entry.Path
	oldPrimaryIQN := entry.IQN
	oldPrimaryISCSI := entry.ISCSIAddr

	// Atomically bump epoch inside lock (R2-F5: prevents race with heartbeat updates).
	newEpoch := entry.Epoch + 1

	// Promote replica to primary.
	entry.VolumeServer = entry.ReplicaServer
	entry.Path = entry.ReplicaPath
	entry.IQN = entry.ReplicaIQN
	entry.ISCSIAddr = entry.ReplicaISCSIAddr
	entry.Epoch = newEpoch
	entry.Role = blockvol.RoleToWire(blockvol.RolePrimary) // R2-F3
	entry.LastLeaseGrant = time.Now()

	// Old primary becomes stale replica (will be rebuilt when it reconnects).
	entry.ReplicaServer = oldPrimaryServer
	entry.ReplicaPath = oldPrimaryPath
	entry.ReplicaIQN = oldPrimaryIQN
	entry.ReplicaISCSIAddr = oldPrimaryISCSI
	entry.ReplicaDataAddr = ""
	entry.ReplicaCtrlAddr = ""
	entry.ReplicaReady = false
	entry.ReplicaDegraded = true
	entry.TransportDegraded = false

	// Update byServer index: new primary server now hosts this volume.
	r.addToServer(entry.VolumeServer, name)
	entry.recomputeReplicaState()
	return newEpoch, nil
}

// AddReplica adds or replaces a replica in the Replicas slice (by server).
// Also updates the byServer index and deprecated scalar fields for backward compat.
func (r *BlockVolumeRegistry) AddReplica(name string, info ReplicaInfo) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.volumes[name]
	if !ok {
		return fmt.Errorf("block volume %q not found", name)
	}
	// Replace if same server already exists.
	replaced := false
	for i := range entry.Replicas {
		if entry.Replicas[i].Server == info.Server {
			entry.Replicas[i] = info
			replaced = true
			break
		}
	}
	if !replaced {
		entry.Replicas = append(entry.Replicas, info)
	}
	r.addToServer(info.Server, name)

	// Sync deprecated scalar fields (first replica → scalar).
	if len(entry.Replicas) > 0 {
		r0 := &entry.Replicas[0]
		entry.ReplicaServer = r0.Server
		entry.ReplicaPath = r0.Path
		entry.ReplicaISCSIAddr = r0.ISCSIAddr
		entry.ReplicaIQN = r0.IQN
		entry.ReplicaDataAddr = r0.DataAddr
		entry.ReplicaCtrlAddr = r0.CtrlAddr
	}
	entry.recomputeReplicaState()
	return nil
}

// RemoveReplica removes a replica by server address.
func (r *BlockVolumeRegistry) RemoveReplica(name, server string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.volumes[name]
	if !ok {
		return fmt.Errorf("block volume %q not found", name)
	}
	found := false
	newReplicas := make([]ReplicaInfo, 0, len(entry.Replicas))
	for _, ri := range entry.Replicas {
		if ri.Server == server {
			found = true
			r.removeFromServer(server, name)
			continue
		}
		newReplicas = append(newReplicas, ri)
	}
	if !found {
		return fmt.Errorf("replica on %q not found for volume %q", server, name)
	}
	entry.Replicas = newReplicas

	// Sync deprecated scalar fields.
	if len(entry.Replicas) > 0 {
		r0 := &entry.Replicas[0]
		entry.ReplicaServer = r0.Server
		entry.ReplicaPath = r0.Path
		entry.ReplicaISCSIAddr = r0.ISCSIAddr
		entry.ReplicaIQN = r0.IQN
		entry.ReplicaDataAddr = r0.DataAddr
		entry.ReplicaCtrlAddr = r0.CtrlAddr
	} else {
		entry.ReplicaServer = ""
		entry.ReplicaPath = ""
		entry.ReplicaISCSIAddr = ""
		entry.ReplicaIQN = ""
		entry.ReplicaDataAddr = ""
		entry.ReplicaCtrlAddr = ""
	}
	entry.recomputeReplicaState()
	return nil
}

// SetPromotionLSNTolerance configures the max WAL LSN lag for promotion eligibility.
func (r *BlockVolumeRegistry) SetPromotionLSNTolerance(tolerance uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.promotionLSNTolerance = tolerance
}

// PromotionLSNTolerance returns the current promotion LSN tolerance.
func (r *BlockVolumeRegistry) PromotionLSNTolerance() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.promotionLSNTolerance
}

// PromotionRejection records why a specific replica was rejected for promotion.
type PromotionRejection struct {
	Server string
	Reason string // "stale_heartbeat", "wal_lag", "wrong_role", "server_dead"
}

// PromotionPreflightResult is the reusable result of a promotion evaluation.
// Used by auto-promotion, manual promote API, preflight status, and logging.
type PromotionPreflightResult struct {
	VolumeName   string
	Promotable   bool                 // true if a candidate was found
	Candidate    *ReplicaInfo         // best candidate (nil if !Promotable)
	CandidateIdx int                  // index in Replicas[] (-1 if !Promotable)
	Rejections   []PromotionRejection // why each non-candidate was rejected
	Reason       string               // human-readable summary when !Promotable
}

// evaluatePromotionLocked evaluates promotion candidates for a volume.
// Caller must hold r.mu (read or write). Returns a preflight result without
// mutating the registry. The four gates:
//  1. Heartbeat freshness (within 2×LeaseTTL)
//  2. WAL LSN recency (within promotionLSNTolerance of primary)
//  3. Role must be RoleReplica (not RoleRebuilding)
//  4. Server must be in blockServers (alive) — fixes B-12
func (r *BlockVolumeRegistry) evaluatePromotionLocked(entry *BlockVolumeEntry) PromotionPreflightResult {
	result := PromotionPreflightResult{
		VolumeName:   entry.Name,
		CandidateIdx: -1,
	}
	if len(entry.Replicas) == 0 {
		result.Reason = "no replicas"
		return result
	}

	now := time.Now()
	freshnessCutoff := 2 * entry.LeaseTTL
	if freshnessCutoff == 0 {
		freshnessCutoff = 60 * time.Second
	}
	primaryLSN := entry.WALHeadLSN
	// EC-6 fix: when the primary is dead, its last-reported WALHeadLSN
	// includes entries that were fsync'd locally but never shipped.
	// The replica can never catch up because the primary is gone.
	// Skip the WAL LSN gate so the best available replica is promoted,
	// accepting that the last few unshipped entries may be lost.
	primaryAlive := r.blockServers[entry.VolumeServer] != nil

	bestIdx := -1
	for i := range entry.Replicas {
		ri := &entry.Replicas[i]

		// Gate 1: heartbeat freshness. Zero means never heartbeated — unsafe
		// to promote because the registry has no proof the replica is alive,
		// caught up, or fully initialized.
		if ri.LastHeartbeat.IsZero() {
			result.Rejections = append(result.Rejections, PromotionRejection{
				Server: ri.Server,
				Reason: "no_heartbeat",
			})
			continue
		}
		if now.Sub(ri.LastHeartbeat) > freshnessCutoff {
			result.Rejections = append(result.Rejections, PromotionRejection{
				Server: ri.Server,
				Reason: "stale_heartbeat",
			})
			continue
		}
		// Gate 2: WAL LSN recency.
		// Skip if primary LSN is 0 (no data yet — all eligible).
		// EC-6 fix: also skip if primary is dead — its LSN is stale and
		// the replica can never catch up. Promote the best available.
		if primaryAlive && primaryLSN > 0 && ri.WALHeadLSN+r.promotionLSNTolerance < primaryLSN {
			result.Rejections = append(result.Rejections, PromotionRejection{
				Server: ri.Server,
				Reason: "wal_lag",
			})
			continue
		}
		// Gate 3: role must be exactly RoleReplica. Zero/unset role means
		// the replica was created but never confirmed its role via heartbeat.
		if blockvol.RoleFromWire(ri.Role) != blockvol.RoleReplica {
			result.Rejections = append(result.Rejections, PromotionRejection{
				Server: ri.Server,
				Reason: "wrong_role",
			})
			continue
		}
		// Gate 4: server must be alive (in blockServers set) — B-12 fix.
		if r.blockServers[ri.Server] == nil {
			result.Rejections = append(result.Rejections, PromotionRejection{
				Server: ri.Server,
				Reason: "server_dead",
			})
			continue
		}
		// Eligible — pick best by health score, tie-break by WALHeadLSN.
		if bestIdx == -1 {
			bestIdx = i
		} else if ri.HealthScore > entry.Replicas[bestIdx].HealthScore {
			bestIdx = i
		} else if ri.HealthScore == entry.Replicas[bestIdx].HealthScore &&
			ri.WALHeadLSN > entry.Replicas[bestIdx].WALHeadLSN {
			bestIdx = i
		}
	}

	if bestIdx == -1 {
		result.Reason = "no eligible replicas"
		if len(result.Rejections) > 0 {
			result.Reason += ": " + result.Rejections[0].Reason
			if len(result.Rejections) > 1 {
				result.Reason += fmt.Sprintf(" (+%d more)", len(result.Rejections)-1)
			}
		}
		return result
	}

	result.Promotable = true
	ri := entry.Replicas[bestIdx]
	result.Candidate = &ri
	result.CandidateIdx = bestIdx
	return result
}

// EvaluatePromotion returns a read-only preflight result for the named volume
// without mutating the registry. Safe for status/logging/manual promote preview.
func (r *BlockVolumeRegistry) EvaluatePromotion(name string) (PromotionPreflightResult, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	entry, ok := r.volumes[name]
	if !ok {
		return PromotionPreflightResult{VolumeName: name, Reason: "volume not found"}, fmt.Errorf("block volume %q not found", name)
	}
	return r.evaluatePromotionLocked(entry), nil
}

// applyPromotionLocked applies the promotion of a replica at candidateIdx to primary.
// Caller must hold r.mu (write lock). The promoted replica is removed from Replicas[].
// Old primary is NOT added to Replicas (needs rebuild). Returns the new epoch.
func (r *BlockVolumeRegistry) applyPromotionLocked(entry *BlockVolumeEntry, name string, candidate ReplicaInfo, candidateIdx int) uint64 {
	// Remove old primary from byServer index.
	r.removeFromServer(entry.VolumeServer, name)

	// Bump epoch atomically.
	newEpoch := entry.Epoch + 1

	// Promote replica to primary.
	entry.VolumeServer = candidate.Server
	entry.Path = candidate.Path
	entry.IQN = candidate.IQN
	entry.ISCSIAddr = candidate.ISCSIAddr
	entry.NvmeAddr = candidate.NvmeAddr
	entry.NQN = candidate.NQN
	entry.Epoch = newEpoch
	entry.Role = blockvol.RoleToWire(blockvol.RolePrimary)
	entry.LastLeaseGrant = time.Now()
	entry.PendingPrimaryHeartbeat = true
	entry.HealthScore = candidate.HealthScore
	entry.WALHeadLSN = candidate.WALHeadLSN

	// Clear stale rebuild/publication metadata from old primary (B-11 partial fix).
	entry.RebuildListenAddr = ""
	entry.NeedsRebuild = false
	entry.HasNeedsRebuild = false
	entry.PublishHealthy = false
	entry.HasPublishHealthy = false
	entry.HeartbeatVolumeMode = ""
	entry.HasHeartbeatVolumeMode = false
	entry.HeartbeatVolumeReason = ""
	entry.HasHeartbeatVolumeReason = false
	entry.EngineProjectionMode = ""
	entry.HasEngineProjectionMode = false

	// Remove promoted from Replicas. Others stay.
	entry.Replicas = append(entry.Replicas[:candidateIdx], entry.Replicas[candidateIdx+1:]...)

	// Sync deprecated scalar fields.
	if len(entry.Replicas) > 0 {
		r0 := &entry.Replicas[0]
		entry.ReplicaServer = r0.Server
		entry.ReplicaPath = r0.Path
		entry.ReplicaISCSIAddr = r0.ISCSIAddr
		entry.ReplicaIQN = r0.IQN
		entry.ReplicaDataAddr = r0.DataAddr
		entry.ReplicaCtrlAddr = r0.CtrlAddr
	} else {
		entry.ReplicaServer = ""
		entry.ReplicaPath = ""
		entry.ReplicaISCSIAddr = ""
		entry.ReplicaIQN = ""
		entry.ReplicaDataAddr = ""
		entry.ReplicaCtrlAddr = ""
	}

	// Update byServer index: new primary server now hosts this volume.
	r.addToServer(entry.VolumeServer, name)
	entry.recomputeReplicaState()

	return newEpoch
}

// PromoteBestReplica promotes the best eligible replica to primary.
// Eligibility: heartbeat fresh (within 2×LeaseTTL), WALHeadLSN within tolerance of primary,
// role must be RoleReplica (not RoleRebuilding), and server must be alive (B-12 fix).
// The promoted replica is removed from Replicas[]. Other replicas stay.
// Old primary is NOT added to Replicas (needs rebuild).
// Returns the new epoch and the preflight result.
func (r *BlockVolumeRegistry) PromoteBestReplica(name string) (uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.volumes[name]
	if !ok {
		return 0, fmt.Errorf("block volume %q not found", name)
	}

	pf := r.evaluatePromotionLocked(entry)
	if !pf.Promotable {
		return 0, fmt.Errorf("block volume %q: %s", name, pf.Reason)
	}

	promoted := *pf.Candidate
	bestIdx := pf.CandidateIdx

	newEpoch := r.applyPromotionLocked(entry, name, promoted, bestIdx)
	return newEpoch, nil
}

// PromoteReplicaByServer promotes a specific replica identified by server
// address to primary. Used by T3 V2 path where the master already selected
// the winner via fresh evidence. Bypasses V1 eligibility gates since the
// caller (V2 evidence path) owns eligibility determination.
func (r *BlockVolumeRegistry) PromoteReplicaByServer(name, server string) (uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.volumes[name]
	if !ok {
		return 0, fmt.Errorf("block volume %q not found", name)
	}
	for i, ri := range entry.Replicas {
		if ri.Server == server {
			newEpoch := r.applyPromotionLocked(entry, name, ri, i)
			return newEpoch, nil
		}
	}
	return 0, fmt.Errorf("block volume %q: replica server %q not found", name, server)
}

// evaluateManualPromotionLocked evaluates promotion candidates for a manual promote request.
// Caller must hold r.mu (read or write).
//
// Differences from evaluatePromotionLocked:
//   - Primary-alive gate: if !force and current primary is alive, reject with "primary_alive".
//   - Target filtering: if targetServer != "", only evaluate that specific replica.
//     Returns Reason="target_not_found" if that server is not a replica.
//   - Force flag: bypasses soft gates (primary_alive, stale_heartbeat, wal_lag)
//     but keeps hard gates (no_heartbeat with zero time, wrong_role, server_dead).
//
// Gate table:
//
//	Gate             | Normal | Force
//	primary_alive    | Reject | Skip
//	no_heartbeat(0)  | Reject | Reject
//	stale_heartbeat  | Reject | Skip
//	wal_lag          | Reject | Skip
//	wrong_role       | Reject | Reject
//	server_dead      | Reject | Reject
func (r *BlockVolumeRegistry) evaluateManualPromotionLocked(entry *BlockVolumeEntry, targetServer string, force bool) PromotionPreflightResult {
	result := PromotionPreflightResult{
		VolumeName:   entry.Name,
		CandidateIdx: -1,
	}

	// Primary-alive gate (soft — skipped when force=true).
	if !force && r.blockServers[entry.VolumeServer] != nil {
		result.Reason = "primary_alive"
		return result
	}

	if len(entry.Replicas) == 0 {
		result.Reason = "no replicas"
		return result
	}

	// Target filtering: if a specific server is requested, find its index first.
	// Return early if not found.
	if targetServer != "" {
		found := false
		for i := range entry.Replicas {
			if entry.Replicas[i].Server == targetServer {
				found = true
				break
			}
		}
		if !found {
			result.Reason = "target_not_found"
			return result
		}
	}

	now := time.Now()
	freshnessCutoff := 2 * entry.LeaseTTL
	if freshnessCutoff == 0 {
		freshnessCutoff = 60 * time.Second
	}
	primaryLSN := entry.WALHeadLSN

	bestIdx := -1
	for i := range entry.Replicas {
		ri := &entry.Replicas[i]

		// If targeting a specific server, skip all others.
		if targetServer != "" && ri.Server != targetServer {
			continue
		}

		// Hard gate: no heartbeat (zero time) — unsafe regardless of force.
		if ri.LastHeartbeat.IsZero() {
			result.Rejections = append(result.Rejections, PromotionRejection{
				Server: ri.Server,
				Reason: "no_heartbeat",
			})
			continue
		}

		// Soft gate: stale heartbeat — skipped when force=true.
		if !force && now.Sub(ri.LastHeartbeat) > freshnessCutoff {
			result.Rejections = append(result.Rejections, PromotionRejection{
				Server: ri.Server,
				Reason: "stale_heartbeat",
			})
			continue
		}

		// Soft gate: WAL lag — skipped when force=true.
		if !force && primaryLSN > 0 && ri.WALHeadLSN+r.promotionLSNTolerance < primaryLSN {
			result.Rejections = append(result.Rejections, PromotionRejection{
				Server: ri.Server,
				Reason: "wal_lag",
			})
			continue
		}

		// Hard gate: role must be exactly RoleReplica.
		if blockvol.RoleFromWire(ri.Role) != blockvol.RoleReplica {
			result.Rejections = append(result.Rejections, PromotionRejection{
				Server: ri.Server,
				Reason: "wrong_role",
			})
			continue
		}

		// Hard gate: server must be alive (in blockServers set).
		if r.blockServers[ri.Server] == nil {
			result.Rejections = append(result.Rejections, PromotionRejection{
				Server: ri.Server,
				Reason: "server_dead",
			})
			continue
		}

		// Eligible — pick best by health score, tie-break by WALHeadLSN.
		if bestIdx == -1 {
			bestIdx = i
		} else if ri.HealthScore > entry.Replicas[bestIdx].HealthScore {
			bestIdx = i
		} else if ri.HealthScore == entry.Replicas[bestIdx].HealthScore &&
			ri.WALHeadLSN > entry.Replicas[bestIdx].WALHeadLSN {
			bestIdx = i
		}
	}

	if bestIdx == -1 {
		result.Reason = "no eligible replicas"
		if len(result.Rejections) > 0 {
			result.Reason += ": " + result.Rejections[0].Reason
			if len(result.Rejections) > 1 {
				result.Reason += fmt.Sprintf(" (+%d more)", len(result.Rejections)-1)
			}
		}
		return result
	}

	result.Promotable = true
	ri := entry.Replicas[bestIdx]
	result.Candidate = &ri
	result.CandidateIdx = bestIdx
	return result
}

// ManualPromote promotes a specific replica (or the best eligible replica) to primary.
// Unlike PromoteBestReplica, it accepts operator overrides:
//   - targetServer: if non-empty, only that replica is considered.
//   - force: bypasses soft gates (primary_alive, stale_heartbeat, wal_lag).
//
// Returns (newEpoch, oldPrimary, oldPath, preflightResult, nil) on success.
// oldPrimary and oldPath are captured under the lock to avoid TOCTOU with
// concurrent auto-failover (BUG-T5-2 fix).
// Returns (0, "", "", preflightResult, err) on rejection or lookup failure.
func (r *BlockVolumeRegistry) ManualPromote(name, targetServer string, force bool) (uint64, string, string, PromotionPreflightResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	entry, ok := r.volumes[name]
	if !ok {
		return 0, "", "", PromotionPreflightResult{VolumeName: name, Reason: "volume not found"},
			fmt.Errorf("block volume %q not found", name)
	}

	// Capture old primary info under lock (BUG-T5-2 fix).
	oldPrimary := entry.VolumeServer
	oldPath := entry.Path

	pf := r.evaluateManualPromotionLocked(entry, targetServer, force)
	if !pf.Promotable {
		return 0, "", "", pf, fmt.Errorf("block volume %q: %s", name, pf.Reason)
	}

	promoted := *pf.Candidate
	candidateIdx := pf.CandidateIdx

	newEpoch := r.applyPromotionLocked(entry, name, promoted, candidateIdx)
	return newEpoch, oldPrimary, oldPath, pf, nil
}

// MarkBlockCapable records that the given server supports block volumes.
func (r *BlockVolumeRegistry) MarkBlockCapable(server string) {
	r.mu.Lock()
	if r.blockServers[server] == nil {
		r.blockServers[server] = &blockServerInfo{}
	}
	r.mu.Unlock()
}

// UnmarkBlockCapable removes a server from the block-capable set.
func (r *BlockVolumeRegistry) UnmarkBlockCapable(server string) {
	r.mu.Lock()
	delete(r.blockServers, server)
	r.mu.Unlock()
}

// LeaseGrant holds the minimal fields for a lease renewal.
type LeaseGrant struct {
	Path       string
	Epoch      uint64
	Role       uint32
	LeaseTtlMs uint32
}

// LeaseGrants generates lightweight lease renewals for all active primary
// volumes on a server. Only primaries need lease renewal — replicas are passive
// WAL receivers without a write lease. Grants carry path + epoch + role + TTL
// and are processed by HandleAssignment's same-role refresh path, which
// validates the epoch and calls lease.Grant().
// Volumes with a pending assignment are excluded (the full assignment handles lease).
func (r *BlockVolumeRegistry) LeaseGrants(server string, pendingPaths map[string]bool) []LeaseGrant {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names, ok := r.byServer[server]
	if !ok {
		return nil
	}
	var grants []LeaseGrant
	for name := range names {
		e := r.volumes[name]
		if e == nil || e.Status != StatusActive {
			continue
		}
		// Only primaries need lease renewal. Replicas are passive WAL receivers
		// and don't hold a write lease.
		if blockvol.RoleFromWire(e.Role) != blockvol.RolePrimary {
			continue
		}
		// Primary must be on this server.
		if e.VolumeServer != server {
			continue
		}
		if pendingPaths[e.Path] {
			continue
		}
		grants = append(grants, LeaseGrant{
			Path:       e.Path,
			Epoch:      e.Epoch,
			Role:       e.Role,
			LeaseTtlMs: blockvol.LeaseTTLToWire(e.LeaseTTL),
		})
	}
	return grants
}

// ListAll returns all registered block volume entries, sorted by name.
func (r *BlockVolumeRegistry) ListAll() []BlockVolumeEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	entries := make([]BlockVolumeEntry, 0, len(r.volumes))
	for _, e := range r.volumes {
		entries = append(entries, e.clone())
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
	return entries
}

// BlockServerSummary summarizes a block-capable volume server.
type BlockServerSummary struct {
	Address      string
	VolumeCount  int
	BlockCapable bool
}

// ServerSummaries returns a summary for each block-capable server.
func (r *BlockVolumeRegistry) ServerSummaries() []BlockServerSummary {
	r.mu.RLock()
	defer r.mu.RUnlock()
	summaries := make([]BlockServerSummary, 0, len(r.blockServers))
	for addr := range r.blockServers {
		count := 0
		if names, ok := r.byServer[addr]; ok {
			count = len(names)
		}
		summaries = append(summaries, BlockServerSummary{
			Address:      addr,
			VolumeCount:  count,
			BlockCapable: true,
		})
	}
	sort.Slice(summaries, func(i, j int) bool { return summaries[i].Address < summaries[j].Address })
	return summaries
}

// PlacementCandidates returns enriched candidate information for placement planning.
// This is the bridge point between the registry and the placement planner.
// Long-term, this would be replaced by topology-backed candidate gathering.
func (r *BlockVolumeRegistry) PlacementCandidates() []PlacementCandidateInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	candidates := make([]PlacementCandidateInfo, 0, len(r.blockServers))
	for addr, info := range r.blockServers {
		count := 0
		if names, ok := r.byServer[addr]; ok {
			count = len(names)
		}
		c := PlacementCandidateInfo{
			Address:     addr,
			VolumeCount: count,
		}
		if info != nil {
			c.NvmeCapable = info.NvmeAddr != ""
			c.DiskType = info.DiskType
			c.AvailableBytes = info.AvailableBytes
		}
		candidates = append(candidates, c)
	}
	return candidates
}

// IsBlockCapable returns true if the given server is in the block-capable set (alive).
func (r *BlockVolumeRegistry) IsBlockCapable(server string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.blockServers[server] != nil
}

// VolumesWithDeadPrimary returns names of volumes where the given server is a replica
// and the current primary is NOT in the block-capable set (dead/disconnected).
// Used by T2 (B-06) to detect orphaned primaries that need re-promotion.
func (r *BlockVolumeRegistry) VolumesWithDeadPrimary(replicaServer string) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names, ok := r.byServer[replicaServer]
	if !ok {
		return nil
	}
	var orphaned []string
	for name := range names {
		entry := r.volumes[name]
		if entry == nil {
			continue
		}
		// Only consider volumes where this server is a replica (not the primary).
		if entry.VolumeServer == replicaServer {
			continue
		}
		// Check if the primary server is dead.
		if r.blockServers[entry.VolumeServer] == nil {
			orphaned = append(orphaned, name)
		}
	}
	return orphaned
}

// HasNVMeCapableServer returns true if any registered block-capable server
// has reported a non-empty NVMe address via heartbeat.
func (r *BlockVolumeRegistry) HasNVMeCapableServer() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, info := range r.blockServers {
		if info != nil && info.NvmeAddr != "" {
			return true
		}
	}
	return false
}

// BlockCapableServers returns the list of servers known to support block volumes.
func (r *BlockVolumeRegistry) BlockCapableServers() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	servers := make([]string, 0, len(r.blockServers))
	for s := range r.blockServers {
		servers = append(servers, s)
	}
	return servers
}

// MaxBarrierLagLSN returns the maximum WAL lag across all volumes and replicas.
// This is the primary durability-risk metric: primary WALHeadLSN minus
// replica's WALHeadLSN. SLO threshold: < 100 under normal load.
func (r *BlockVolumeRegistry) MaxBarrierLagLSN() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var maxLag uint64
	for _, entry := range r.volumes {
		for _, ri := range entry.Replicas {
			if ri.WALLag > maxLag {
				maxLag = ri.WALLag
			}
		}
	}
	return maxLag
}

// AssignmentQueueDepth returns the total number of pending assignments across all servers.
func (r *BlockVolumeRegistry) AssignmentQueueDepth() int {
	// Delegated to the assignment queue, not the registry.
	// Placeholder: the queue tracks its own depth.
	return 0
}

// nameFromPath extracts the volume name from a .blk file path.
// e.g. "/opt/data/block/my-volume.blk" -> "my-volume"
func nameFromPath(path string) string {
	base := filepath.Base(path)
	if strings.HasSuffix(base, ".blk") {
		return strings.TrimSuffix(base, ".blk")
	}
	return base
}
