package weed_server

import (
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"

	bridgeblockvol "github.com/seaweedfs/seaweedfs/sw-block/bridge/blockvol"
	engine "github.com/seaweedfs/seaweedfs/sw-block/engine/replication"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/server/blockcmd"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/iscsi"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/nvme"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/v2bridge"
)

// volReplState tracks active replication addresses per volume.
type volReplState struct {
	replicaDataAddr string
	replicaCtrlAddr string
	// allReplicas stores the full replica set for multi-replica idempotence.
	allReplicas       []blockvol.ReplicaAddr
	roleApplied       bool
	receiverReady     bool
	shipperConfigured bool
	replicaEligible   bool
	publishHealthy    bool
}

// BlockReadinessSnapshot names the assignment-to-publication closure at the
// BlockService boundary. These flags are owned by the service/adapter layer,
// not by blockvol's local storage mechanics.
//
// Important:
// PublishHealthy here is the server-boundary mirror of the core-owned
// publication health when a core projection exists. Adapter-local fallback
// remains only for paths where the core is absent.
type BlockReadinessSnapshot struct {
	RoleApplied       bool
	ReceiverReady     bool
	ShipperConfigured bool
	ShipperConnected  bool
	ReplicaEligible   bool
	PublishHealthy    bool
}

// NVMeConfig holds NVMe/TCP target configuration passed from CLI flags.
type NVMeConfig struct {
	Enabled     bool
	ListenAddr  string
	Portal      string // reserved for heartbeat/CSI integration (CP10-2)
	NQNPrefix   string
	MaxIOQueues int
}

// BlockService manages block volumes and the iSCSI/NVMe target servers.
type BlockService struct {
	blockStore     *storage.BlockVolumeStore
	targetServer   *iscsi.TargetServer
	nvmeServer     *nvme.Server
	iqnPrefix      string
	nqnPrefix      string
	blockDir       string
	listenAddr     string
	nvmeListenAddr string

	// Replication state (CP6-3).
	replMu     sync.RWMutex
	replStates map[string]*volReplState // keyed by volume path

	// V2 engine bridge (Phase 08 P1).
	v2Bridge       *v2bridge.ControlBridge
	v2Orchestrator *engine.RecoveryOrchestrator
	v2Core         *engine.CoreEngine
	v2Recovery     *RecoveryManager
	coreProjMu     sync.RWMutex
	coreProj       map[string]engine.PublicationProjection
	coreExecMu     sync.RWMutex
	coreExec       map[string][]string
	protocolExecMu sync.RWMutex
	protocolExec   map[string]volumeProtocolExecutionState

	// T4: activation gate — promoted primaries that have not passed
	// reconstruction quality check are gated from serving.
	activationGateMu sync.RWMutex
	activationGated  map[string]string // path → reason (non-empty = gated)

	// P3: last-applied assignment per volume path for idempotence.
	lastAssignMu sync.RWMutex
	lastAssign   map[string]lastAppliedAssignment
	// localServerID: stable identity for this volume server.
	// May be an opaque string (from -id flag) or ip:port (default fallback).
	// NOT guaranteed to be a routable address — do not use for transport endpoints.
	localServerID string

	// advertisedHost: routable host for this volume server (from -ip flag or auto-detected).
	// Used by CP13-2 to canonicalize wildcard-bind replica listener addresses to
	// routable host:port. This is the -ip value (IP or resolvable hostname),
	// never an opaque server identity from -id.
	advertisedHost string
	// blockInventoryAuthoritative reports whether the in-memory block inventory is
	// authoritative enough to drive master-side stale cleanup from a full
	// heartbeat. It becomes false when startup inventory scan fails.
	blockInventoryAuthoritative bool

	// TestHook: if set, invoked when the legacy direct rebuild starter is used.
	onLegacyStartRebuild func(path, rebuildAddr string, epoch uint64)

	blockStateNotifyCh chan bool
}

// V2Orchestrator returns the V2 engine orchestrator for inspection/testing.
func (bs *BlockService) V2Orchestrator() *engine.RecoveryOrchestrator {
	return bs.v2Orchestrator
}

// V2Core returns the explicit Phase 14/15 core shell if wired.
func (bs *BlockService) V2Core() *engine.CoreEngine {
	return bs.v2Core
}

// BlockInventoryAuthoritative reports whether the current block inventory can be
// treated as authoritative for full-heartbeat stale cleanup.
func (bs *BlockService) BlockInventoryAuthoritative() bool {
	if bs == nil {
		return false
	}
	return bs.blockInventoryAuthoritative
}

// CoreProjection returns the latest adapter-cached projection emitted by the
// explicit V2 core on the narrow live path.
func (bs *BlockService) CoreProjection(path string) (engine.PublicationProjection, bool) {
	bs.coreProjMu.RLock()
	defer bs.coreProjMu.RUnlock()
	if bs.coreProj == nil {
		return engine.PublicationProjection{}, false
	}
	proj, ok := bs.coreProj[path]
	return proj, ok
}

// ExecutedCoreCommands returns the bounded list of core commands executed on the
// current integrated path for one volume. Intended for focused runtime-ownership
// proofs in Phase 16.
func (bs *BlockService) ExecutedCoreCommands(path string) []string {
	bs.coreExecMu.RLock()
	defer bs.coreExecMu.RUnlock()
	if bs.coreExec == nil {
		return nil
	}
	cmds := bs.coreExec[path]
	out := make([]string, len(cmds))
	copy(out, cmds)
	return out
}

// CoreProjectionMismatches reports fields that should already agree on the
// current bounded core-present path but do not.
func (bs *BlockService) CoreProjectionMismatches(path string) []string {
	proj, ok := bs.CoreProjection(path)
	if !ok {
		return []string{"missing_core_projection"}
	}
	readiness := bs.ReadinessSnapshot(path)
	var mismatches []string
	if readiness.RoleApplied != proj.Readiness.RoleApplied {
		mismatches = append(mismatches, "role_applied")
	}
	if readiness.ReceiverReady != proj.Readiness.ReceiverReady {
		mismatches = append(mismatches, "receiver_ready")
	}
	if readiness.ShipperConfigured != proj.Readiness.ShipperConfigured {
		mismatches = append(mismatches, "shipper_configured")
	}
	if readiness.ShipperConnected != proj.Readiness.ShipperConnected {
		mismatches = append(mismatches, "shipper_connected")
	}
	if readiness.PublishHealthy != proj.Publication.Healthy {
		mismatches = append(mismatches, "publish_healthy")
	}
	return mismatches
}

// SetServerID sets the stable server identity for V2 control semantics.
// This may be an opaque string (from -id flag) — not guaranteed routable.
func (bs *BlockService) SetServerID(id string) {
	bs.localServerID = id
}

// SetAdvertisedHost sets the routable host for replica endpoint canonicalization.
// This is the -ip flag value (IP address or resolvable hostname), never an
// opaque server identity from -id. Called at startup from volume.go.
func (bs *BlockService) SetAdvertisedHost(host string) {
	bs.advertisedHost = host
}

// WireStateChangeNotify sets up volume state callbacks on all registered
// volumes so that shipper transitions and durable-boundary advances trigger an
// immediate heartbeat via the provided channel. Non-blocking send (buffered
// chan 1).
func (bs *BlockService) WireStateChangeNotify(ch chan bool) {
	bs.blockStateNotifyCh = ch
	bs.blockStore.IterateBlockVolumes(func(path string, vol *blockvol.BlockVol) {
		bs.attachVolumeStateCallbacks(path, vol)
	})
}

func (bs *BlockService) attachVolumeStateCallbacks(path string, vol *blockvol.BlockVol) {
	if bs == nil || vol == nil {
		return
	}
	ch := bs.blockStateNotifyCh
	vol.SetOnShipperStateChange(func(from, to blockvol.ReplicaState) {
		bs.handleShipperStateChange(path, from, to, ch)
	})
	vol.SetOnBarrierAccepted(func(flushedLSN uint64) {
		bs.handleBarrierAccepted(path, flushedLSN, ch)
	})
	vol.SetOnBarrierRejected(func(reason string) {
		bs.handleBarrierRejected(path, reason, ch)
	})
}

func (bs *BlockService) handleShipperStateChange(path string, from, to blockvol.ReplicaState, ch chan bool) {
	if ch != nil {
		select {
		case ch <- true:
		default: // already pending
		}
	}
	glog.V(0).Infof("block service: shipper state change path=%s from=%s to=%s", path, from, to)
	if bs == nil || bs.v2Core == nil {
		return
	}
	proj, ok := bs.CoreProjection(path)
	if !ok || proj.Role != engine.RolePrimary {
		return
	}
	if to == blockvol.ReplicaInSync {
		bs.applyCoreEvent(engine.ShipperConnectedObserved{ID: path})
	}
}

func (bs *BlockService) handleBarrierAccepted(path string, flushedLSN uint64, ch chan bool) {
	if ch != nil {
		select {
		case ch <- true:
		default: // already pending
		}
	}
	if bs == nil || bs.v2Core == nil || flushedLSN == 0 {
		return
	}
	proj, ok := bs.CoreProjection(path)
	if !ok || proj.Role != engine.RolePrimary || proj.Boundary.DurableLSN >= flushedLSN {
		return
	}
	if !proj.Readiness.ShipperConnected && bs.isPrimaryShipperConnected(path) {
		bs.applyCoreEvent(engine.ShipperConnectedObserved{ID: path})
		proj, ok = bs.CoreProjection(path)
		if !ok || proj.Role != engine.RolePrimary || proj.Boundary.DurableLSN >= flushedLSN {
			return
		}
	}
	bs.applyCoreEvent(engine.BarrierAccepted{ID: path, FlushedLSN: flushedLSN})
}

func (bs *BlockService) handleBarrierRejected(path string, reason string, ch chan bool) {
	if ch != nil {
		select {
		case ch <- true:
		default: // already pending
		}
	}
	if bs == nil || bs.v2Core == nil || reason == "" {
		return
	}
	proj, ok := bs.CoreProjection(path)
	if !ok || proj.Role != engine.RolePrimary {
		return
	}
	bs.applyCoreEvent(engine.BarrierRejected{ID: path, Reason: reason})
}

// StartBlockService scans blockDir for .blk files, opens them as block volumes,
// registers them with iSCSI and optionally NVMe target servers, and starts listening.
// Returns nil if blockDir is empty (feature disabled).
func StartBlockService(listenAddr, blockDir, iqnPrefix, portalAddr string, nvmeCfg NVMeConfig) *BlockService {
	if blockDir == "" {
		return nil
	}

	if iqnPrefix == "" {
		iqnPrefix = "iqn.2024-01.com.seaweedfs:vol."
	}
	nqnPrefix := nvmeCfg.NQNPrefix
	if nqnPrefix == "" {
		nqnPrefix = "nqn.2024-01.com.seaweedfs:vol."
	}

	bs := &BlockService{
		blockStore:                  storage.NewBlockVolumeStore(),
		iqnPrefix:                   iqnPrefix,
		nqnPrefix:                   nqnPrefix,
		blockDir:                    blockDir,
		listenAddr:                  listenAddr,
		nvmeListenAddr:              nvmeCfg.ListenAddr,
		v2Bridge:                    v2bridge.NewControlBridge(),
		v2Orchestrator:              engine.NewRecoveryOrchestrator(),
		v2Core:                      engine.NewCoreEngine(),
		localServerID:               listenAddr, // INTERIM: transport-shaped, see field doc
		coreProj:                    make(map[string]engine.PublicationProjection),
		protocolExec:                make(map[string]volumeProtocolExecutionState),
		activationGated:             make(map[string]string),
		blockInventoryAuthoritative: false,
	}
	bs.v2Recovery = NewRecoveryManager(bs)

	// iSCSI target setup.
	logger := log.New(os.Stderr, "iscsi: ", log.LstdFlags)

	config := iscsi.DefaultTargetConfig()
	config.TargetName = iqnPrefix + "default"

	bs.targetServer = iscsi.NewTargetServer(listenAddr, config, logger)
	if portalAddr != "" {
		bs.targetServer.SetPortalAddr(portalAddr)
	}

	// NVMe/TCP target setup (optional).
	if nvmeCfg.Enabled {
		maxQ := uint16(4)
		if nvmeCfg.MaxIOQueues >= 1 && nvmeCfg.MaxIOQueues <= 128 {
			maxQ = uint16(nvmeCfg.MaxIOQueues)
		}
		bs.nvmeServer = nvme.NewServer(nvme.Config{
			ListenAddr:  nvmeCfg.ListenAddr,
			NQNPrefix:   nqnPrefix,
			MaxIOQueues: maxQ,
			Enabled:     true,
		})
	}

	// Scan blockDir for .blk files.
	entries, err := os.ReadDir(blockDir)
	if err != nil {
		glog.Warningf("block service: cannot read dir %s: %v", blockDir, err)
		return bs
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".blk") {
			continue
		}
		path := filepath.Join(blockDir, entry.Name())
		vol, err := bs.blockStore.AddBlockVolume(path, "")
		if err != nil {
			// Auto-initialize raw files (e.g. created via truncate).
			info, serr := entry.Info()
			if serr == nil && info.Size() > 0 {
				glog.V(0).Infof("block service: auto-creating blockvol %s (%d bytes)", path, info.Size())
				os.Remove(path) // remove raw file so CreateBlockVol can use O_EXCL
				created, cerr := blockvol.CreateBlockVol(path, blockvol.CreateOptions{
					VolumeSize: uint64(info.Size()),
				})
				if cerr != nil {
					glog.Warningf("block service: auto-create %s: %v", path, cerr)
					continue
				}
				created.Close()
				vol, err = bs.blockStore.AddBlockVolume(path, "")
				if err != nil {
					glog.Warningf("block service: skip %s after auto-create: %v", path, err)
					continue
				}
			} else {
				glog.Warningf("block service: skip %s: %v", path, err)
				continue
			}
		}

		name := strings.TrimSuffix(entry.Name(), ".blk")
		bs.registerVolume(vol, name)
	}
	bs.blockInventoryAuthoritative = true

	// Start iSCSI target in background.
	go func() {
		if err := bs.targetServer.ListenAndServe(); err != nil {
			glog.Warningf("block service: iSCSI target stopped: %v", err)
		}
	}()
	glog.V(0).Infof("block service: iSCSI target started on %s", listenAddr)

	// Start NVMe/TCP target in background (if enabled).
	if bs.nvmeServer != nil {
		if err := bs.nvmeServer.ListenAndServe(); err != nil {
			glog.Warningf("block service: NVMe/TCP target failed to start: %v (iSCSI continues)", err)
			bs.nvmeServer = nil // disable NVMe, iSCSI continues
		} else {
			glog.V(0).Infof("block service: NVMe/TCP target started on %s", nvmeCfg.ListenAddr)
		}
	}

	return bs
}

// registerVolume adds a volume to both iSCSI and NVMe targets.
func (bs *BlockService) registerVolume(vol *blockvol.BlockVol, name string) {
	iqn := bs.iqnPrefix + blockvol.SanitizeIQN(name)
	adapter := blockvol.NewBlockVolAdapter(vol)
	bs.targetServer.AddVolume(iqn, adapter)

	if bs.nvmeServer != nil {
		nqn := blockvol.BuildNQN(bs.nqnPrefix, name)
		nvmeAdapter := nvme.NewNVMeAdapter(vol)
		bs.nvmeServer.AddVolume(nqn, nvmeAdapter, nvmeAdapter.DeviceNGUID())
	}

	glog.V(0).Infof("block service: registered %s", name)
}

// Store returns the underlying BlockVolumeStore.
func (bs *BlockService) Store() *storage.BlockVolumeStore {
	return bs.blockStore
}

// BlockDir returns the block volume data directory.
func (bs *BlockService) BlockDir() string {
	return bs.blockDir
}

// ListenAddr returns the iSCSI target listen address.
func (bs *BlockService) ListenAddr() string {
	return bs.listenAddr
}

// NvmeListenAddr returns the configured NVMe/TCP target listen address, or empty if NVMe is disabled.
func (bs *BlockService) NvmeListenAddr() string {
	if bs.nvmeServer != nil {
		return bs.nvmeListenAddr
	}
	return ""
}

// NQN returns the NVMe subsystem NQN for a volume name.
func (bs *BlockService) NQN(name string) string {
	return blockvol.BuildNQN(bs.nqnPrefix, name)
}

// CreateBlockVol creates a new .blk file, registers it with BlockVolumeStore
// and iSCSI TargetServer. Returns path, IQN, iSCSI addr.
// Idempotent: if volume already exists with same or larger size, returns existing info.
func (bs *BlockService) CreateBlockVol(name string, sizeBytes uint64, diskType string, durabilityMode string) (path, iqn, iscsiAddr string, err error) {
	return bs.CreateBlockVolWithOptions(name, sizeBytes, 0, diskType, durabilityMode)
}

// CreateBlockVolWithOptions creates a new .blk file with the requested geometry,
// including an optional WAL size override.
func (bs *BlockService) CreateBlockVolWithOptions(name string, sizeBytes uint64, walSizeBytes uint64, diskType string, durabilityMode string) (path, iqn, iscsiAddr string, err error) {
	sanitized := blockvol.SanitizeFilename(name)
	path = filepath.Join(bs.blockDir, sanitized+".blk")
	iqn = bs.iqnPrefix + blockvol.SanitizeIQN(name)
	iscsiAddr = bs.listenAddr

	// Check if already registered.
	if vol, ok := bs.blockStore.GetBlockVolume(path); ok {
		bs.attachVolumeStateCallbacks(path, vol)
		info := vol.Info()
		if info.VolumeSize < sizeBytes {
			return "", "", "", fmt.Errorf("block volume %q exists with size %d (requested %d)",
				name, info.VolumeSize, sizeBytes)
		}
		// Re-add to targets in case they were cleared (crash recovery).
		// AddVolume is idempotent — no-op if already registered.
		adapter := blockvol.NewBlockVolAdapter(vol)
		bs.targetServer.AddVolume(iqn, adapter)
		if bs.nvmeServer != nil {
			nqn := blockvol.BuildNQN(bs.nqnPrefix, name)
			nvmeAdapter := nvme.NewNVMeAdapter(vol)
			bs.nvmeServer.AddVolume(nqn, nvmeAdapter, nvmeAdapter.DeviceNGUID())
		}
		return path, iqn, iscsiAddr, nil
	}

	// F2: VS-side validation — reject invalid mode strings (defense-in-depth).
	var durMode blockvol.DurabilityMode
	if durabilityMode != "" {
		var perr error
		durMode, perr = blockvol.ParseDurabilityMode(durabilityMode)
		if perr != nil {
			return "", "", "", fmt.Errorf("invalid durability mode: %w", perr)
		}
	}

	// Create the .blk file.
	if err := os.MkdirAll(bs.blockDir, 0755); err != nil {
		return "", "", "", fmt.Errorf("create block dir: %w", err)
	}
	created, err := blockvol.CreateBlockVol(path, blockvol.CreateOptions{
		VolumeSize:     sizeBytes,
		WALSize:        walSizeBytes,
		DurabilityMode: durMode,
	})
	if err != nil {
		return "", "", "", fmt.Errorf("create block volume: %w", err)
	}
	created.Close()

	// Open and register.
	vol, err := bs.blockStore.AddBlockVolume(path, diskType)
	if err != nil {
		os.Remove(path)
		return "", "", "", fmt.Errorf("register block volume: %w", err)
	}
	bs.attachVolumeStateCallbacks(path, vol)

	adapter := blockvol.NewBlockVolAdapter(vol)
	bs.targetServer.AddVolume(iqn, adapter)

	if bs.nvmeServer != nil {
		nqn := blockvol.BuildNQN(bs.nqnPrefix, name)
		nvmeAdapter := nvme.NewNVMeAdapter(vol)
		bs.nvmeServer.AddVolume(nqn, nvmeAdapter, nvmeAdapter.DeviceNGUID())
	}

	glog.V(0).Infof("block service: created %s as %s (%d bytes)", path, iqn, sizeBytes)
	return path, iqn, iscsiAddr, nil
}

// DeleteBlockVol disconnects iSCSI sessions, closes the volume, and removes the .blk file.
// Idempotent: returns nil if volume not found.
func (bs *BlockService) DeleteBlockVol(name string) error {
	sanitized := blockvol.SanitizeFilename(name)
	path := filepath.Join(bs.blockDir, sanitized+".blk")
	iqn := bs.iqnPrefix + blockvol.SanitizeIQN(name)

	// Disconnect active iSCSI sessions and remove target entry.
	if bs.targetServer != nil {
		bs.targetServer.DisconnectVolume(iqn)
	}

	// Remove from NVMe target.
	if bs.nvmeServer != nil {
		nqn := blockvol.BuildNQN(bs.nqnPrefix, name)
		bs.nvmeServer.RemoveVolume(nqn)
	}

	// Close and unregister.
	if err := bs.blockStore.RemoveBlockVolume(path); err != nil {
		// Not found is OK (idempotent).
		if !strings.Contains(err.Error(), "not found") {
			return fmt.Errorf("remove block volume: %w", err)
		}
	}

	// Remove the .blk file and any snapshot files.
	os.Remove(path)
	matches, _ := filepath.Glob(path + ".snap.*")
	for _, m := range matches {
		os.Remove(m)
	}

	glog.V(0).Infof("block service: deleted %s", path)
	return nil
}

// ProcessAssignments applies assignments from master, including replication setup.
// V2 bridge: also delivers each assignment to the V2 engine for recovery ownership.
func (bs *BlockService) ProcessAssignments(assignments []blockvol.BlockVolumeAssignment) {
	_ = bs.ApplyAssignments(assignments)
}

// ApplyAssignments applies assignments through the single authoritative
// BlockService lifecycle: role apply, replication wiring, and publication
// readiness bookkeeping. Returns per-assignment errors parallel to the input.
func (bs *BlockService) ApplyAssignments(assignments []blockvol.BlockVolumeAssignment) []error {
	errs := make([]error, len(assignments))
	var legacyRecoveryResults []engine.AssignmentResult
	changedPaths := make(map[string]struct{}, len(assignments))

	// V2 bridge: convert and deliver to engine orchestrator (Phase 08 P1).
	// P3: skip V2 processing for repeated unchanged assignments.
	// P4: RecoveryManager starts/cancels recovery goroutines based on results.
	if bs.v2Bridge != nil && bs.v2Orchestrator != nil {
		for _, a := range assignments {
			changedPaths[a.Path] = struct{}{}
			// P3 idempotence: skip V2 processing if this assignment is
			// materially unchanged from the last one applied for this path.
			if bs.isAssignmentUnchanged(a) {
				continue
			}
			bs.recordAppliedAssignment(a)

			intent := bs.v2Bridge.ConvertAssignment(a, bs.localServerID)
			result := bs.v2Orchestrator.ProcessAssignment(intent)
			glog.V(1).Infof("v2bridge: assignment %s epoch=%d → added=%d removed=%d sessions=%d",
				a.Path, a.Epoch, len(result.Added), len(result.Removed),
				len(result.SessionsCreated)+len(result.SessionsSuperseded))

			// P4: drive live recovery execution based on engine result.
			if bs.v2Recovery != nil {
				if bs.v2Core == nil {
					if len(result.SessionsCreated) > 0 || len(result.SessionsSuperseded) > 0 || len(result.Removed) > 0 {
						legacyRecoveryResults = append(legacyRecoveryResults, result)
					}
				}
			}
		}
	}

	// V1 processing (requires blockStore).
	if bs.blockStore == nil {
		for path := range changedPaths {
			bs.syncProtocolExecutionState(path)
		}
		return errs
	}
	for i, a := range assignments {
		changedPaths[a.Path] = struct{}{}
		role := blockvol.RoleFromWire(a.Role)
		bs.recordAppliedAssignment(a)
		if err := bs.applyCoreAssignmentEvent(a); err != nil {
			errs[i] = err
			glog.Warningf("block service: assignment %s epoch=%d role=%s: %v", a.Path, a.Epoch, role, err)
			continue
		}

		// 2. Replication setup based on role + addresses.
		switch role {
		case blockvol.RolePrimary:
		case blockvol.RoleReplica:
		case blockvol.RoleRebuilding:
			if a.RebuildAddr != "" && bs.v2Core == nil {
				bs.startRebuild(a.Path, a.RebuildAddr, a.Epoch)
			}
		}
	}
	if bs.v2Recovery != nil {
		for _, result := range legacyRecoveryResults {
			bs.v2Recovery.HandleAssignmentResult(result, assignments)
		}
	}
	for path := range changedPaths {
		bs.syncProtocolExecutionState(path)
	}
	return errs
}

func (bs *BlockService) applyCoreAssignmentEvent(a blockvol.BlockVolumeAssignment) error {
	if bs == nil || bs.v2Core == nil {
		return bs.applyRoleAssignment(a)
	}
	if bs.isLeaseOnlyPrimaryRefresh(a) {
		return bs.applyRoleAssignment(a)
	}
	ev, ok := bs.coreAssignmentEvent(a)
	if !ok {
		return nil
	}
	result := bs.coreApplyAndLog(ev)
	if err := bs.applyCoreCommandsWithAssignment(result.Commands, &a); err != nil {
		return err
	}
	// T4: After assignment execution, check the resulting V2 core projection
	// and gate activation locally if the mode indicates the node should not
	// serve. This is the enforcement point — it happens immediately after
	// assignment, before the next heartbeat round-trip.
	bs.evaluateActivationGate(a.Path)
	return nil
}

func (bs *BlockService) isLeaseOnlyPrimaryRefresh(a blockvol.BlockVolumeAssignment) bool {
	if bs == nil || bs.v2Core == nil {
		return false
	}
	if blockvol.RoleFromWire(a.Role) != blockvol.RolePrimary {
		return false
	}
	if a.ReplicaDataAddr != "" || a.ReplicaCtrlAddr != "" || len(a.ReplicaAddrs) > 0 {
		return false
	}
	proj, ok := bs.CoreProjection(a.Path)
	if !ok {
		return false
	}
	return proj.Epoch == a.Epoch && proj.Role == engine.RolePrimary
}

// evaluateActivationGate checks the current V2 core projection for a volume
// and gates or clears activation accordingly. This is the local enforcement
// point for T4 — the promoted node decides locally whether reconstruction
// quality allows serving.
//
// Gate matrix (Phase 20 contract):
//   - Gate:  degraded, needs_rebuild, missing_engine_projection
//   - Allow: publish_healthy, replica_ready, bootstrap_pending, allocated_only
//
// bootstrap_pending and allocated_only are allowed because they represent
// fresh bootstrap or normal bring-up states with no stale-data risk. Gating
// them creates a deadlock: iSCSI removed → no writes → shipper never
// connects → mode never advances to publish_healthy.
//
// Enforcement: when gated, the volume is disconnected from the iSCSI target
// (active sessions terminated, volume removed). When ungated, the volume is
// re-registered with the iSCSI target.
func (bs *BlockService) evaluateActivationGate(path string) {
	if bs == nil {
		return
	}
	bs.activationGateMu.Lock()
	if bs.activationGated == nil {
		bs.activationGated = make(map[string]string)
	}
	bs.activationGateMu.Unlock()

	proj, ok := bs.CoreProjection(path)
	if !ok {
		// Fail-closed: if V2 core is active but projection is missing for
		// this path, gate locally. Mirrors T2's missing_engine_projection
		// posture. Only skip enforcement if V2 core is entirely absent.
		if bs.v2Core == nil {
			return
		}
		bs.activationGateMu.Lock()
		_, wasGated := bs.activationGated[path]
		bs.activationGated[path] = "missing_engine_projection"
		bs.activationGateMu.Unlock()
		if !wasGated {
			bs.gateServing(path, "missing_engine_projection")
		}
		return
	}
	bs.activationGateMu.Lock()
	_, wasGated := bs.activationGated[path]
	switch proj.Mode.Name {
	case "degraded", "needs_rebuild":
		// Unsafe reconstruction states — hard gate.
		reason := fmt.Sprintf("engine_projection_mode=%s", proj.Mode.Name)
		if proj.Mode.Reason != "" {
			reason += ": " + proj.Mode.Reason
		}
		bs.activationGated[path] = reason
		bs.activationGateMu.Unlock()
		if !wasGated {
			bs.gateServing(path, reason)
		}
	default:
		// All other modes (publish_healthy, replica_ready, bootstrap_pending,
		// allocated_only) are allowed. Fresh bootstrap must remain
		// discoverable so the shipper can complete first closure.
		delete(bs.activationGated, path)
		bs.activationGateMu.Unlock()
		if wasGated {
			bs.ungateServing(path)
		}
	}
}

// gateServing disconnects a volume from the iSCSI target, terminating active
// sessions and preventing new connections. This is the actual serving
// enforcement for T4 — not just bookkeeping.
func (bs *BlockService) gateServing(path, reason string) {
	name := volumeNameFromPath(path)
	glog.V(0).Infof("activation gated: %s — %s (disconnecting frontends)", path, reason)
	if bs.targetServer != nil {
		iqn := bs.iqnPrefix + blockvol.SanitizeIQN(name)
		bs.targetServer.DisconnectVolume(iqn)
	}
	if bs.nvmeServer != nil {
		nqn := blockvol.BuildNQN(bs.nqnPrefix, name)
		bs.nvmeServer.RemoveVolume(nqn)
	}
}

// ungateServing re-registers a volume with the iSCSI target after the gate
// clears (projection reaches a serving-allowed state).
func (bs *BlockService) ungateServing(path string) {
	glog.V(0).Infof("activation gate cleared: %s (re-registering frontends)", path)
	vol, ok := bs.blockStore.GetBlockVolume(path)
	if !ok || vol == nil {
		return
	}
	name := volumeNameFromPath(path)
	if bs.targetServer != nil {
		iqn := bs.iqnPrefix + blockvol.SanitizeIQN(name)
		adapter := blockvol.NewBlockVolAdapter(vol)
		bs.targetServer.AddVolume(iqn, adapter)
	}
	if bs.nvmeServer != nil {
		nqn := blockvol.BuildNQN(bs.nqnPrefix, name)
		nvmeAdapter := nvme.NewNVMeAdapter(vol)
		bs.nvmeServer.AddVolume(nqn, nvmeAdapter, nvmeAdapter.DeviceNGUID())
	}
}

// volumeNameFromPath extracts the volume name from a .blk file path.
func volumeNameFromPath(path string) string {
	name := filepath.Base(path)
	return strings.TrimSuffix(name, ".blk")
}

// IsActivationGated returns whether a volume is currently gated from serving
// and the reason. Used by iSCSI/NVMe adapter and heartbeat surface.
func (bs *BlockService) IsActivationGated(path string) (bool, string) {
	if bs == nil {
		return false, ""
	}
	bs.activationGateMu.RLock()
	defer bs.activationGateMu.RUnlock()
	reason, gated := bs.activationGated[path]
	return gated, reason
}

// ClearActivationGate removes the activation gate for a volume. Called when
// recovery completes and the projection reaches a serving-allowed state.
func (bs *BlockService) ClearActivationGate(path string) {
	if bs == nil {
		return
	}
	bs.activationGateMu.Lock()
	defer bs.activationGateMu.Unlock()
	if bs.activationGated == nil {
		bs.activationGated = make(map[string]string)
	}
	delete(bs.activationGated, path)
}

func (bs *BlockService) applyCoreEvent(ev engine.Event) {
	if bs == nil || bs.v2Core == nil {
		return
	}
	result := bs.coreApplyAndLog(ev)
	bs.applyCoreCommands(result.Commands)
	// T4: re-evaluate activation gate after every core event. This is the
	// runtime recovery path — when recovery/catch-up completes and the
	// projection transitions to a serving-allowed state, the gate clears
	// and the volume is re-registered with the iSCSI target.
	bs.evaluateActivationGate(ev.VolumeID())
	bs.syncProtocolExecutionState(ev.VolumeID())
}

// coreApplyAndLog applies an event to the V2 core and logs the transition.
// All core event paths (assignment-driven and observation-driven) must use this
// so the VS log contains a complete trace for post-run diagnosis.
func (bs *BlockService) coreApplyAndLog(ev engine.Event) engine.ApplyResult {
	result := bs.v2Core.ApplyEvent(ev)
	glog.V(0).Infof("core [%s]: event=%T mode=%s pub=%v reason=%q readiness={applied=%v shipper_cfg=%v shipper_conn=%v recv=%v} boundary={durable=%d committed=%d last_barrier_ok=%v last_barrier_reason=%q} cmds=%d",
		ev.VolumeID(), ev, result.Projection.Mode.Name,
		result.Projection.Publication.Healthy, result.Projection.Publication.Reason,
		result.Projection.Readiness.RoleApplied, result.Projection.Readiness.ShipperConfigured,
		result.Projection.Readiness.ShipperConnected, result.Projection.Readiness.ReceiverReady,
		result.Projection.Boundary.DurableLSN, result.Projection.Boundary.CommittedLSN,
		result.Projection.Boundary.LastBarrierOK, result.Projection.Boundary.LastBarrierReason,
		len(result.Commands))
	return result
}

func (bs *BlockService) applyCoreCommands(cmds []engine.Command) {
	_ = bs.applyCoreCommandsWithAssignment(cmds, nil)
}

func (bs *BlockService) applyCoreCommandsWithAssignment(cmds []engine.Command, assignment *blockvol.BlockVolumeAssignment) error {
	if bs == nil {
		return nil
	}
	return bs.coreCommandDispatcher().Run(cmds, assignment)
}

func (bs *BlockService) coreCommandDispatcher() *blockcmd.Dispatcher {
	var recovery blockcmd.RecoveryCoordinator
	if bs != nil && bs.v2Recovery != nil {
		recovery = bs.v2Recovery
	}
	var projection blockcmd.ProjectionReader
	if bs != nil && bs.v2Core != nil {
		projection = bs.v2Core
	}
	var recordCommand blockcmd.CommandRecorder
	var emitCoreEvent blockcmd.CoreEventEmitter
	var projectionCache blockcmd.ProjectionCacheWriter
	if bs != nil {
		recordCommand = bs.recordExecutedCoreCommand
		emitCoreEvent = bs.applyCoreEvent
		projectionCache = bs
	}
	return blockcmd.NewDispatcher(
		blockcmd.NewServiceOps(
			coreCommandBackend{bs: bs},
			recovery,
			projection,
			func(replicaID string) blockcmd.SessionInvalidator {
				if bs == nil || bs.v2Orchestrator == nil {
					return nil
				}
				return bs.v2Orchestrator.Registry.Sender(replicaID)
			},
		),
		blockcmd.NewHostEffects(recordCommand, emitCoreEvent, projection, projectionCache),
	)
}

func (bs *BlockService) commandBindings() *v2bridge.CommandBindings {
	if bs == nil {
		return nil
	}
	return v2bridge.NewCommandBindings(bs.blockStore, bs.listenAddr, bs.advertisedHost)
}

type coreCommandBackend struct {
	bs *BlockService
}

func (ops coreCommandBackend) ApplyRole(assignment blockvol.BlockVolumeAssignment) (bool, error) {
	if ops.bs == nil {
		return false, nil
	}
	if err := ops.bs.applyRoleAssignment(assignment); err != nil {
		return false, err
	}
	return true, nil
}

func (ops coreCommandBackend) StartReceiver(assignment blockvol.BlockVolumeAssignment) (bool, error) {
	if ops.bs == nil {
		return false, nil
	}
	if assignment.ReplicaDataAddr == "" || assignment.ReplicaCtrlAddr == "" {
		return false, nil
	}
	if err := ops.bs.setupReplicaReceiver(assignment.Path, assignment.ReplicaDataAddr, assignment.ReplicaCtrlAddr); err != nil {
		return false, err
	}
	return true, nil
}

func (ops coreCommandBackend) ConfigureShipper(volumeID string, replicas []engine.ReplicaAssignment) (bool, bool, error) {
	if ops.bs == nil {
		return false, false, nil
	}
	addrs := make([]blockvol.ReplicaAddr, 0, len(replicas))
	for _, replica := range replicas {
		if replica.Endpoint.DataAddr == "" || replica.Endpoint.CtrlAddr == "" {
			continue
		}
		addrs = append(addrs, blockvol.ReplicaAddr{
			ServerID: replica.ReplicaID,
			DataAddr: replica.Endpoint.DataAddr,
			CtrlAddr: replica.Endpoint.CtrlAddr,
		})
	}
	if len(addrs) == 0 {
		return false, false, nil
	}
	// Always use setupPrimaryReplicationMulti to preserve ServerID on all
	// paths, including RF=2 single-replica. setupPrimaryReplication (scalar)
	// drops ServerID because it only takes dataAddr/ctrlAddr.
	if err := ops.bs.setupPrimaryReplicationMulti(volumeID, addrs); err != nil {
		return false, false, err
	}
	return true, ops.bs.isPrimaryShipperConnected(volumeID), nil
}

func (bs *BlockService) applyRoleAssignment(a blockvol.BlockVolumeAssignment) error {
	if bs == nil || bs.blockStore == nil {
		return nil
	}
	if err := bs.commandBindings().ApplyRole(a); err != nil {
		return err
	}
	role := blockvol.RoleFromWire(a.Role)
	bs.noteRoleApplied(a.Path, role)
	bs.applyCoreEvent(engine.RoleApplied{ID: a.Path})
	return nil
}

func (bs *BlockService) recordExecutedCoreCommand(path, name string) {
	bs.coreExecMu.Lock()
	defer bs.coreExecMu.Unlock()
	if bs.coreExec == nil {
		bs.coreExec = make(map[string][]string)
	}
	bs.coreExec[path] = append(bs.coreExec[path], name)
}

func (bs *BlockService) coreAssignmentEvent(a blockvol.BlockVolumeAssignment) (engine.AssignmentDelivered, bool) {
	role := blockvol.RoleFromWire(a.Role)
	ev := engine.AssignmentDelivered{
		ID:    a.Path,
		Epoch: a.Epoch,
	}
	switch role {
	case blockvol.RolePrimary:
		ev.Role = engine.RolePrimary
		if len(a.ReplicaAddrs) > 0 {
			ev.Replicas = make([]engine.ReplicaAssignment, 0, len(a.ReplicaAddrs))
			for _, ra := range a.ReplicaAddrs {
				replicaServerID := legacyReplicaServerID(ra.ServerID, ra.DataAddr, ra.CtrlAddr)
				if replicaServerID == "" {
					continue
				}
				ev.Replicas = append(ev.Replicas, bridgeblockvol.ReplicaAssignmentForServer(a.Path, replicaServerID, engine.Endpoint{
					DataAddr: ra.DataAddr,
					CtrlAddr: ra.CtrlAddr,
				}))
			}
			if len(ev.Replicas) > 0 {
				ev.RecoveryTarget = engine.SessionCatchUp
			}
		} else if replicaServerID := legacyReplicaServerID(a.ReplicaServerID, a.ReplicaDataAddr, a.ReplicaCtrlAddr); replicaServerID != "" && a.ReplicaDataAddr != "" {
			ev.Replicas = []engine.ReplicaAssignment{
				bridgeblockvol.ReplicaAssignmentForServer(a.Path, replicaServerID, engine.Endpoint{
					DataAddr: a.ReplicaDataAddr,
					CtrlAddr: a.ReplicaCtrlAddr,
				}),
			}
			ev.RecoveryTarget = engine.SessionCatchUp
		}
		return ev, true
	case blockvol.RoleReplica:
		ev.Role = engine.RoleReplica
		ev.Replicas = []engine.ReplicaAssignment{
			bridgeblockvol.ReplicaAssignmentForServer(a.Path, bs.localServerID, engine.Endpoint{
				DataAddr: a.ReplicaDataAddr,
				CtrlAddr: a.ReplicaCtrlAddr,
			}),
		}
		return ev, true
	case blockvol.RoleRebuilding:
		ev.Role = engine.RoleReplica
		ev.RecoveryTarget = bridgeblockvol.RecoveryTargetForRole("rebuilding")
		ev.Replicas = []engine.ReplicaAssignment{
			bridgeblockvol.ReplicaAssignmentForServer(a.Path, bs.localServerID, engine.Endpoint{
				DataAddr: a.ReplicaDataAddr,
				CtrlAddr: a.ReplicaCtrlAddr,
			}),
		}
		return ev, true
	default:
		return engine.AssignmentDelivered{}, false
	}
}

// legacyReplicaServerID preserves the stable identity rule on the remaining
// server-local path: ReplicaID must come from an explicit ServerID and must
// never be synthesized from transport addresses.
func legacyReplicaServerID(serverID, dataAddr, ctrlAddr string) string {
	_ = dataAddr
	_ = ctrlAddr
	return serverID
}

func (bs *BlockService) isPrimaryShipperConnected(path string) bool {
	if bs == nil || bs.blockStore == nil {
		return false
	}
	return bs.commandBindings().IsPrimaryShipperConnected(path)
}

// setupPrimaryReplication configures WAL shipping from primary to replica
// and starts the rebuild server (R1-2).
func (bs *BlockService) setupPrimaryReplication(path, replicaDataAddr, replicaCtrlAddr string) error {
	// P3 idempotence: skip if replica state is unchanged.
	bs.replMu.RLock()
	existing := bs.replStates[path]
	bs.replMu.RUnlock()
	if existing != nil && existing.replicaDataAddr == replicaDataAddr && existing.replicaCtrlAddr == replicaCtrlAddr {
		// Unchanged repeated assignment — idempotent, no side effects.
		bs.markPrimaryTransportConfigured(path, []blockvol.ReplicaAddr{{
			DataAddr: replicaDataAddr,
			CtrlAddr: replicaCtrlAddr,
		}})
		return nil
	}

	rebuildAddr, err := bs.commandBindings().ConfigurePrimaryReplication(path, []blockvol.ReplicaAddr{{
		DataAddr: replicaDataAddr,
		CtrlAddr: replicaCtrlAddr,
	}})
	if err != nil {
		glog.Warningf("block service: setup primary replication %s: %v", path, err)
		return err
	}
	bs.markPrimaryTransportConfigured(path, []blockvol.ReplicaAddr{{
		DataAddr: replicaDataAddr,
		CtrlAddr: replicaCtrlAddr,
	}})
	glog.V(0).Infof("block service: primary %s shipping WAL to %s/%s (rebuild=%s)", path, replicaDataAddr, replicaCtrlAddr, rebuildAddr)
	return nil
}

// setupPrimaryReplicationMulti configures WAL shipping from primary to N replicas
// using SetReplicaAddrs (CP8-2: multi-replica support).
func (bs *BlockService) setupPrimaryReplicationMulti(path string, addrs []blockvol.ReplicaAddr) error {
	// P3 idempotence: skip if ALL replica addresses unchanged.
	// Compare full replica set, not just the first entry.
	if len(addrs) > 0 {
		bs.replMu.RLock()
		existing := bs.replStates[path]
		bs.replMu.RUnlock()
		if existing != nil && bs.multiReplicaUnchanged(path, addrs) {
			bs.markPrimaryTransportConfigured(path, addrs)
			return nil
		}
	}

	rebuildAddr, err := bs.commandBindings().ConfigurePrimaryReplication(path, addrs)
	if err != nil {
		glog.Warningf("block service: setup primary replication (multi) %s: %v", path, err)
		return err
	}
	bs.markPrimaryTransportConfigured(path, addrs)
	glog.V(0).Infof("block service: primary %s shipping WAL to %d replicas (rebuild=%s)", path, len(addrs), rebuildAddr)
	return nil
}

// setupReplicaReceiver starts the replica WAL receiver.
func (bs *BlockService) setupReplicaReceiver(path, dataAddr, ctrlAddr string) error {
	endpoints, err := bs.commandBindings().StartReceiver(path, dataAddr, ctrlAddr)
	if err != nil {
		glog.Warningf("block service: setup replica receiver %s: %v", path, err)
		return err
	}
	bs.markReceiverReady(path, endpoints.DataAddr, endpoints.CtrlAddr)
	glog.V(0).Infof("block service: replica %s receiving on %s/%s", path, endpoints.DataAddr, endpoints.CtrlAddr)
	return nil
}

// startRebuild starts a rebuild in the background.
// R2-F7: Rebuild success/failure is logged but not reported back to master.
// Future work: VS could report rebuild completion via heartbeat so master
// can update registry state (e.g., promote from Rebuilding to Replica).
func (bs *BlockService) startRebuild(path, rebuildAddr string, epoch uint64) {
	if bs != nil && bs.onLegacyStartRebuild != nil {
		bs.onLegacyStartRebuild(path, rebuildAddr, epoch)
	}
	go func() {
		vol, ok := bs.blockStore.GetBlockVolume(path)
		if !ok {
			glog.Warningf("block service: rebuild %s: volume not found", path)
			return
		}
		if err := blockvol.StartRebuild(vol, rebuildAddr, 0, epoch); err != nil {
			glog.Warningf("block service: rebuild %s from %s: %v", path, rebuildAddr, err)
			return
		}
		glog.V(0).Infof("block service: rebuild %s from %s completed", path, rebuildAddr)
	}()
}

// SnapshotBlockVol creates a snapshot on the named volume.
func (bs *BlockService) SnapshotBlockVol(name string, snapID uint32) (createdAt int64, sizeBytes uint64, err error) {
	path := bs.volumePath(name)
	err = bs.blockStore.WithVolume(path, func(vol *blockvol.BlockVol) error {
		if serr := vol.CreateSnapshot(snapID); serr != nil {
			return serr
		}
		// Find the snapshot we just created for its metadata.
		for _, s := range vol.ListSnapshots() {
			if s.ID == snapID {
				createdAt = s.CreatedAt.Unix()
				sizeBytes = vol.Info().VolumeSize
				return nil
			}
		}
		return fmt.Errorf("snapshot %d created but not found in list", snapID)
	})
	return
}

// DeleteBlockSnapshot deletes a snapshot on the named volume.
// Idempotent: returns nil if snapshot does not exist.
func (bs *BlockService) DeleteBlockSnapshot(name string, snapID uint32) error {
	path := bs.volumePath(name)
	return bs.blockStore.WithVolume(path, func(vol *blockvol.BlockVol) error {
		err := vol.DeleteSnapshot(snapID)
		if err != nil && err.Error() == "blockvol: snapshot not found" {
			return nil // idempotent
		}
		return err
	})
}

// RestoreBlockSnapshot restores the named volume to the specified snapshot.
// This is a destructive operation: all writes after the snapshot are lost.
func (bs *BlockService) RestoreBlockSnapshot(name string, snapID uint32) error {
	path := bs.volumePath(name)
	return bs.blockStore.WithVolume(path, func(vol *blockvol.BlockVol) error {
		return vol.RestoreSnapshot(snapID)
	})
}

// ListBlockSnapshots lists all snapshots on the named volume.
func (bs *BlockService) ListBlockSnapshots(name string) ([]blockvol.SnapshotInfo, uint64, error) {
	path := bs.volumePath(name)
	var infos []blockvol.SnapshotInfo
	var volSize uint64
	err := bs.blockStore.WithVolume(path, func(vol *blockvol.BlockVol) error {
		infos = vol.ListSnapshots()
		volSize = vol.Info().VolumeSize
		return nil
	})
	return infos, volSize, err
}

// ExpandBlockVol expands the named volume to newSize bytes.
func (bs *BlockService) ExpandBlockVol(name string, newSize uint64) (uint64, error) {
	path := bs.volumePath(name)
	var actualSize uint64
	err := bs.blockStore.WithVolume(path, func(vol *blockvol.BlockVol) error {
		if eerr := vol.Expand(newSize); eerr != nil {
			return eerr
		}
		actualSize = vol.Info().VolumeSize
		return nil
	})
	return actualSize, err
}

// PrepareExpandBlockVol prepares an expand on the named volume without committing.
func (bs *BlockService) PrepareExpandBlockVol(name string, newSize, expandEpoch uint64) error {
	path := bs.volumePath(name)
	return bs.blockStore.WithVolume(path, func(vol *blockvol.BlockVol) error {
		return vol.PrepareExpand(newSize, expandEpoch)
	})
}

// CommitExpandBlockVol commits a prepared expand on the named volume.
func (bs *BlockService) CommitExpandBlockVol(name string, expandEpoch uint64) (uint64, error) {
	path := bs.volumePath(name)
	var actualSize uint64
	err := bs.blockStore.WithVolume(path, func(vol *blockvol.BlockVol) error {
		if eerr := vol.CommitExpand(expandEpoch); eerr != nil {
			return eerr
		}
		actualSize = vol.Info().VolumeSize
		return nil
	})
	return actualSize, err
}

// CancelExpandBlockVol cancels a prepared expand on the named volume.
func (bs *BlockService) CancelExpandBlockVol(name string, expandEpoch uint64) error {
	path := bs.volumePath(name)
	return bs.blockStore.WithVolume(path, func(vol *blockvol.BlockVol) error {
		return vol.CancelExpand(expandEpoch)
	})
}

// volumePath converts a volume name to its .blk file path.
func (bs *BlockService) volumePath(name string) string {
	sanitized := blockvol.SanitizeFilename(name)
	return filepath.Join(bs.blockDir, sanitized+".blk")
}

// GetReplState returns the replication state for a volume path.
func (bs *BlockService) GetReplState(path string) (dataAddr, ctrlAddr string) {
	bs.replMu.RLock()
	defer bs.replMu.RUnlock()
	if s, ok := bs.replStates[path]; ok {
		return s.replicaDataAddr, s.replicaCtrlAddr
	}
	return "", ""
}

// CollectBlockVolumeHeartbeat returns heartbeat info for all block volumes,
// with replication addresses filled in from BlockService state (R1-4).
func (bs *BlockService) CollectBlockVolumeHeartbeat() []blockvol.BlockVolumeInfoMessage {
	msgs := bs.blockStore.CollectBlockVolumeHeartbeat()
	bs.replMu.RLock()
	defer bs.replMu.RUnlock()
	for i := range msgs {
		bs.observePrimaryShipperConnectivity(msgs[i].Path)
		if s, ok := bs.replStates[msgs[i].Path]; ok {
			msgs[i].ReplicaDataAddr, msgs[i].ReplicaCtrlAddr = bs.heartbeatReplicaAddrs(msgs[i].Path, s)
			msgs[i].ReplicaReady = bs.heartbeatReplicaReady(msgs[i].Path, s)
			msgs[i].NeedsRebuild = bs.heartbeatNeedsRebuild(msgs[i].Path, s)
			msgs[i].PublishHealthy = bs.heartbeatPublishHealthy(msgs[i].Path, s)
			msgs[i].VolumeMode = bs.heartbeatVolumeMode(msgs[i].Path, s)
			msgs[i].VolumeModeReason = bs.heartbeatVolumeModeReason(msgs[i].Path, s)
		}
		msgs[i].ReplicaDegraded = bs.heartbeatReplicaDegraded(msgs[i].Path, msgs[i].ReplicaDegraded)
		msgs[i].EngineProjectionMode = bs.heartbeatEngineProjectionMode(msgs[i].Path)
		if gated, reason := bs.IsActivationGated(msgs[i].Path); gated {
			msgs[i].ActivationGated = true
			msgs[i].ActivationGateReason = reason
		}
		// NVMe publication: report nvme_addr and nqn if NVMe target is running.
		if bs.nvmeListenAddr != "" {
			msgs[i].NvmeAddr = bs.nvmeListenAddr
			// Derive volume name from path for NQN construction.
			name := filepath.Base(msgs[i].Path)
			name = strings.TrimSuffix(name, ".blk")
			msgs[i].NQN = bs.NQN(name)
		}
	}
	return msgs
}

func (bs *BlockService) observePrimaryShipperConnectivity(path string) {
	if bs == nil || bs.v2Core == nil {
		return
	}
	proj, ok := bs.CoreProjection(path)
	if !ok || proj.Role != engine.RolePrimary || proj.Readiness.ShipperConnected {
		return
	}
	connected := bs.isPrimaryShipperConnected(path)
	glog.V(0).Infof("block service: recheck shipper connectivity %s connected=%v mode=%s reason=%q",
		path, connected, proj.Mode.Name, proj.Publication.Reason)
	bs.observePrimaryShipperConnectivityStatus(path, connected)
}

func (bs *BlockService) observePrimaryShipperConnectivityStatus(path string, connected bool) {
	if bs == nil || bs.v2Core == nil || !connected {
		return
	}
	proj, ok := bs.CoreProjection(path)
	if !ok || proj.Role != engine.RolePrimary || proj.Readiness.ShipperConnected {
		return
	}
	bs.applyCoreEvent(engine.ShipperConnectedObserved{ID: path})
}

// heartbeatReplicaAddrs returns the scalar replica transport addresses that
// should be exposed on the current heartbeat surface. On the Phase 15 live path
// it prefers the explicit core projection when present, while preserving the
// older adapter-local fallback for unrebound paths.
func (bs *BlockService) heartbeatReplicaAddrs(path string, state *volReplState) (string, string) {
	if state == nil {
		return "", ""
	}
	if proj, ok := bs.CoreProjection(path); ok {
		switch proj.Role {
		case engine.RolePrimary:
			if proj.Readiness.ShipperConfigured {
				return state.replicaDataAddr, state.replicaCtrlAddr
			}
		case engine.RoleReplica:
			if proj.Readiness.ReceiverReady {
				return state.replicaDataAddr, state.replicaCtrlAddr
			}
		}
		return "", ""
	}
	if state.publishHealthy {
		return state.replicaDataAddr, state.replicaCtrlAddr
	}
	return "", ""
}

// heartbeatReplicaReady returns the explicit replica-readiness truth that should
// be exposed on the current heartbeat surface. On the core-present path it
// prefers the core-owned readiness projection; older paths fall back to the
// adapter-local publish gate for compatibility.
func (bs *BlockService) heartbeatReplicaReady(path string, state *volReplState) bool {
	if state == nil {
		return false
	}
	if proj, ok := bs.CoreProjection(path); ok {
		return proj.Readiness.ReplicaReady
	}
	return state.publishHealthy
}

// heartbeatNeedsRebuild returns the explicit needs_rebuild truth that should be
// exposed on the current heartbeat surface. On the core-present path it
// preserves the stronger core mode instead of collapsing it into the degraded
// bit. Older paths keep returning false and rely on previous heuristics.
func (bs *BlockService) heartbeatNeedsRebuild(path string, state *volReplState) bool {
	if state == nil {
		return false
	}
	if proj, ok := bs.CoreProjection(path); ok {
		return proj.Mode.Name == engine.ModeNeedsRebuild
	}
	return false
}

// heartbeatPublishHealthy returns the explicit publish_healthy truth that should
// be exposed on the current heartbeat surface. On the core-present path it
// preserves the core-owned publication truth rather than reconstructing it from
// secondary readiness/degraded signals. Older paths fall back to the existing
// adapter-local publish flag for compatibility.
func (bs *BlockService) heartbeatPublishHealthy(path string, state *volReplState) bool {
	if state == nil {
		return false
	}
	if proj, ok := bs.CoreProjection(path); ok {
		return proj.Publication.Healthy
	}
	return state.publishHealthy
}

// heartbeatVolumeMode returns the explicit outward volume mode that should be
// exposed on the current heartbeat surface. On the core-present path it
// preserves the core-owned mode directly. Older paths return empty so the
// master can fall back to previous reconstruction logic.
func (bs *BlockService) heartbeatVolumeMode(path string, state *volReplState) string {
	if state == nil {
		return ""
	}
	if proj, ok := bs.CoreProjection(path); ok {
		return string(proj.Mode.Name)
	}
	return ""
}

// heartbeatVolumeModeReason returns the explicit outward volume mode reason that
// should be exposed on the current heartbeat surface. On the core-present path
// it prefers the mode reason and falls back to the publication reason so the
// master can preserve the bounded explanation behind outward mode transitions.
// Older paths return empty and keep backward-compatible default behavior.
func (bs *BlockService) heartbeatVolumeModeReason(path string, state *volReplState) string {
	if state == nil {
		return ""
	}
	if proj, ok := bs.CoreProjection(path); ok {
		if proj.Mode.Reason != "" {
			return proj.Mode.Reason
		}
		return proj.Publication.Reason
	}
	return ""
}

// heartbeatEngineProjectionMode returns the pure V2 engine-derived local
// projection mode. This reads ONLY from the V2 core projection — no ad-hoc
// fallback. Returns empty if V2 core is not present for this volume.
// Distinct from heartbeatVolumeMode which may fall back to non-V2 paths.
func (bs *BlockService) heartbeatEngineProjectionMode(path string) string {
	if proj, ok := bs.CoreProjection(path); ok {
		return string(proj.Mode.Name)
	}
	return ""
}

// heartbeatReplicaDegraded returns the bounded degraded bit for the current
// heartbeat surface. On the Phase 15 live path it prefers the core mode when
// present, then falls back to the runtime-local status bit.
func (bs *BlockService) heartbeatReplicaDegraded(path string, current bool) bool {
	if proj, ok := bs.CoreProjection(path); ok {
		switch proj.Mode.Name {
		case engine.ModeDegraded, engine.ModeNeedsRebuild:
			return true
		default:
			return false
		}
	}
	return current
}

// multiReplicaUnchanged checks if the full replica set is unchanged.
func (bs *BlockService) multiReplicaUnchanged(path string, addrs []blockvol.ReplicaAddr) bool {
	bs.replMu.RLock()
	defer bs.replMu.RUnlock()
	existing, ok := bs.replStates[path]
	if !ok || existing == nil {
		return false
	}
	if len(existing.allReplicas) != len(addrs) {
		return false
	}
	for i := range addrs {
		if existing.allReplicas[i].DataAddr != addrs[i].DataAddr ||
			existing.allReplicas[i].CtrlAddr != addrs[i].CtrlAddr ||
			existing.allReplicas[i].ServerID != addrs[i].ServerID {
			return false
		}
	}
	return true
}

func (bs *BlockService) ensureReplStateLocked(path string) *volReplState {
	if bs.replStates == nil {
		bs.replStates = make(map[string]*volReplState)
	}
	state := bs.replStates[path]
	if state == nil {
		state = &volReplState{}
		bs.replStates[path] = state
	}
	return state
}

func (bs *BlockService) noteRoleApplied(path string, role blockvol.Role) {
	bs.replMu.Lock()
	defer bs.replMu.Unlock()
	state := bs.ensureReplStateLocked(path)
	state.roleApplied = true
	switch role {
	case blockvol.RoleReplica:
		state.receiverReady = false
		state.shipperConfigured = false
		state.replicaEligible = false
		state.publishHealthy = false
	case blockvol.RolePrimary:
		state.receiverReady = false
		state.shipperConfigured = false
		state.replicaEligible = false
		state.publishHealthy = true
	case blockvol.RoleRebuilding:
		state.receiverReady = false
		state.shipperConfigured = false
		state.replicaEligible = false
		state.publishHealthy = false
	default:
		state.receiverReady = false
		state.shipperConfigured = false
		state.replicaEligible = false
		state.publishHealthy = false
		state.replicaDataAddr = ""
		state.replicaCtrlAddr = ""
		state.allReplicas = nil
	}
}

func (bs *BlockService) markPrimaryTransportConfigured(path string, addrs []blockvol.ReplicaAddr) {
	bs.replMu.Lock()
	defer bs.replMu.Unlock()
	state := bs.ensureReplStateLocked(path)
	state.shipperConfigured = len(addrs) > 0
	state.publishHealthy = true
	state.replicaEligible = false
	state.receiverReady = false
	if len(addrs) == 0 {
		state.replicaDataAddr = ""
		state.replicaCtrlAddr = ""
		state.allReplicas = nil
		return
	}
	copied := make([]blockvol.ReplicaAddr, len(addrs))
	copy(copied, addrs)
	state.allReplicas = copied
	state.replicaDataAddr = addrs[0].DataAddr
	state.replicaCtrlAddr = addrs[0].CtrlAddr
}

func (bs *BlockService) markReceiverReady(path, dataAddr, ctrlAddr string) {
	bs.replMu.Lock()
	defer bs.replMu.Unlock()
	state := bs.ensureReplStateLocked(path)
	state.receiverReady = true
	state.replicaEligible = true
	state.publishHealthy = true
	state.shipperConfigured = false
	state.replicaDataAddr = dataAddr
	state.replicaCtrlAddr = ctrlAddr
	state.allReplicas = nil
}

// ReadinessSnapshot reports the current server-boundary readiness/publication
// closure for one volume. On the core-present path it prefers the explicit core
// projection for aligned readiness plus publication health, while retaining
// adapter-local fallback only when the core is absent.
func (bs *BlockService) ReadinessSnapshot(path string) BlockReadinessSnapshot {
	snap := BlockReadinessSnapshot{}
	bs.replMu.RLock()
	state := bs.replStates[path]
	if state != nil {
		snap.RoleApplied = state.roleApplied
		snap.ReceiverReady = state.receiverReady
		snap.ShipperConfigured = state.shipperConfigured
		snap.ReplicaEligible = state.replicaEligible
		snap.PublishHealthy = state.publishHealthy
	}
	bs.replMu.RUnlock()
	if !snap.ShipperConfigured || bs.blockStore == nil {
		if proj, ok := bs.CoreProjection(path); ok {
			snap.RoleApplied = proj.Readiness.RoleApplied
			snap.ReceiverReady = proj.Readiness.ReceiverReady
			snap.ShipperConfigured = proj.Readiness.ShipperConfigured
			snap.ShipperConnected = proj.Readiness.ShipperConnected
			snap.ReplicaEligible = proj.Readiness.ReplicaReady
			snap.PublishHealthy = proj.Publication.Healthy
		}
		return snap
	}
	_ = bs.blockStore.WithVolume(path, func(vol *blockvol.BlockVol) error {
		snap.ShipperConnected = len(vol.ReplicaShipperStates()) > 0 && !vol.Status().ReplicaDegraded
		return nil
	})
	if proj, ok := bs.CoreProjection(path); ok {
		snap.RoleApplied = proj.Readiness.RoleApplied
		snap.ReceiverReady = proj.Readiness.ReceiverReady
		snap.ShipperConfigured = proj.Readiness.ShipperConfigured
		snap.ShipperConnected = proj.Readiness.ShipperConnected
		snap.ReplicaEligible = proj.Readiness.ReplicaReady
		snap.PublishHealthy = proj.Publication.Healthy
	}
	return snap
}

// --- P3: Assignment idempotence ---

// lastAppliedAssignment stores the full assignment for idempotence comparison.
// Keyed by volume path.
type lastAppliedAssignment struct {
	Path            string
	Epoch           uint64
	Role            uint32
	ReplicaServerID string
	ReplicaDataAddr string
	ReplicaCtrlAddr string
	ReplicaAddrs    []blockvol.ReplicaAddr
}

func lastAppliedFrom(a blockvol.BlockVolumeAssignment) lastAppliedAssignment {
	// Copy the slice to avoid aliasing.
	var addrs []blockvol.ReplicaAddr
	if len(a.ReplicaAddrs) > 0 {
		addrs = make([]blockvol.ReplicaAddr, len(a.ReplicaAddrs))
		copy(addrs, a.ReplicaAddrs)
	}
	return lastAppliedAssignment{
		Path:            a.Path,
		Epoch:           a.Epoch,
		Role:            a.Role,
		ReplicaServerID: a.ReplicaServerID,
		ReplicaDataAddr: a.ReplicaDataAddr,
		ReplicaCtrlAddr: a.ReplicaCtrlAddr,
		ReplicaAddrs:    addrs,
	}
}

func (la lastAppliedAssignment) equals(a blockvol.BlockVolumeAssignment) bool {
	if la.Path != a.Path || la.Epoch != a.Epoch || la.Role != a.Role {
		return false
	}
	if la.ReplicaServerID != a.ReplicaServerID || la.ReplicaDataAddr != a.ReplicaDataAddr || la.ReplicaCtrlAddr != a.ReplicaCtrlAddr {
		return false
	}
	if len(la.ReplicaAddrs) != len(a.ReplicaAddrs) {
		return false
	}
	for i := range la.ReplicaAddrs {
		if la.ReplicaAddrs[i].DataAddr != a.ReplicaAddrs[i].DataAddr ||
			la.ReplicaAddrs[i].CtrlAddr != a.ReplicaAddrs[i].CtrlAddr ||
			la.ReplicaAddrs[i].ServerID != a.ReplicaAddrs[i].ServerID {
			return false
		}
	}
	return true
}

func (bs *BlockService) isAssignmentUnchanged(a blockvol.BlockVolumeAssignment) bool {
	bs.lastAssignMu.RLock()
	defer bs.lastAssignMu.RUnlock()
	if bs.lastAssign == nil {
		return false
	}
	last, ok := bs.lastAssign[a.Path]
	if !ok {
		return false
	}
	return last.equals(a)
}

func (bs *BlockService) recordAppliedAssignment(a blockvol.BlockVolumeAssignment) {
	bs.lastAssignMu.Lock()
	defer bs.lastAssignMu.Unlock()
	if bs.lastAssign == nil {
		bs.lastAssign = make(map[string]lastAppliedAssignment)
	}
	bs.lastAssign[a.Path] = lastAppliedFrom(a)
}

// ReplicationPorts computes deterministic replication ports for a volume.
// Ports are derived from a hash of the volume path offset from the iSCSI base port.
func (bs *BlockService) ReplicationPorts(volPath string) (dataPort, ctrlPort, rebuildPort int) {
	basePort := 3260
	if idx := strings.LastIndex(bs.listenAddr, ":"); idx >= 0 {
		var p int
		if _, err := fmt.Sscanf(bs.listenAddr[idx+1:], "%d", &p); err == nil && p > 0 {
			basePort = p
		}
	}
	h := fnv.New32a()
	h.Write([]byte(volPath))
	offset := int(h.Sum32()%500) * 3
	dataPort = basePort + 1000 + offset
	ctrlPort = dataPort + 1
	rebuildPort = dataPort + 2
	return
}

// Shutdown gracefully stops the iSCSI and NVMe targets and closes all block volumes.
func (bs *BlockService) Shutdown() {
	if bs == nil {
		return
	}
	glog.V(0).Infof("block service: shutting down...")
	// P4: drain active recovery goroutines before closing volumes.
	if bs.v2Recovery != nil {
		bs.v2Recovery.Shutdown()
	}
	if bs.nvmeServer != nil {
		bs.nvmeServer.Close()
	}
	if bs.targetServer != nil {
		bs.targetServer.Close()
	}
	bs.blockStore.Close()
	glog.V(0).Infof("block service: shut down")
}

// SetBlockService wires a BlockService into the VolumeServer so that
// heartbeats include block volume info and the server is marked block-capable.
// Also wires shipper state change callbacks for immediate heartbeat on degradation.
func (vs *VolumeServer) SetBlockService(bs *BlockService) {
	vs.blockService = bs
	bs.WireStateChangeNotify(vs.blockStateChangeChan)
}
