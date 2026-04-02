package weed_server

import (
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	engine "github.com/seaweedfs/seaweedfs/sw-block/engine/replication"
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
	allReplicas []blockvol.ReplicaAddr
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
	v2Recovery     *RecoveryManager

	// P3: last-applied assignment per volume path for idempotence.
	lastAssignMu sync.RWMutex
	lastAssign   map[string]lastAppliedAssignment
	// localServerID: stable identity for this volume server.
	// INTERIM: uses listenAddr (transport-shaped). Should be replaced
	// with a registry-assigned stable server ID in a later hardening pass.
	localServerID string
}

// V2Orchestrator returns the V2 engine orchestrator for inspection/testing.
func (bs *BlockService) V2Orchestrator() *engine.RecoveryOrchestrator {
	return bs.v2Orchestrator
}

// SetServerID sets the stable server identity for V2 control semantics.
// Should be called with the gRPC address that the master knows this VS by,
// replacing the interim listenAddr-based identity.
func (bs *BlockService) SetServerID(id string) {
	bs.localServerID = id
}

// WireStateChangeNotify sets up shipper state change callbacks on all
// registered volumes so that degradation/recovery triggers an immediate
// heartbeat via the provided channel. Non-blocking send (buffered chan 1).
func (bs *BlockService) WireStateChangeNotify(ch chan bool) {
	bs.blockStore.IterateBlockVolumes(func(path string, vol *blockvol.BlockVol) {
		vol.SetOnShipperStateChange(func(from, to blockvol.ReplicaState) {
			select {
			case ch <- true:
			default: // already pending
			}
		})
	})
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
		blockStore:     storage.NewBlockVolumeStore(),
		iqnPrefix:      iqnPrefix,
		nqnPrefix:      nqnPrefix,
		blockDir:       blockDir,
		listenAddr:     listenAddr,
		nvmeListenAddr: nvmeCfg.ListenAddr,
		v2Bridge:       v2bridge.NewControlBridge(),
		v2Orchestrator: engine.NewRecoveryOrchestrator(),
		localServerID:  listenAddr, // INTERIM: transport-shaped, see field doc
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
	sanitized := blockvol.SanitizeFilename(name)
	path = filepath.Join(bs.blockDir, sanitized+".blk")
	iqn = bs.iqnPrefix + blockvol.SanitizeIQN(name)
	iscsiAddr = bs.listenAddr

	// Check if already registered.
	if vol, ok := bs.blockStore.GetBlockVolume(path); ok {
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
	// V2 bridge: convert and deliver to engine orchestrator (Phase 08 P1).
	// P3: skip V2 processing for repeated unchanged assignments.
	// P4: RecoveryManager starts/cancels recovery goroutines based on results.
	if bs.v2Bridge != nil && bs.v2Orchestrator != nil {
		for _, a := range assignments {
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
			if bs.v2Recovery != nil && (len(result.SessionsCreated) > 0 || len(result.SessionsSuperseded) > 0 || len(result.Removed) > 0) {
				bs.v2Recovery.HandleAssignmentResult(result, assignments)
			}
		}
	}

	// V1 processing (requires blockStore).
	if bs.blockStore == nil {
		return
	}
	for _, a := range assignments {
		role := blockvol.RoleFromWire(a.Role)
		ttl := blockvol.LeaseTTLFromWire(a.LeaseTtlMs)

		// 1. Apply role/epoch/lease.
		if err := bs.blockStore.WithVolume(a.Path, func(vol *blockvol.BlockVol) error {
			return vol.HandleAssignment(a.Epoch, role, ttl)
		}); err != nil {
			glog.Warningf("block service: assignment %s epoch=%d role=%s: %v", a.Path, a.Epoch, role, err)
			continue
		}

		// 2. Replication setup based on role + addresses.
		switch role {
		case blockvol.RolePrimary:
			// CP8-2: ReplicaAddrs (multi-replica) takes precedence over scalar fields.
			if len(a.ReplicaAddrs) > 0 {
				bs.setupPrimaryReplicationMulti(a.Path, a.ReplicaAddrs)
			} else if a.ReplicaDataAddr != "" && a.ReplicaCtrlAddr != "" {
				bs.setupPrimaryReplication(a.Path, a.ReplicaDataAddr, a.ReplicaCtrlAddr)
			}
		case blockvol.RoleReplica:
			if a.ReplicaDataAddr != "" && a.ReplicaCtrlAddr != "" {
				bs.setupReplicaReceiver(a.Path, a.ReplicaDataAddr, a.ReplicaCtrlAddr)
			}
		case blockvol.RoleRebuilding:
			if a.RebuildAddr != "" {
				bs.startRebuild(a.Path, a.RebuildAddr, a.Epoch)
			}
		}
	}
}

// setupPrimaryReplication configures WAL shipping from primary to replica
// and starts the rebuild server (R1-2).
func (bs *BlockService) setupPrimaryReplication(path, replicaDataAddr, replicaCtrlAddr string) {
	// P3 idempotence: skip if replica state is unchanged.
	bs.replMu.RLock()
	existing := bs.replStates[path]
	bs.replMu.RUnlock()
	if existing != nil && existing.replicaDataAddr == replicaDataAddr && existing.replicaCtrlAddr == replicaCtrlAddr {
		// Unchanged repeated assignment — idempotent, no side effects.
		return
	}

	// Compute deterministic rebuild listen address.
	_, _, rebuildPort := bs.ReplicationPorts(path)
	host := bs.listenAddr
	if idx := strings.LastIndex(host, ":"); idx >= 0 {
		host = host[:idx]
	}
	rebuildAddr := fmt.Sprintf("%s:%d", host, rebuildPort)

	if err := bs.blockStore.WithVolume(path, func(vol *blockvol.BlockVol) error {
		vol.SetReplicaAddr(replicaDataAddr, replicaCtrlAddr)
		// R1-2: Start rebuild server so replicas can catch up after failover.
		if err := vol.StartRebuildServer(rebuildAddr); err != nil {
			glog.Warningf("block service: start rebuild server %s on %s: %v", path, rebuildAddr, err)
			// Non-fatal: WAL shipping can work without rebuild server.
		}
		return nil
	}); err != nil {
		glog.Warningf("block service: setup primary replication %s: %v", path, err)
		return
	}
	// Track replication state for heartbeat reporting (R1-4).
	// These addresses are what the primary ships to — they come from the
	// master's assignment. They should already be canonical (from
	// AllocateBlockVolumeResponse), but if not, they'll be reported as-is.
	bs.replMu.Lock()
	if bs.replStates == nil {
		bs.replStates = make(map[string]*volReplState)
	}
	bs.replStates[path] = &volReplState{
		replicaDataAddr: replicaDataAddr,
		replicaCtrlAddr: replicaCtrlAddr,
	}
	bs.replMu.Unlock()
	glog.V(0).Infof("block service: primary %s shipping WAL to %s/%s (rebuild=%s)", path, replicaDataAddr, replicaCtrlAddr, rebuildAddr)
}

// setupPrimaryReplicationMulti configures WAL shipping from primary to N replicas
// using SetReplicaAddrs (CP8-2: multi-replica support).
func (bs *BlockService) setupPrimaryReplicationMulti(path string, addrs []blockvol.ReplicaAddr) {
	// P3 idempotence: skip if ALL replica addresses unchanged.
	// Compare full replica set, not just the first entry.
	if len(addrs) > 0 {
		bs.replMu.RLock()
		existing := bs.replStates[path]
		bs.replMu.RUnlock()
		if existing != nil && bs.multiReplicaUnchanged(path, addrs) {
			return
		}
	}

	// Compute deterministic rebuild listen address.
	_, _, rebuildPort := bs.ReplicationPorts(path)
	host := bs.listenAddr
	if idx := strings.LastIndex(host, ":"); idx >= 0 {
		host = host[:idx]
	}
	rebuildAddr := fmt.Sprintf("%s:%d", host, rebuildPort)

	if err := bs.blockStore.WithVolume(path, func(vol *blockvol.BlockVol) error {
		vol.SetReplicaAddrs(addrs)
		if err := vol.StartRebuildServer(rebuildAddr); err != nil {
			glog.Warningf("block service: start rebuild server %s on %s: %v", path, rebuildAddr, err)
		}
		return nil
	}); err != nil {
		glog.Warningf("block service: setup primary replication (multi) %s: %v", path, err)
		return
	}
	// Track replication state for heartbeat reporting.
	bs.replMu.Lock()
	if bs.replStates == nil {
		bs.replStates = make(map[string]*volReplState)
	}
	// Store full replica set + first replica for backward compat heartbeat.
	if len(addrs) > 0 {
		// Copy the addrs slice to avoid aliasing.
		copied := make([]blockvol.ReplicaAddr, len(addrs))
		copy(copied, addrs)
		bs.replStates[path] = &volReplState{
			replicaDataAddr: addrs[0].DataAddr,
			replicaCtrlAddr: addrs[0].CtrlAddr,
			allReplicas:     copied,
		}
	}
	bs.replMu.Unlock()
	glog.V(0).Infof("block service: primary %s shipping WAL to %d replicas (rebuild=%s)", path, len(addrs), rebuildAddr)
}

// setupReplicaReceiver starts the replica WAL receiver.
func (bs *BlockService) setupReplicaReceiver(path, dataAddr, ctrlAddr string) {
	// Store canonical addresses from the receiver (not raw assignment addresses).
	// The receiver canonicalizes wildcard ":port" to "ip:port" via CP13-2.
	var canonDataAddr, canonCtrlAddr string
	if err := bs.blockStore.WithVolume(path, func(vol *blockvol.BlockVol) error {
		if err := vol.StartReplicaReceiver(dataAddr, ctrlAddr); err != nil {
			return err
		}
		// Read back canonical addresses from the receiver.
		if vol.ReplicaReceiverAddr() != nil {
			canonDataAddr = vol.ReplicaReceiverAddr().DataAddr
			canonCtrlAddr = vol.ReplicaReceiverAddr().CtrlAddr
		}
		return nil
	}); err != nil {
		glog.Warningf("block service: setup replica receiver %s: %v", path, err)
		return
	}
	// Fallback to assignment addresses if receiver didn't report.
	if canonDataAddr == "" {
		canonDataAddr = dataAddr
	}
	if canonCtrlAddr == "" {
		canonCtrlAddr = ctrlAddr
	}
	bs.replMu.Lock()
	if bs.replStates == nil {
		bs.replStates = make(map[string]*volReplState)
	}
	bs.replStates[path] = &volReplState{
		replicaDataAddr: canonDataAddr,
		replicaCtrlAddr: canonCtrlAddr,
	}
	bs.replMu.Unlock()
	glog.V(0).Infof("block service: replica %s receiving on %s/%s", path, canonDataAddr, canonCtrlAddr)
}

// startRebuild starts a rebuild in the background.
// R2-F7: Rebuild success/failure is logged but not reported back to master.
// Future work: VS could report rebuild completion via heartbeat so master
// can update registry state (e.g., promote from Rebuilding to Replica).
func (bs *BlockService) startRebuild(path, rebuildAddr string, epoch uint64) {
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
		if s, ok := bs.replStates[msgs[i].Path]; ok {
			msgs[i].ReplicaDataAddr = s.replicaDataAddr
			msgs[i].ReplicaCtrlAddr = s.replicaCtrlAddr
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
