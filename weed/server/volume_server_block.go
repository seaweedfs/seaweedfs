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
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/iscsi"
)

// volReplState tracks active replication addresses per volume.
type volReplState struct {
	replicaDataAddr string
	replicaCtrlAddr string
}

// BlockService manages block volumes and the iSCSI target server.
type BlockService struct {
	blockStore   *storage.BlockVolumeStore
	targetServer *iscsi.TargetServer
	iqnPrefix    string
	blockDir     string
	listenAddr   string

	// Replication state (CP6-3).
	replMu     sync.RWMutex
	replStates map[string]*volReplState // keyed by volume path
}

// StartBlockService scans blockDir for .blk files, opens them as block volumes,
// registers them with an iSCSI target server, and starts listening.
// Returns nil if blockDir is empty (feature disabled).
func StartBlockService(listenAddr, blockDir, iqnPrefix string) *BlockService {
	if blockDir == "" {
		return nil
	}

	if iqnPrefix == "" {
		iqnPrefix = "iqn.2024-01.com.seaweedfs:vol."
	}

	bs := &BlockService{
		blockStore: storage.NewBlockVolumeStore(),
		iqnPrefix:  iqnPrefix,
		blockDir:   blockDir,
		listenAddr: listenAddr,
	}

	logger := log.New(os.Stderr, "iscsi: ", log.LstdFlags)

	config := iscsi.DefaultTargetConfig()
	config.TargetName = iqnPrefix + "default"

	bs.targetServer = iscsi.NewTargetServer(listenAddr, config, logger)

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

		// Derive IQN from filename: vol1.blk -> iqn.2024-01.com.seaweedfs:vol.vol1
		name := strings.TrimSuffix(entry.Name(), ".blk")
		iqn := iqnPrefix + blockvol.SanitizeIQN(name)
		adapter := blockvol.NewBlockVolAdapter(vol)
		bs.targetServer.AddVolume(iqn, adapter)
		glog.V(0).Infof("block service: registered %s as %s", path, iqn)
	}

	// Start iSCSI target in background.
	go func() {
		if err := bs.targetServer.ListenAndServe(); err != nil {
			glog.Warningf("block service: iSCSI target stopped: %v", err)
		}
	}()

	glog.V(0).Infof("block service: iSCSI target started on %s", listenAddr)
	return bs
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

// CreateBlockVol creates a new .blk file, registers it with BlockVolumeStore
// and iSCSI TargetServer. Returns path, IQN, iSCSI addr.
// Idempotent: if volume already exists with same or larger size, returns existing info.
func (bs *BlockService) CreateBlockVol(name string, sizeBytes uint64, diskType string) (path, iqn, iscsiAddr string, err error) {
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
		// Re-add to TargetServer in case it was cleared (crash recovery).
		// AddVolume is idempotent — no-op if already registered.
		adapter := blockvol.NewBlockVolAdapter(vol)
		bs.targetServer.AddVolume(iqn, adapter)
		return path, iqn, iscsiAddr, nil
	}

	// Create the .blk file.
	if err := os.MkdirAll(bs.blockDir, 0755); err != nil {
		return "", "", "", fmt.Errorf("create block dir: %w", err)
	}
	created, err := blockvol.CreateBlockVol(path, blockvol.CreateOptions{
		VolumeSize: sizeBytes,
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
func (bs *BlockService) ProcessAssignments(assignments []blockvol.BlockVolumeAssignment) {
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
			if a.ReplicaDataAddr != "" && a.ReplicaCtrlAddr != "" {
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

// setupReplicaReceiver starts the replica WAL receiver.
func (bs *BlockService) setupReplicaReceiver(path, dataAddr, ctrlAddr string) {
	if err := bs.blockStore.WithVolume(path, func(vol *blockvol.BlockVol) error {
		return vol.StartReplicaReceiver(dataAddr, ctrlAddr)
	}); err != nil {
		glog.Warningf("block service: setup replica receiver %s: %v", path, err)
		return
	}
	bs.replMu.Lock()
	if bs.replStates == nil {
		bs.replStates = make(map[string]*volReplState)
	}
	bs.replStates[path] = &volReplState{
		replicaDataAddr: dataAddr,
		replicaCtrlAddr: ctrlAddr,
	}
	bs.replMu.Unlock()
	glog.V(0).Infof("block service: replica %s receiving on %s/%s", path, dataAddr, ctrlAddr)
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
	}
	return msgs
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

// Shutdown gracefully stops the iSCSI target and closes all block volumes.
func (bs *BlockService) Shutdown() {
	if bs == nil {
		return
	}
	glog.V(0).Infof("block service: shutting down...")
	if bs.targetServer != nil {
		bs.targetServer.Close()
	}
	bs.blockStore.Close()
	glog.V(0).Infof("block service: shut down")
}
