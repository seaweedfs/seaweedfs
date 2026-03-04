package weed_server

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/iscsi"
)

// BlockService manages block volumes and the iSCSI target server.
type BlockService struct {
	blockStore   *storage.BlockVolumeStore
	targetServer *iscsi.TargetServer
	iqnPrefix    string
	blockDir     string
	listenAddr   string
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
