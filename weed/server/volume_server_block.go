package weed_server

import (
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
		vol, err := bs.blockStore.AddBlockVolume(path)
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
				vol, err = bs.blockStore.AddBlockVolume(path)
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
		iqn := iqnPrefix + name
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
