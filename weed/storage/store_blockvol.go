package storage

import (
	"fmt"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// BlockVolumeStore manages block volumes (iSCSI-backed).
// It is a standalone component held by VolumeServer, not embedded into Store,
// to keep the existing Store codebase unchanged.
type BlockVolumeStore struct {
	mu      sync.RWMutex
	volumes map[string]*blockvol.BlockVol // keyed by volume file path
}

// NewBlockVolumeStore creates a new block volume manager.
func NewBlockVolumeStore() *BlockVolumeStore {
	return &BlockVolumeStore{
		volumes: make(map[string]*blockvol.BlockVol),
	}
}

// AddBlockVolume opens and registers a block volume.
func (bs *BlockVolumeStore) AddBlockVolume(path string, cfgs ...blockvol.BlockVolConfig) (*blockvol.BlockVol, error) {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	if _, ok := bs.volumes[path]; ok {
		return nil, fmt.Errorf("block volume already registered: %s", path)
	}

	vol, err := blockvol.OpenBlockVol(path, cfgs...)
	if err != nil {
		return nil, fmt.Errorf("open block volume %s: %w", path, err)
	}

	bs.volumes[path] = vol
	glog.V(0).Infof("block volume registered: %s", path)
	return vol, nil
}

// RemoveBlockVolume closes and unregisters a block volume.
func (bs *BlockVolumeStore) RemoveBlockVolume(path string) error {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	vol, ok := bs.volumes[path]
	if !ok {
		return fmt.Errorf("block volume not found: %s", path)
	}

	if err := vol.Close(); err != nil {
		glog.Warningf("error closing block volume %s: %v", path, err)
	}
	delete(bs.volumes, path)
	glog.V(0).Infof("block volume removed: %s", path)
	return nil
}

// GetBlockVolume returns a registered block volume by path.
func (bs *BlockVolumeStore) GetBlockVolume(path string) (*blockvol.BlockVol, bool) {
	bs.mu.RLock()
	defer bs.mu.RUnlock()
	vol, ok := bs.volumes[path]
	return vol, ok
}

// ListBlockVolumes returns the paths of all registered block volumes.
func (bs *BlockVolumeStore) ListBlockVolumes() []string {
	bs.mu.RLock()
	defer bs.mu.RUnlock()
	paths := make([]string, 0, len(bs.volumes))
	for p := range bs.volumes {
		paths = append(paths, p)
	}
	return paths
}

// Close closes all block volumes.
func (bs *BlockVolumeStore) Close() {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	for path, vol := range bs.volumes {
		if err := vol.Close(); err != nil {
			glog.Warningf("error closing block volume %s: %v", path, err)
		}
		delete(bs.volumes, path)
	}
	glog.V(0).Infof("all block volumes closed")
}
