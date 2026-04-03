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
	mu        sync.RWMutex
	volumes   map[string]*blockvol.BlockVol // keyed by volume file path
	diskTypes map[string]string             // path -> disk type (e.g. "ssd")
}

// NewBlockVolumeStore creates a new block volume manager.
func NewBlockVolumeStore() *BlockVolumeStore {
	return &BlockVolumeStore{
		volumes:   make(map[string]*blockvol.BlockVol),
		diskTypes: make(map[string]string),
	}
}

// AddBlockVolume opens and registers a block volume.
// diskType is metadata for heartbeat reporting (e.g. "ssd", "hdd").
func (bs *BlockVolumeStore) AddBlockVolume(path, diskType string, cfgs ...blockvol.BlockVolConfig) (*blockvol.BlockVol, error) {
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
	bs.diskTypes[path] = diskType
	glog.V(0).Infof("block volume registered: %s (disk=%s)", path, diskType)
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
	delete(bs.diskTypes, path)
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

// IterateBlockVolumes calls fn for each registered block volume.
func (bs *BlockVolumeStore) IterateBlockVolumes(fn func(path string, vol *blockvol.BlockVol)) {
	bs.mu.RLock()
	defer bs.mu.RUnlock()
	for path, vol := range bs.volumes {
		fn(path, vol)
	}
}

// CollectBlockVolumeHeartbeat returns status for all registered
// block volumes, suitable for inclusion in a heartbeat message.
func (bs *BlockVolumeStore) CollectBlockVolumeHeartbeat() []blockvol.BlockVolumeInfoMessage {
	bs.mu.RLock()
	defer bs.mu.RUnlock()
	msgs := make([]blockvol.BlockVolumeInfoMessage, 0, len(bs.volumes))
	for path, vol := range bs.volumes {
		msgs = append(msgs, blockvol.ToBlockVolumeInfoMessage(path, bs.diskTypes[path], vol))
	}
	return msgs
}

// WithVolume looks up a volume by path and calls fn while holding RLock.
// This prevents RemoveBlockVolume from closing the volume while fn runs
// (BUG-CP4B3-1: TOCTOU between GetBlockVolume and HandleAssignment).
func (bs *BlockVolumeStore) WithVolume(path string, fn func(*blockvol.BlockVol) error) error {
	bs.mu.RLock()
	defer bs.mu.RUnlock()
	vol, ok := bs.volumes[path]
	if !ok {
		return fmt.Errorf("block volume not found: %s", path)
	}
	return fn(vol)
}

// ProcessBlockVolumeAssignments applies only the local role/epoch/lease part of
// a batch of assignments. It does NOT wire replica receivers, shippers, or
// publication readiness. The authoritative runtime lifecycle lives in
// BlockService.ApplyAssignments.
//
// Returns a slice of errors parallel to the input (nil = success). Unknown
// volumes and invalid transitions are logged and returned as errors, but do not
// stop processing of remaining assignments.
func (bs *BlockVolumeStore) ProcessBlockVolumeAssignments(
	assignments []blockvol.BlockVolumeAssignment,
) []error {
	errs := make([]error, len(assignments))
	for i, a := range assignments {
		role := blockvol.RoleFromWire(a.Role)
		ttl := blockvol.LeaseTTLFromWire(a.LeaseTtlMs)
		if err := bs.WithVolume(a.Path, func(vol *blockvol.BlockVol) error {
			return vol.HandleAssignment(a.Epoch, role, ttl)
		}); err != nil {
			errs[i] = err
			glog.Warningf("assignment: volume %s epoch=%d role=%s: %v",
				a.Path, a.Epoch, role, err)
		}
	}
	return errs
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
