package csi

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/iscsi"
)

var (
	ErrNotReady       = errors.New("csi: volume manager not ready")
	ErrVolumeExists   = errors.New("csi: volume already exists")
	ErrVolumeNotFound = errors.New("csi: volume not found")
)

// managedVolume tracks a single BlockVol instance and its iSCSI target.
type managedVolume struct {
	vol       *blockvol.BlockVol
	path      string // file path to .blk file
	iqn       string // target IQN for this volume
	nqn       string // NVMe subsystem NQN for this volume
	sizeBytes uint64
}

// managerState tracks the lifecycle of the VolumeManager.
type managerState int

const (
	stateStopped  managerState = iota // initial or after Stop()
	stateStarting                     // Start() in progress
	stateReady                        // running normally
	stateFailed                       // Start() failed, retryable
)

// VolumeManager manages multiple BlockVol instances behind a shared TargetServer.
type VolumeManager struct {
	mu        sync.RWMutex
	dataDir   string
	volumes   map[string]*managedVolume
	target    *iscsi.TargetServer
	iqnPrefix string
	nqnPrefix string
	config    iscsi.TargetConfig
	logger    *log.Logger
	state     managerState
	iscsiAddr string
	nvmeAddr  string
}

// VolumeManagerOpts holds optional configuration for VolumeManager.
type VolumeManagerOpts struct {
	NvmeAddr  string // NVMe/TCP target address (ip:port), empty if NVMe disabled
	NQNPrefix string // NQN prefix for NVMe subsystems
}

// NewVolumeManager creates a new VolumeManager.
func NewVolumeManager(dataDir, iscsiAddr, iqnPrefix string, logger *log.Logger, opts ...VolumeManagerOpts) *VolumeManager {
	if logger == nil {
		logger = log.Default()
	}
	config := iscsi.DefaultTargetConfig()
	vm := &VolumeManager{
		dataDir:   dataDir,
		volumes:   make(map[string]*managedVolume),
		iqnPrefix: iqnPrefix,
		config:    config,
		logger:    logger,
		iscsiAddr: iscsiAddr,
	}
	if len(opts) > 0 {
		vm.nvmeAddr = opts[0].NvmeAddr
		vm.nqnPrefix = opts[0].NQNPrefix
	}
	return vm
}

// Start initializes and starts the shared TargetServer.
// Safe to call after Stop() or after a failed Start(). Returns immediately if already running.
// The listener is created synchronously so port-in-use errors surface immediately.
func (m *VolumeManager) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.state == stateReady {
		return nil // already running
	}
	if m.state == stateStarting {
		return fmt.Errorf("csi: start already in progress")
	}
	m.state = stateStarting

	if err := os.MkdirAll(m.dataDir, 0755); err != nil {
		m.state = stateFailed
		return fmt.Errorf("csi: create data dir: %w", err)
	}

	m.target = iscsi.NewTargetServer(m.iscsiAddr, m.config, m.logger)

	// Create listener synchronously so bind errors are reported immediately.
	ln, err := net.Listen("tcp", m.iscsiAddr)
	if err != nil {
		m.target = nil
		m.state = stateFailed
		return fmt.Errorf("csi: listen %s: %w", m.iscsiAddr, err)
	}

	ts := m.target // capture for goroutine (m.target may be reset by Stop)
	go func() {
		if err := ts.Serve(ln); err != nil {
			m.logger.Printf("target server error: %v", err)
		}
	}()

	m.state = stateReady
	m.logger.Printf("volume manager started: dataDir=%s iscsiAddr=%s", m.dataDir, ln.Addr())
	return nil
}

// Stop closes all volumes and the target server. After Stop, Start may be called again.
func (m *VolumeManager) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for name, mv := range m.volumes {
		if m.target != nil {
			m.target.DisconnectVolume(mv.iqn)
		}
		mv.vol.Close()
		delete(m.volumes, name)
	}

	var err error
	if m.target != nil {
		err = m.target.Close()
		m.target = nil
	}

	m.state = stateStopped
	return err
}

// ErrVolumeSizeMismatch indicates a volume exists on disk with a different size.
var ErrVolumeSizeMismatch = errors.New("csi: volume exists with different size")

// CreateVolume creates a new BlockVol file and registers it with the target.
// Idempotent: if the .blk file already exists on disk (e.g. after driver restart),
// it is opened and tracked. Returns ErrVolumeSizeMismatch if the existing volume
// has a smaller size than requested.
func (m *VolumeManager) CreateVolume(name string, sizeBytes uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.state != stateReady {
		return ErrNotReady
	}

	// Already tracked in-memory.
	if mv, ok := m.volumes[name]; ok {
		if mv.sizeBytes >= sizeBytes {
			return nil // idempotent
		}
		return ErrVolumeSizeMismatch
	}

	volPath := m.volumePath(name)

	// Check for existing .blk file on disk (survives driver restart).
	if _, statErr := os.Stat(volPath); statErr == nil {
		vol, err := blockvol.OpenBlockVol(volPath)
		if err != nil {
			return fmt.Errorf("csi: open existing blockvol: %w", err)
		}
		info := vol.Info()
		if info.VolumeSize < sizeBytes {
			vol.Close()
			return ErrVolumeSizeMismatch
		}
		iqn := m.volumeIQN(name)
		adapter := &blockvol.BlockVolAdapter{Vol: vol, TPGID: 1}
		m.target.AddVolume(iqn, adapter)
		m.volumes[name] = &managedVolume{
			vol:       vol,
			path:      volPath,
			iqn:       iqn,
			nqn:       m.volumeNQN(name),
			sizeBytes: info.VolumeSize,
		}
		m.logger.Printf("adopted existing volume %q: %s (%d bytes)", name, iqn, info.VolumeSize)
		return nil
	}

	vol, err := blockvol.CreateBlockVol(volPath, blockvol.CreateOptions{
		VolumeSize: sizeBytes,
		BlockSize:  4096,
		WALSize:    64 * 1024 * 1024,
	})
	if err != nil {
		return fmt.Errorf("csi: create blockvol: %w", err)
	}

	iqn := m.volumeIQN(name)
	adapter := &blockvol.BlockVolAdapter{Vol: vol, TPGID: 1}
	m.target.AddVolume(iqn, adapter)

	m.volumes[name] = &managedVolume{
		vol:       vol,
		path:      volPath,
		iqn:       iqn,
		nqn:       m.volumeNQN(name),
		sizeBytes: sizeBytes,
	}

	m.logger.Printf("created volume %q: %s (%d bytes)", name, iqn, sizeBytes)
	return nil
}

// DeleteVolume closes and deletes a volume file and associated snapshot files.
func (m *VolumeManager) DeleteVolume(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	mv, ok := m.volumes[name]
	if !ok {
		// Idempotent: try to remove file anyway in case volume was not tracked.
		volPath := m.volumePath(name)
		os.Remove(volPath)
		removeSnapshotFiles(volPath)
		return nil
	}

	if m.target != nil {
		m.target.DisconnectVolume(mv.iqn)
	}
	mv.vol.Close()
	os.Remove(mv.path)
	removeSnapshotFiles(mv.path)
	delete(m.volumes, name)

	m.logger.Printf("deleted volume %q", name)
	return nil
}

// removeSnapshotFiles removes any .snap.* delta files associated with a volume path.
func removeSnapshotFiles(volPath string) {
	matches, _ := filepath.Glob(volPath + ".snap.*")
	for _, m := range matches {
		os.Remove(m)
	}
}

// OpenVolume opens an existing BlockVol file and adds it to the target.
func (m *VolumeManager) OpenVolume(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.state != stateReady {
		return ErrNotReady
	}

	if _, ok := m.volumes[name]; ok {
		return nil // already open, idempotent
	}

	volPath := m.volumePath(name)
	vol, err := blockvol.OpenBlockVol(volPath)
	if err != nil {
		return fmt.Errorf("csi: open blockvol: %w", err)
	}

	info := vol.Info()
	iqn := m.volumeIQN(name)
	adapter := &blockvol.BlockVolAdapter{Vol: vol, TPGID: 1}
	m.target.AddVolume(iqn, adapter)

	m.volumes[name] = &managedVolume{
		vol:       vol,
		path:      volPath,
		iqn:       iqn,
		nqn:       m.volumeNQN(name),
		sizeBytes: info.VolumeSize,
	}

	m.logger.Printf("opened volume %q: %s", name, iqn)
	return nil
}

// CloseVolume disconnects sessions, removes from target, and closes the BlockVol.
func (m *VolumeManager) CloseVolume(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	mv, ok := m.volumes[name]
	if !ok {
		return nil // already closed, idempotent
	}

	if m.target != nil {
		m.target.DisconnectVolume(mv.iqn)
	}
	mv.vol.Close()
	delete(m.volumes, name)

	m.logger.Printf("closed volume %q", name)
	return nil
}

// VolumeIQN returns the iSCSI IQN for a volume name.
func (m *VolumeManager) VolumeIQN(name string) string {
	return m.volumeIQN(name)
}

// VolumeExists returns true if the volume is currently tracked.
func (m *VolumeManager) VolumeExists(name string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.volumes[name]
	return ok
}

// VolumeSizeBytes returns the size of a tracked volume or 0 if not found.
func (m *VolumeManager) VolumeSizeBytes(name string) uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if mv, ok := m.volumes[name]; ok {
		return mv.sizeBytes
	}
	return 0
}

// ListenAddr returns the target server's listen address.
func (m *VolumeManager) ListenAddr() string {
	if m.target != nil {
		return m.target.ListenAddr()
	}
	return ""
}

// NvmeAddr returns the NVMe/TCP target address, or empty if NVMe is disabled.
func (m *VolumeManager) NvmeAddr() string {
	return m.nvmeAddr
}

// VolumeNQN returns the NVMe NQN for a volume name. Returns empty if nqnPrefix is not set.
func (m *VolumeManager) VolumeNQN(name string) string {
	if m.nqnPrefix == "" {
		return ""
	}
	return m.volumeNQN(name)
}

func (m *VolumeManager) volumeNQN(name string) string {
	return blockvol.BuildNQN(m.nqnPrefix, name)
}

// WithVolume runs fn while holding the manager lock with a reference to the volume.
func (m *VolumeManager) WithVolume(name string, fn func(*blockvol.BlockVol) error) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	mv, ok := m.volumes[name]
	if !ok {
		return ErrVolumeNotFound
	}
	return fn(mv.vol)
}

// CreateSnapshot creates a snapshot on the named volume.
func (m *VolumeManager) CreateSnapshot(name string, snapID uint32) (*SnapshotInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	mv, ok := m.volumes[name]
	if !ok {
		return nil, ErrVolumeNotFound
	}
	if err := mv.vol.CreateSnapshot(snapID); err != nil {
		return nil, err
	}
	// Find the snapshot we just created.
	for _, s := range mv.vol.ListSnapshots() {
		if s.ID == snapID {
			return &SnapshotInfo{
				SnapshotID: snapID,
				VolumeID:   name,
				CreatedAt:  s.CreatedAt.Unix(),
				SizeBytes:  mv.sizeBytes,
			}, nil
		}
	}
	return nil, fmt.Errorf("snapshot %d created but not found", snapID)
}

// DeleteSnapshot deletes a snapshot. Idempotent: returns nil if not found.
func (m *VolumeManager) DeleteSnapshot(name string, snapID uint32) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	mv, ok := m.volumes[name]
	if !ok {
		return ErrVolumeNotFound
	}
	err := mv.vol.DeleteSnapshot(snapID)
	if err != nil && err.Error() == "blockvol: snapshot not found" {
		return nil
	}
	return err
}

// ListSnapshots returns all snapshots for the named volume.
func (m *VolumeManager) ListSnapshots(name string) ([]*SnapshotInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	mv, ok := m.volumes[name]
	if !ok {
		return nil, ErrVolumeNotFound
	}
	engineInfos := mv.vol.ListSnapshots()
	result := make([]*SnapshotInfo, 0, len(engineInfos))
	for _, s := range engineInfos {
		result = append(result, &SnapshotInfo{
			SnapshotID: s.ID,
			VolumeID:   name,
			CreatedAt:  s.CreatedAt.Unix(),
			SizeBytes:  mv.sizeBytes,
		})
	}
	return result, nil
}

// ExpandVolume expands the named volume. Returns the new size.
func (m *VolumeManager) ExpandVolume(name string, newSizeBytes uint64) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	mv, ok := m.volumes[name]
	if !ok {
		return 0, ErrVolumeNotFound
	}
	if err := mv.vol.Expand(newSizeBytes); err != nil {
		return 0, err
	}
	mv.sizeBytes = mv.vol.Info().VolumeSize
	return mv.sizeBytes, nil
}

func (m *VolumeManager) volumePath(name string) string {
	return filepath.Join(m.dataDir, sanitizeFilename(name)+".blk")
}

func (m *VolumeManager) volumeIQN(name string) string {
	return m.iqnPrefix + ":" + SanitizeIQN(name)
}

// sanitizeFilename delegates to the shared blockvol.SanitizeFilename.
func sanitizeFilename(name string) string {
	return blockvol.SanitizeFilename(name)
}

// SanitizeIQN delegates to the shared blockvol.SanitizeIQN.
func SanitizeIQN(name string) string {
	return blockvol.SanitizeIQN(name)
}
