package purev2

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	engine "github.com/seaweedfs/seaweedfs/sw-block/engine/replication"
	"github.com/seaweedfs/seaweedfs/weed/server/blockcmd"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/v2bridge"
)

const defaultListenAddr = "127.0.0.1:3260"

// Config defines the small pure V2 runtime shell configuration.
type Config struct {
	ListenAddr     string
	AdvertisedHost string
	DiskType       string
}

// VolumeDebugSnapshot is the bounded outward runtime view for one volume.
type VolumeDebugSnapshot struct {
	Path             string
	Info             blockvol.VolumeInfo
	Status           blockvol.BlockVolumeStatus
	Projection       engine.PublicationProjection
	HasProjection    bool
	CoreState        engine.VolumeState
	HasCoreState     bool
	ExecutedCommands []string
}

// Runtime is a small pure-V2 process shell for RF1 local execution.
// It intentionally excludes master heartbeat, failover, and product-surface loops.
type Runtime struct {
	store      *storage.BlockVolumeStore
	bindings   *v2bridge.CommandBindings
	core       *engine.CoreEngine
	dispatcher *blockcmd.Dispatcher
	config     Config

	mu          sync.RWMutex
	projections map[string]engine.PublicationProjection
	executed    map[string][]string
}

// New creates a new RF1-oriented pure V2 runtime shell.
func New(cfg Config) *Runtime {
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = defaultListenAddr
	}
	rt := &Runtime{
		store:       storage.NewBlockVolumeStore(),
		core:        engine.NewCoreEngine(),
		config:      cfg,
		projections: make(map[string]engine.PublicationProjection),
		executed:    make(map[string][]string),
	}
	rt.bindings = v2bridge.NewCommandBindings(rt.store, cfg.ListenAddr, cfg.AdvertisedHost)
	rt.dispatcher = blockcmd.NewDispatcher(
		blockcmd.NewServiceOps(runtimeBackend{runtime: rt}, nil, rt, nil),
		blockcmd.NewHostEffects(rt.recordCommand, rt.emitCoreEvent, rt, rt),
	)
	return rt
}

// CreateVolume creates a new local block volume and registers it in the runtime store.
func (rt *Runtime) CreateVolume(path string, opts blockvol.CreateOptions, cfgs ...blockvol.BlockVolConfig) error {
	if rt == nil {
		return fmt.Errorf("purev2: runtime is nil")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("purev2: create parent dir for %s: %w", path, err)
	}
	vol, err := blockvol.CreateBlockVol(path, opts, cfgs...)
	if err != nil {
		return fmt.Errorf("purev2: create volume %s: %w", path, err)
	}
	if err := vol.Close(); err != nil {
		return fmt.Errorf("purev2: close created volume %s: %w", path, err)
	}
	_, err = rt.store.AddBlockVolume(path, rt.config.DiskType, cfgs...)
	if err != nil {
		return fmt.Errorf("purev2: register created volume %s: %w", path, err)
	}
	return nil
}

// OpenVolume opens an existing volume into the runtime store.
func (rt *Runtime) OpenVolume(path string, cfgs ...blockvol.BlockVolConfig) error {
	if rt == nil {
		return fmt.Errorf("purev2: runtime is nil")
	}
	if _, err := rt.store.AddBlockVolume(path, rt.config.DiskType, cfgs...); err != nil {
		return fmt.Errorf("purev2: open volume %s: %w", path, err)
	}
	return nil
}

// ApplyPrimaryAssignment injects a static RF1 primary assignment into the pure runtime.
func (rt *Runtime) ApplyPrimaryAssignment(path string, epoch uint64, leaseTTL time.Duration) error {
	return rt.applyAssignment(blockvol.BlockVolumeAssignment{
		Path:       path,
		Epoch:      epoch,
		Role:       blockvol.RoleToWire(blockvol.RolePrimary),
		LeaseTtlMs: blockvol.LeaseTTLToWire(leaseTTL),
	})
}

// BootstrapPrimary creates a volume if needed and applies a primary RF1 assignment.
func (rt *Runtime) BootstrapPrimary(path string, opts blockvol.CreateOptions, epoch uint64, leaseTTL time.Duration, cfgs ...blockvol.BlockVolConfig) error {
	if _, ok := rt.store.GetBlockVolume(path); !ok {
		if _, err := os.Stat(path); err == nil {
			if err := rt.OpenVolume(path, cfgs...); err != nil {
				return err
			}
		} else {
			if err := rt.CreateVolume(path, opts, cfgs...); err != nil {
				return err
			}
		}
	}
	return rt.ApplyPrimaryAssignment(path, epoch, leaseTTL)
}

// WriteLBA writes data at the given logical block address.
func (rt *Runtime) WriteLBA(path string, lba uint64, data []byte) error {
	var status blockvol.V2StatusSnapshot
	err := rt.store.WithVolume(path, func(vol *blockvol.BlockVol) error {
		if err := vol.WriteLBA(lba, data); err != nil {
			return err
		}
		status = vol.StatusSnapshot()
		return nil
	})
	if err != nil {
		return err
	}
	rt.observeLocalBoundaries(path, status)
	return nil
}

// ReadLBA reads data at the given logical block address.
func (rt *Runtime) ReadLBA(path string, lba uint64, length uint32) ([]byte, error) {
	var out []byte
	err := rt.store.WithVolume(path, func(vol *blockvol.BlockVol) error {
		data, err := vol.ReadLBA(lba, length)
		if err != nil {
			return err
		}
		out = data
		return nil
	})
	return out, err
}

// SyncCache forces durable local flush through the reused blockvol backend.
func (rt *Runtime) SyncCache(path string) error {
	var (
		status  blockvol.V2StatusSnapshot
		attempt bool
	)
	err := rt.store.WithVolume(path, func(vol *blockvol.BlockVol) error {
		attempt = true
		if err := vol.SyncCache(); err != nil {
			return err
		}
		status = vol.StatusSnapshot()
		return nil
	})
	if err != nil {
		if attempt {
			rt.emitCoreEvent(engine.BarrierRejected{ID: path, Reason: err.Error()})
		}
		return err
	}
	rt.observeLocalBoundaries(path, status)
	rt.emitCoreEvent(engine.BarrierAccepted{ID: path, FlushedLSN: status.CommittedLSN})
	rt.emitCoreEvent(engine.CheckpointAdvanced{ID: path, CheckpointLSN: status.CheckpointLSN})
	return nil
}

// Snapshot returns the bounded runtime and core view for one volume.
func (rt *Runtime) Snapshot(path string) (VolumeDebugSnapshot, error) {
	if rt == nil {
		return VolumeDebugSnapshot{}, fmt.Errorf("purev2: runtime is nil")
	}
	var snap VolumeDebugSnapshot
	err := rt.store.WithVolume(path, func(vol *blockvol.BlockVol) error {
		snap.Path = path
		snap.Info = vol.Info()
		snap.Status = vol.Status()
		return nil
	})
	if err != nil {
		return VolumeDebugSnapshot{}, err
	}

	if proj, ok := rt.Projection(path); ok {
		snap.HasProjection = true
		snap.Projection = proj
	}
	if st, ok := rt.core.State(path); ok {
		snap.HasCoreState = true
		snap.CoreState = st
	}

	rt.mu.RLock()
	snap.ExecutedCommands = append([]string(nil), rt.executed[path]...)
	rt.mu.RUnlock()
	return snap, nil
}

// WithVolume exposes controlled access to one underlying block volume.
// It exists so higher-level runtimes can attach frontend adapters without
// importing weed/server lifecycle code.
func (rt *Runtime) WithVolume(path string, fn func(*blockvol.BlockVol) error) error {
	if rt == nil {
		return fmt.Errorf("purev2: runtime is nil")
	}
	return rt.store.WithVolume(path, fn)
}

// Projection implements blockcmd.ProjectionReader.
func (rt *Runtime) Projection(volumeID string) (engine.PublicationProjection, bool) {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	proj, ok := rt.projections[volumeID]
	return proj, ok
}

// StoreProjection implements blockcmd.ProjectionCacheWriter.
func (rt *Runtime) StoreProjection(volumeID string, projection engine.PublicationProjection) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.projections[volumeID] = projection
}

// Close closes all registered volumes.
func (rt *Runtime) Close() {
	if rt == nil {
		return
	}
	rt.store.Close()
}

func (rt *Runtime) applyAssignment(a blockvol.BlockVolumeAssignment) error {
	ev, ok := assignmentEvent(a)
	if !ok {
		return nil
	}
	result := rt.core.ApplyEvent(ev)
	return rt.dispatcher.Run(result.Commands, &a)
}

func (rt *Runtime) emitCoreEvent(ev engine.Event) {
	result := rt.core.ApplyEvent(ev)
	_ = rt.dispatcher.Run(result.Commands, nil)
}

func (rt *Runtime) recordCommand(volumeID, name string) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.executed[volumeID] = append(rt.executed[volumeID], name)
}

func (rt *Runtime) observeLocalBoundaries(path string, status blockvol.V2StatusSnapshot) {
	rt.emitCoreEvent(engine.CommittedLSNAdvanced{
		ID:           path,
		CommittedLSN: status.CommittedLSN,
	})
}

func assignmentEvent(a blockvol.BlockVolumeAssignment) (engine.AssignmentDelivered, bool) {
	ev := engine.AssignmentDelivered{
		ID:    a.Path,
		Epoch: a.Epoch,
	}
	switch blockvol.RoleFromWire(a.Role) {
	case blockvol.RolePrimary:
		ev.Role = engine.RolePrimary
		return ev, true
	case blockvol.RoleReplica:
		ev.Role = engine.RoleReplica
		return ev, true
	case blockvol.RoleRebuilding:
		ev.Role = engine.RoleReplica
		return ev, true
	default:
		return engine.AssignmentDelivered{}, false
	}
}

type runtimeBackend struct {
	runtime *Runtime
}

func (ops runtimeBackend) ApplyRole(assignment blockvol.BlockVolumeAssignment) (bool, error) {
	if ops.runtime == nil || ops.runtime.bindings == nil {
		return false, nil
	}
	if err := ops.runtime.bindings.ApplyRole(assignment); err != nil {
		return false, err
	}
	ops.runtime.emitCoreEvent(engine.RoleApplied{ID: assignment.Path})
	return true, nil
}

func (ops runtimeBackend) StartReceiver(assignment blockvol.BlockVolumeAssignment) (bool, error) {
	if ops.runtime == nil || ops.runtime.bindings == nil {
		return false, nil
	}
	if assignment.ReplicaDataAddr == "" || assignment.ReplicaCtrlAddr == "" {
		return false, nil
	}
	if _, err := ops.runtime.bindings.StartReceiver(assignment.Path, assignment.ReplicaDataAddr, assignment.ReplicaCtrlAddr); err != nil {
		return false, err
	}
	return true, nil
}

func (ops runtimeBackend) ConfigureShipper(volumeID string, replicas []engine.ReplicaAssignment) (bool, bool, error) {
	if ops.runtime == nil || ops.runtime.bindings == nil || len(replicas) == 0 {
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
	if _, err := ops.runtime.bindings.ConfigurePrimaryReplication(volumeID, addrs); err != nil {
		return false, false, err
	}
	return true, ops.runtime.bindings.IsPrimaryShipperConnected(volumeID), nil
}
