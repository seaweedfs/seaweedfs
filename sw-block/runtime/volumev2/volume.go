package volumev2

import (
	"fmt"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/sw-block/runtime/masterv2"
	"github.com/seaweedfs/seaweedfs/sw-block/runtime/protocolv2"
	"github.com/seaweedfs/seaweedfs/sw-block/runtime/purev2"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// Config defines the minimal volumev2 runtime identity.
type Config struct {
	NodeID    string
	DataPlane DataPlane
	Runtime   *purev2.Runtime
}

type volumeBinding struct {
	name string
	path string
}

// Node is the minimal volumev2 runtime POC.
// It owns volume identity, local execution, and heartbeat reporting.
type Node struct {
	id        string
	dataPlane DataPlane

	mu      sync.RWMutex
	volumes map[string]volumeBinding
}

// New creates a minimal volumev2 node runtime.
func New(cfg Config) (*Node, error) {
	if cfg.NodeID == "" {
		return nil, fmt.Errorf("volumev2: node id is required")
	}
	dp := cfg.DataPlane
	if dp == nil {
		rt := cfg.Runtime
		if rt == nil {
			rt = purev2.New(purev2.Config{})
		}
		var err error
		dp, err = NewPureRuntimeDataPlane(rt)
		if err != nil {
			return nil, err
		}
	}
	return &Node{
		id:        cfg.NodeID,
		dataPlane: dp,
		volumes:   make(map[string]volumeBinding),
	}, nil
}

// NodeID returns the stable runtime identity.
func (n *Node) NodeID() string {
	if n == nil {
		return ""
	}
	return n.id
}

// ApplyAssignments applies the control messages emitted by masterv2.
func (n *Node) ApplyAssignments(assignments []masterv2.Assignment) error {
	if n == nil {
		return fmt.Errorf("volumev2: node is nil")
	}
	for _, assignment := range assignments {
		if assignment.NodeID != "" && assignment.NodeID != n.id {
			continue
		}
		if assignment.Role != "primary" {
			return fmt.Errorf("volumev2: unsupported role %q", assignment.Role)
		}
		if err := n.dataPlane.BootstrapPrimary(
			assignment.Path,
			assignment.CreateOptions,
			assignment.Epoch,
			assignment.LeaseTTL,
		); err != nil {
			return fmt.Errorf("volumev2: apply assignment %s: %w", assignment.Name, err)
		}
		n.mu.Lock()
		n.volumes[assignment.Name] = volumeBinding{name: assignment.Name, path: assignment.Path}
		n.mu.Unlock()
	}
	return nil
}

// Heartbeat reports the minimal local runtime state back to masterv2.
func (n *Node) Heartbeat() (masterv2.NodeHeartbeat, error) {
	if n == nil {
		return masterv2.NodeHeartbeat{}, fmt.Errorf("volumev2: node is nil")
	}
	n.mu.RLock()
	bindings := make([]volumeBinding, 0, len(n.volumes))
	for _, binding := range n.volumes {
		bindings = append(bindings, binding)
	}
	n.mu.RUnlock()

	report := masterv2.NodeHeartbeat{
		NodeID:     n.id,
		ReportedAt: time.Now(),
		Volumes:    make([]masterv2.VolumeHeartbeat, 0, len(bindings)),
	}
	for _, binding := range bindings {
		snap, err := n.dataPlane.Snapshot(binding.path)
		if err != nil {
			return masterv2.NodeHeartbeat{}, fmt.Errorf("volumev2: snapshot %s: %w", binding.name, err)
		}
		var committedLSN uint64
		if err := n.dataPlane.WithVolume(binding.path, func(vol *blockvol.BlockVol) error {
			committedLSN = vol.StatusSnapshot().CommittedLSN
			return nil
		}); err != nil {
			return masterv2.NodeHeartbeat{}, fmt.Errorf("volumev2: committed snapshot %s: %w", binding.name, err)
		}
		vol := masterv2.VolumeHeartbeat{
			Name:         binding.name,
			Path:         binding.path,
			Epoch:        snap.Status.Epoch,
			Role:         snap.Status.Role.String(),
			CommittedLSN: committedLSN,
		}
		if snap.HasProjection {
			vol.Mode = string(snap.Projection.Mode.Name)
			vol.ModeReason = snap.Projection.Mode.Reason
			vol.RoleApplied = snap.Projection.Readiness.RoleApplied
			vol.ReplicaReady = snap.Projection.Readiness.ReplicaReady
		}
		report.Volumes = append(report.Volumes, vol)
	}
	return report, nil
}

// QueryPromotionEvidence returns fresh failover evidence outside the periodic
// heartbeat path. This keeps promotion arbitration separate from liveness.
func (n *Node) QueryPromotionEvidence(req masterv2.PromotionQueryRequest) (masterv2.PromotionQueryResponse, error) {
	path, err := n.pathFor(req.VolumeName)
	if err != nil {
		return masterv2.PromotionQueryResponse{}, err
	}
	snap, err := n.dataPlane.Snapshot(path)
	if err != nil {
		return masterv2.PromotionQueryResponse{}, fmt.Errorf("volumev2: snapshot %s: %w", req.VolumeName, err)
	}
	resp := masterv2.PromotionQueryResponse{
		VolumeName: req.VolumeName,
		NodeID:     n.id,
		Epoch:      snap.Status.Epoch,
		Role:       snap.Status.Role.String(),
	}
	if err := n.dataPlane.WithVolume(path, func(vol *blockvol.BlockVol) error {
		status := vol.StatusSnapshot()
		resp.CommittedLSN = status.CommittedLSN
		resp.WALHeadLSN = status.WALHeadLSN
		return nil
	}); err != nil {
		return masterv2.PromotionQueryResponse{}, fmt.Errorf("volumev2: promotion evidence %s: %w", req.VolumeName, err)
	}
	if snap.HasProjection {
		resp.ReceiverReady = snap.Projection.Readiness.ReplicaReady
		switch {
		case req.ExpectedEpoch != 0 && resp.Epoch != req.ExpectedEpoch:
			resp.Reason = "epoch_mismatch"
		case !snap.Projection.Readiness.RoleApplied:
			resp.Reason = "role_not_applied"
		case snap.Projection.Mode.Name == "needs_rebuild":
			resp.Reason = "needs_rebuild"
		default:
			resp.Eligible = true
		}
	} else if req.ExpectedEpoch != 0 && resp.Epoch != req.ExpectedEpoch {
		resp.Reason = "epoch_mismatch"
	} else {
		resp.Eligible = true
	}
	return resp, nil
}

// QueryReplicaSummary returns a bounded takeover/reconstruction summary for one
// volume. It preserves distinct LSN semantics while avoiding raw internal
// shipper/session detail.
func (n *Node) QueryReplicaSummary(req protocolv2.ReplicaSummaryRequest) (protocolv2.ReplicaSummaryResponse, error) {
	path, err := n.pathFor(req.VolumeName)
	if err != nil {
		return protocolv2.ReplicaSummaryResponse{}, err
	}
	snap, err := n.dataPlane.Snapshot(path)
	if err != nil {
		return protocolv2.ReplicaSummaryResponse{}, fmt.Errorf("volumev2: snapshot %s: %w", req.VolumeName, err)
	}
	resp := protocolv2.ReplicaSummaryResponse{
		VolumeName: req.VolumeName,
		NodeID:     n.id,
		Epoch:      snap.Status.Epoch,
		Role:       snap.Status.Role.String(),
	}
	var status blockvol.V2StatusSnapshot
	if err := n.dataPlane.WithVolume(path, func(vol *blockvol.BlockVol) error {
		status = vol.StatusSnapshot()
		return nil
	}); err != nil {
		return protocolv2.ReplicaSummaryResponse{}, fmt.Errorf("volumev2: replica summary %s: %w", req.VolumeName, err)
	}
	resp.CommittedLSN = status.CommittedLSN
	resp.CheckpointLSN = status.CheckpointLSN

	if snap.HasProjection {
		resp.Mode = string(snap.Projection.Mode.Name)
		resp.ModeReason = snap.Projection.Mode.Reason
		resp.RoleApplied = snap.Projection.Readiness.RoleApplied
		resp.ReceiverReady = snap.Projection.Readiness.ReplicaReady
		resp.DurableLSN = snap.Projection.Boundary.DurableLSN
		resp.TargetLSN = snap.Projection.Boundary.TargetLSN
		resp.AchievedLSN = snap.Projection.Boundary.AchievedLSN
		resp.LastBarrierOK = snap.Projection.Boundary.LastBarrierOK
		resp.LastBarrierReason = snap.Projection.Boundary.LastBarrierReason
	}
	if snap.HasCoreState {
		resp.RecoveryPhase = string(snap.CoreState.Recovery.Phase)
		if snap.CoreState.Boundary.DurableLSN > resp.DurableLSN {
			resp.DurableLSN = snap.CoreState.Boundary.DurableLSN
		}
		if snap.CoreState.Boundary.TargetLSN > resp.TargetLSN {
			resp.TargetLSN = snap.CoreState.Boundary.TargetLSN
		}
		if snap.CoreState.Boundary.AchievedLSN > resp.AchievedLSN {
			resp.AchievedLSN = snap.CoreState.Boundary.AchievedLSN
		}
		if snap.CoreState.Boundary.CheckpointLSN > resp.CheckpointLSN {
			resp.CheckpointLSN = snap.CoreState.Boundary.CheckpointLSN
		}
		if snap.CoreState.Boundary.LastBarrierReason != "" {
			resp.LastBarrierReason = snap.CoreState.Boundary.LastBarrierReason
		}
		resp.LastBarrierOK = snap.CoreState.Boundary.LastBarrierOK
	}

	switch {
	case req.ExpectedEpoch != 0 && resp.Epoch != req.ExpectedEpoch:
		resp.Reason = "epoch_mismatch"
	case !resp.RoleApplied:
		resp.Reason = "role_not_applied"
	case resp.Mode == "needs_rebuild":
		resp.Reason = "needs_rebuild"
	default:
		resp.Eligible = true
	}

	return resp, nil
}

// WriteLBA writes data to one named local volume.
func (n *Node) WriteLBA(name string, lba uint64, data []byte) error {
	path, err := n.pathFor(name)
	if err != nil {
		return err
	}
	return n.dataPlane.WriteLBA(path, lba, data)
}

// ReadLBA reads data from one named local volume.
func (n *Node) ReadLBA(name string, lba uint64, length uint32) ([]byte, error) {
	path, err := n.pathFor(name)
	if err != nil {
		return nil, err
	}
	return n.dataPlane.ReadLBA(path, lba, length)
}

// SyncCache flushes one named local volume.
func (n *Node) SyncCache(name string) error {
	path, err := n.pathFor(name)
	if err != nil {
		return err
	}
	return n.dataPlane.SyncCache(path)
}

// Snapshot returns the local debug snapshot for one named volume.
func (n *Node) Snapshot(name string) (purev2.VolumeDebugSnapshot, error) {
	path, err := n.pathFor(name)
	if err != nil {
		return purev2.VolumeDebugSnapshot{}, err
	}
	return n.dataPlane.Snapshot(path)
}

// Close shuts down the underlying pure runtime.
func (n *Node) Close() {
	if n == nil || n.dataPlane == nil {
		return
	}
	n.dataPlane.Close()
}

func (n *Node) pathFor(name string) (string, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	binding, ok := n.volumes[name]
	if !ok {
		return "", fmt.Errorf("volumev2: unknown volume %q", name)
	}
	return binding.path, nil
}
