package masterv2

import (
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/sw-block/runtime/protocolv2"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// Config defines the minimal masterv2 control-loop settings.
type Config struct {
	LeaseTTL time.Duration
}

// VolumeSpec is one master-owned desired volume intent.
type VolumeSpec struct {
	Name          string
	Path          string
	PrimaryNodeID string
	CreateOptions blockvol.CreateOptions
}

// Assignment is the minimal masterv2 -> volumev2 control message.
type Assignment = protocolv2.Assignment

// VolumeHeartbeat is the minimal volumev2 -> masterv2 periodic identity observation.
type VolumeHeartbeat = protocolv2.VolumeHeartbeat

// NodeHeartbeat is the minimal per-node heartbeat used by the POC control loop.
type NodeHeartbeat = protocolv2.NodeHeartbeat

// PromotionQueryRequest asks one candidate for fresh failover evidence.
type PromotionQueryRequest = protocolv2.PromotionQueryRequest

// PromotionQueryResponse returns fresh candidate evidence at query time.
type PromotionQueryResponse = protocolv2.PromotionQueryResponse

// VolumeView is the master-side observed state for one desired volume.
type VolumeView struct {
	Name            string
	Path            string
	PrimaryNodeID   string
	DesiredEpoch    uint64
	ObservedEpoch   uint64
	ObservedRole    string
	Mode            string
	ModeReason      string
	CommittedLSN    uint64
	RoleApplied     bool
	ReplicaReady    bool
	LastHeartbeatAt time.Time
}

type desiredVolume struct {
	spec  VolumeSpec
	epoch uint64
}

// Master is a small in-process V2 control plane POC.
// It owns desired state and emits assignments on heartbeats.
type Master struct {
	mu      sync.RWMutex
	cfg     Config
	desired map[string]desiredVolume
	views   map[string]VolumeView
}

// New creates the minimal masterv2 POC.
func New(cfg Config) *Master {
	if cfg.LeaseTTL <= 0 {
		cfg.LeaseTTL = 30 * time.Second
	}
	return &Master{
		cfg:     cfg,
		desired: make(map[string]desiredVolume),
		views:   make(map[string]VolumeView),
	}
}

// DeclarePrimary records one desired RF1 primary placement.
func (m *Master) DeclarePrimary(spec VolumeSpec) error {
	if spec.Name == "" {
		return fmt.Errorf("masterv2: volume name is required")
	}
	if spec.Path == "" {
		return fmt.Errorf("masterv2: volume path is required")
	}
	if spec.PrimaryNodeID == "" {
		return fmt.Errorf("masterv2: primary node id is required")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.desired[spec.Name]
	if !ok {
		entry = desiredVolume{epoch: 1}
	} else if entry.spec.Path != spec.Path || entry.spec.PrimaryNodeID != spec.PrimaryNodeID || entry.spec.CreateOptions != spec.CreateOptions {
		entry.epoch++
	}
	entry.spec = spec
	m.desired[spec.Name] = entry
	view := m.views[spec.Name]
	view.Name = spec.Name
	view.Path = spec.Path
	view.PrimaryNodeID = spec.PrimaryNodeID
	view.DesiredEpoch = entry.epoch
	m.views[spec.Name] = view
	return nil
}

// HandleHeartbeat consumes one node heartbeat and returns any assignments the node should apply.
func (m *Master) HandleHeartbeat(hb NodeHeartbeat) ([]Assignment, error) {
	if hb.NodeID == "" {
		return nil, fmt.Errorf("masterv2: heartbeat node id is required")
	}
	if hb.ReportedAt.IsZero() {
		hb.ReportedAt = time.Now()
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	byName := make(map[string]VolumeHeartbeat, len(hb.Volumes))
	for _, vol := range hb.Volumes {
		byName[vol.Name] = vol
		view := m.views[vol.Name]
		view.Name = vol.Name
		view.Path = vol.Path
		view.ObservedEpoch = vol.Epoch
		view.ObservedRole = vol.Role
		view.Mode = vol.Mode
		view.ModeReason = vol.ModeReason
		view.CommittedLSN = vol.CommittedLSN
		view.RoleApplied = vol.RoleApplied
		view.ReplicaReady = vol.ReplicaReady
		view.LastHeartbeatAt = hb.ReportedAt
		if desired, ok := m.desired[vol.Name]; ok {
			view.PrimaryNodeID = desired.spec.PrimaryNodeID
			view.DesiredEpoch = desired.epoch
		}
		m.views[vol.Name] = view
	}

	var assignments []Assignment
	names := make([]string, 0, len(m.desired))
	for name := range m.desired {
		names = append(names, name)
	}
	slices.Sort(names)

	for _, name := range names {
		desired := m.desired[name]
		if desired.spec.PrimaryNodeID != hb.NodeID {
			continue
		}
		reported, ok := byName[name]
		if ok && reported.Path == desired.spec.Path && reported.Epoch == desired.epoch && reported.Role == "primary" && reported.RoleApplied {
			continue
		}
		assignments = append(assignments, m.primaryAssignment(desired))
	}
	return assignments, nil
}

// Volume returns the latest master-side view for one volume.
func (m *Master) Volume(name string) (VolumeView, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	view, ok := m.views[name]
	return view, ok
}

// SelectPromotionCandidate chooses the best eligible candidate from fresh
// promotion-query responses. Selection is durability-first: highest
// CommittedLSN wins, then WALHeadLSN as a weaker tiebreaker.
func (m *Master) SelectPromotionCandidate(responses []PromotionQueryResponse) (PromotionQueryResponse, error) {
	candidates := make([]PromotionQueryResponse, 0, len(responses))
	for _, resp := range responses {
		if resp.Eligible {
			candidates = append(candidates, resp)
		}
	}
	if len(candidates) == 0 {
		return PromotionQueryResponse{}, fmt.Errorf("masterv2: no eligible promotion candidates")
	}
	slices.SortStableFunc(candidates, func(a, b PromotionQueryResponse) int {
		if a.CommittedLSN != b.CommittedLSN {
			if a.CommittedLSN > b.CommittedLSN {
				return -1
			}
			return 1
		}
		if a.WALHeadLSN != b.WALHeadLSN {
			if a.WALHeadLSN > b.WALHeadLSN {
				return -1
			}
			return 1
		}
		switch {
		case a.NodeID < b.NodeID:
			return -1
		case a.NodeID > b.NodeID:
			return 1
		default:
			return 0
		}
	})
	return candidates[0], nil
}

// AuthorizePromotion selects the best candidate from fresh promotion evidence,
// advances desired ownership when needed, and returns the assignment the chosen
// node should apply. This is authorization only; takeover reconstruction and
// activation remain the new primary's responsibility.
func (m *Master) AuthorizePromotion(volumeName string, responses []PromotionQueryResponse) (Assignment, error) {
	if volumeName == "" {
		return Assignment{}, fmt.Errorf("masterv2: volume name is required")
	}
	selected, err := m.SelectPromotionCandidate(responses)
	if err != nil {
		return Assignment{}, err
	}
	if selected.VolumeName != "" && selected.VolumeName != volumeName {
		return Assignment{}, fmt.Errorf("masterv2: promotion candidate volume %q does not match %q", selected.VolumeName, volumeName)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	desired, ok := m.desired[volumeName]
	if !ok {
		return Assignment{}, fmt.Errorf("masterv2: unknown volume %q", volumeName)
	}
	if desired.spec.PrimaryNodeID != selected.NodeID {
		desired.spec.PrimaryNodeID = selected.NodeID
		desired.epoch++
		m.desired[volumeName] = desired
	}

	view := m.views[volumeName]
	view.Name = desired.spec.Name
	view.Path = desired.spec.Path
	view.PrimaryNodeID = desired.spec.PrimaryNodeID
	view.DesiredEpoch = desired.epoch
	m.views[volumeName] = view

	return m.primaryAssignment(desired), nil
}

func (m *Master) primaryAssignment(desired desiredVolume) Assignment {
	return Assignment{
		Name:          desired.spec.Name,
		Path:          desired.spec.Path,
		NodeID:        desired.spec.PrimaryNodeID,
		Epoch:         desired.epoch,
		LeaseTTL:      m.cfg.LeaseTTL,
		CreateOptions: desired.spec.CreateOptions,
		Role:          "primary",
	}
}
