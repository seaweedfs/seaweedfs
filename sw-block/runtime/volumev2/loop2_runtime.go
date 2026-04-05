package volumev2

import (
	"fmt"
	"slices"

	"github.com/seaweedfs/seaweedfs/sw-block/runtime/protocolv2"
)

// Loop2RuntimeMode is the coarse active runtime mode for the primary-led
// data-control loop. It is a runtime-owned view, not a public protocol surface.
type Loop2RuntimeMode string

const (
	Loop2RuntimeModeKeepUp       Loop2RuntimeMode = "keepup"
	Loop2RuntimeModeCatchingUp   Loop2RuntimeMode = "catching_up"
	Loop2RuntimeModeDegraded     Loop2RuntimeMode = "degraded"
	Loop2RuntimeModeNeedsRebuild Loop2RuntimeMode = "needs_rebuild"
)

// Loop2ReplicaStatus is the bounded per-replica view exposed by the active
// Loop 2 runtime session.
type Loop2ReplicaStatus struct {
	NodeID            string
	Role              string
	Mode              string
	Reason            string
	RecoveryPhase     string
	CommittedLSN      uint64
	DurableLSN        uint64
	CheckpointLSN     uint64
	TargetLSN         uint64
	AchievedLSN       uint64
	LastBarrierOK     bool
	LastBarrierReason string
}

// Loop2RuntimeSnapshot is the runtime-owned active data-control view for one
// volume at one observation point.
type Loop2RuntimeSnapshot struct {
	VolumeName          string
	PrimaryNodeID       string
	ExpectedEpoch       uint64
	Mode                Loop2RuntimeMode
	Reason              string
	ReplicaCount        int
	HealthyReplicaCount int
	CommittedLSN        uint64
	DurableFloorLSN     uint64
	TargetLSN           uint64
	AchievedLSN         uint64
	Replicas            []Loop2ReplicaStatus
}

// Loop2RuntimeSession is the first runtime-owned active Loop 2 controller. It
// periodically observes bounded replica summaries and derives a primary-led
// runtime mode beyond failover-only logic.
type Loop2RuntimeSession struct {
	volumeName    string
	primaryNodeID string
	expectedEpoch uint64
	targets       []FailoverTarget

	snapshot Loop2RuntimeSnapshot
	lastErr  error
}

// NewLoop2RuntimeSession creates a new active Loop 2 session over explicit
// failover targets.
func NewLoop2RuntimeSession(volumeName, primaryNodeID string, expectedEpoch uint64, targets []FailoverTarget) (*Loop2RuntimeSession, error) {
	if volumeName == "" {
		return nil, fmt.Errorf("volumev2: loop2 volume name is required")
	}
	if primaryNodeID == "" {
		return nil, fmt.Errorf("volumev2: loop2 primary node id is required")
	}
	if len(targets) == 0 {
		return nil, fmt.Errorf("volumev2: loop2 targets are required")
	}
	return &Loop2RuntimeSession{
		volumeName:    volumeName,
		primaryNodeID: primaryNodeID,
		expectedEpoch: expectedEpoch,
		targets:       targets,
	}, nil
}

// ObserveOnce collects bounded replica summaries and refreshes the active Loop
// 2 runtime snapshot.
func (s *Loop2RuntimeSession) ObserveOnce() (Loop2RuntimeSnapshot, error) {
	if s == nil {
		return Loop2RuntimeSnapshot{}, fmt.Errorf("volumev2: loop2 session is nil")
	}
	req := protocolv2.ReplicaSummaryRequest{
		VolumeName:    s.volumeName,
		ExpectedEpoch: s.expectedEpoch,
	}
	summaries := make([]protocolv2.ReplicaSummaryResponse, 0, len(s.targets))
	for _, target := range s.targets {
		if target.NodeID == "" || target.Evidence == nil {
			continue
		}
		summary, err := target.Evidence.QueryReplicaSummary(req)
		if err != nil {
			s.lastErr = fmt.Errorf("volumev2: loop2 summary %s: %w", s.volumeName, err)
			return s.snapshot, s.lastErr
		}
		summaries = append(summaries, summary)
	}
	snapshot, err := evaluateLoop2Runtime(s.volumeName, s.primaryNodeID, s.expectedEpoch, summaries)
	if err != nil {
		s.lastErr = err
		return s.snapshot, err
	}
	s.snapshot = snapshot
	s.lastErr = nil
	return snapshot, nil
}

// Snapshot returns the latest active Loop 2 runtime snapshot.
func (s *Loop2RuntimeSession) Snapshot() Loop2RuntimeSnapshot {
	if s == nil {
		return Loop2RuntimeSnapshot{}
	}
	return s.snapshot
}

// LastError returns the last Loop 2 observation error.
func (s *Loop2RuntimeSession) LastError() error {
	if s == nil {
		return fmt.Errorf("volumev2: loop2 session is nil")
	}
	return s.lastErr
}

func evaluateLoop2Runtime(volumeName, primaryNodeID string, expectedEpoch uint64, summaries []protocolv2.ReplicaSummaryResponse) (Loop2RuntimeSnapshot, error) {
	if len(summaries) == 0 {
		return Loop2RuntimeSnapshot{}, fmt.Errorf("volumev2: loop2 summaries are required")
	}
	slices.SortFunc(summaries, func(a, b protocolv2.ReplicaSummaryResponse) int {
		switch {
		case a.NodeID < b.NodeID:
			return -1
		case a.NodeID > b.NodeID:
			return 1
		default:
			return 0
		}
	})

	var (
		primary        protocolv2.ReplicaSummaryResponse
		foundPrimary   bool
		durableFloor   uint64
		maxTarget      uint64
		minAchieved    uint64
		healthyCount   int
		mode           = Loop2RuntimeModeKeepUp
		reason         string
		replicas       = make([]Loop2ReplicaStatus, 0, len(summaries))
		minAchievedSet bool
	)

	for _, summary := range summaries {
		replicas = append(replicas, Loop2ReplicaStatus{
			NodeID:            summary.NodeID,
			Role:              summary.Role,
			Mode:              summary.Mode,
			Reason:            summary.Reason,
			RecoveryPhase:     summary.RecoveryPhase,
			CommittedLSN:      summary.CommittedLSN,
			DurableLSN:        summary.DurableLSN,
			CheckpointLSN:     summary.CheckpointLSN,
			TargetLSN:         summary.TargetLSN,
			AchievedLSN:       summary.AchievedLSN,
			LastBarrierOK:     summary.LastBarrierOK,
			LastBarrierReason: summary.LastBarrierReason,
		})
		if summary.NodeID == primaryNodeID {
			primary = summary
			foundPrimary = true
		}
		if summary.Epoch == expectedEpoch && summary.Eligible {
			healthyCount++
		}
		if durableFloor == 0 || summary.DurableLSN < durableFloor {
			durableFloor = summary.DurableLSN
		}
		if summary.TargetLSN > maxTarget {
			maxTarget = summary.TargetLSN
		}
		if !minAchievedSet || summary.AchievedLSN < minAchieved {
			minAchieved = summary.AchievedLSN
			minAchievedSet = true
		}
	}
	if !foundPrimary {
		return Loop2RuntimeSnapshot{}, fmt.Errorf("volumev2: loop2 primary %q missing from summaries", primaryNodeID)
	}

	for _, summary := range summaries {
		switch {
		case summary.Epoch != expectedEpoch:
			mode = degradeLoop2Mode(mode, Loop2RuntimeModeDegraded)
			if reason == "" {
				reason = "peer_epoch_mismatch"
			}
		case summary.Mode == "needs_rebuild" || summary.Reason == "needs_rebuild" || summary.RecoveryPhase == "needs_rebuild":
			mode = Loop2RuntimeModeNeedsRebuild
			if reason == "" {
				reason = "needs_rebuild"
			}
		case !summary.LastBarrierOK && summary.LastBarrierReason != "":
			mode = degradeLoop2Mode(mode, Loop2RuntimeModeDegraded)
			if reason == "" {
				reason = summary.LastBarrierReason
			}
		case summary.NodeID != primaryNodeID && (summary.RecoveryPhase == "catching_up" || summary.RecoveryPhase == "rebuilding"):
			mode = degradeLoop2Mode(mode, Loop2RuntimeModeCatchingUp)
			if reason == "" {
				reason = summary.RecoveryPhase
			}
		case summary.NodeID != primaryNodeID && summary.TargetLSN > 0 && summary.AchievedLSN < summary.TargetLSN:
			mode = degradeLoop2Mode(mode, Loop2RuntimeModeCatchingUp)
			if reason == "" {
				reason = "peer_target_not_achieved"
			}
		case summary.NodeID != primaryNodeID && summary.DurableLSN < primary.CommittedLSN:
			mode = degradeLoop2Mode(mode, Loop2RuntimeModeCatchingUp)
			if reason == "" {
				reason = "peer_durable_behind_primary"
			}
		}
	}

	return Loop2RuntimeSnapshot{
		VolumeName:          volumeName,
		PrimaryNodeID:       primaryNodeID,
		ExpectedEpoch:       expectedEpoch,
		Mode:                mode,
		Reason:              reason,
		ReplicaCount:        len(summaries),
		HealthyReplicaCount: healthyCount,
		CommittedLSN:        primary.CommittedLSN,
		DurableFloorLSN:     durableFloor,
		TargetLSN:           maxTarget,
		AchievedLSN:         minAchieved,
		Replicas:            replicas,
	}, nil
}

func degradeLoop2Mode(current, next Loop2RuntimeMode) Loop2RuntimeMode {
	if loop2ModeRank(next) > loop2ModeRank(current) {
		return next
	}
	return current
}

func loop2ModeRank(mode Loop2RuntimeMode) int {
	switch mode {
	case Loop2RuntimeModeNeedsRebuild:
		return 3
	case Loop2RuntimeModeDegraded:
		return 2
	case Loop2RuntimeModeCatchingUp:
		return 1
	default:
		return 0
	}
}
