package volumev2

import (
	"fmt"

	"github.com/seaweedfs/seaweedfs/sw-block/runtime/protocolv2"
)

// ReconstructedPrimaryTruth is the bounded truth a newly selected primary can
// derive from replica summaries before resuming data-control ownership.
type ReconstructedPrimaryTruth struct {
	VolumeName    string
	PrimaryNodeID string
	Epoch         uint64
	CommittedLSN  uint64
	DurableLSN    uint64
	CheckpointLSN uint64
	TargetLSN     uint64
	AchievedLSN   uint64
	RecoveryPhase string
	ReplicaCount  int
	Degraded      bool
	NeedsRebuild  bool
	Reason        string
}

// ReconstructPrimaryTruth derives a bounded recovery view for a newly chosen
// primary from the latest replica summaries. It intentionally stays smaller
// than the full internal engine/session graph and fail-closes on ambiguous
// epoch or recovery signals.
func ReconstructPrimaryTruth(primaryNodeID string, summaries []protocolv2.ReplicaSummaryResponse) (ReconstructedPrimaryTruth, error) {
	if primaryNodeID == "" {
		return ReconstructedPrimaryTruth{}, fmt.Errorf("volumev2: primary node id is required")
	}
	if len(summaries) == 0 {
		return ReconstructedPrimaryTruth{}, fmt.Errorf("volumev2: replica summaries are required")
	}

	var (
		selected          protocolv2.ReplicaSummaryResponse
		foundSelected     bool
		recoveryObserved  bool
		aggregateTarget   uint64
		aggregateAchieved uint64
	)
	for _, summary := range summaries {
		if summary.NodeID == primaryNodeID {
			selected = summary
			foundSelected = true
			break
		}
	}
	if !foundSelected {
		return ReconstructedPrimaryTruth{}, fmt.Errorf("volumev2: selected primary %q missing from summaries", primaryNodeID)
	}
	if !selected.Eligible {
		return ReconstructedPrimaryTruth{}, fmt.Errorf("volumev2: selected primary %q is not eligible: %s", primaryNodeID, selected.Reason)
	}

	result := ReconstructedPrimaryTruth{
		VolumeName:    selected.VolumeName,
		PrimaryNodeID: selected.NodeID,
		Epoch:         selected.Epoch,
		CommittedLSN:  selected.CommittedLSN,
		DurableLSN:    selected.DurableLSN,
		CheckpointLSN: selected.CheckpointLSN,
		TargetLSN:     selected.TargetLSN,
		AchievedLSN:   selected.AchievedLSN,
		RecoveryPhase: selected.RecoveryPhase,
	}

	for _, summary := range summaries {
		if summary.VolumeName != selected.VolumeName {
			return ReconstructedPrimaryTruth{}, fmt.Errorf("volumev2: mixed volume summaries %q and %q", selected.VolumeName, summary.VolumeName)
		}
		if summary.Mode == "needs_rebuild" || summary.Reason == "needs_rebuild" {
			result.NeedsRebuild = true
		}
		if !summary.LastBarrierOK && summary.LastBarrierReason != "" {
			result.Degraded = true
			if result.Reason == "" {
				result.Reason = summary.LastBarrierReason
			}
		}
		if summary.Epoch != selected.Epoch {
			result.Degraded = true
			result.Reason = "peer_epoch_mismatch"
			continue
		}
		result.ReplicaCount++

		if summary.CommittedLSN > result.CommittedLSN {
			result.Degraded = true
			result.Reason = "selected_not_most_recent"
		}
		if isRecoveryPhase(summary.RecoveryPhase) {
			if !recoveryObserved {
				aggregateTarget = summary.TargetLSN
				aggregateAchieved = summary.AchievedLSN
				recoveryObserved = true
			} else {
				if summary.TargetLSN > aggregateTarget {
					aggregateTarget = summary.TargetLSN
				}
				if summary.AchievedLSN < aggregateAchieved {
					aggregateAchieved = summary.AchievedLSN
				}
			}
			result.RecoveryPhase = mergeRecoveryPhase(result.RecoveryPhase, summary.RecoveryPhase)
		}
	}

	if recoveryObserved {
		result.TargetLSN = aggregateTarget
		result.AchievedLSN = aggregateAchieved
	}
	if result.NeedsRebuild {
		result.RecoveryPhase = "needs_rebuild"
	}
	return result, nil
}

func isRecoveryPhase(phase string) bool {
	switch phase {
	case "catching_up", "rebuilding", "needs_rebuild":
		return true
	default:
		return false
	}
}

func mergeRecoveryPhase(current, next string) string {
	if recoveryRank(next) > recoveryRank(current) {
		return next
	}
	return current
}

func recoveryRank(phase string) int {
	switch phase {
	case "needs_rebuild":
		return 3
	case "rebuilding":
		return 2
	case "catching_up":
		return 1
	default:
		return 0
	}
}
