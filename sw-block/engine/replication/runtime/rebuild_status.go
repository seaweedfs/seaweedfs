package runtime

import engine "github.com/seaweedfs/seaweedfs/sw-block/engine/replication"

// RebuildCompletionStatus holds the post-rebuild state read from the backend.
// The host adapter fills this after a successful rebuild; the runtime helper
// uses it to emit the core RebuildCommitted event.
type RebuildCompletionStatus struct {
	CommittedLSN  uint64
	CheckpointLSN uint64
}

// DeriveRebuildCommitted computes the RebuildCommitted event from
// post-rebuild status and the original plan. This is the reusable
// shaping logic — the host only needs to supply the raw snapshot values.
func DeriveRebuildCommitted(volumeID string, status RebuildCompletionStatus, plan *engine.RecoveryPlan) engine.RebuildCommitted {
	flushedLSN := status.CommittedLSN
	if flushedLSN == 0 {
		flushedLSN = plan.RebuildTargetLSN
	}
	checkpointLSN := status.CheckpointLSN
	if checkpointLSN == 0 {
		checkpointLSN = plan.RebuildTargetLSN
	}
	achievedLSN := flushedLSN
	if checkpointLSN > achievedLSN {
		achievedLSN = checkpointLSN
	}
	return engine.RebuildCommitted{
		ID:            volumeID,
		AchievedLSN:   achievedLSN,
		FlushedLSN:    flushedLSN,
		CheckpointLSN: checkpointLSN,
	}
}
