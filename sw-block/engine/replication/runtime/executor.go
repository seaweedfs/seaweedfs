package runtime

import (
	"errors"
	"strings"

	engine "github.com/seaweedfs/seaweedfs/sw-block/engine/replication"
)

// RecoveryCallbacks is the host-side callback interface for recovery execution.
// The runtime helper drives plan execution; the host supplies concrete
// IO bindings and receives completion notifications.
type RecoveryCallbacks interface {
	// OnRecoveryProgress is called when replay/rebuild reaches an explicit
	// achieved boundary but the recovery session is not yet fully closed.
	OnRecoveryProgress(volumeID, replicaID string, achievedLSN uint64)

	// OnCatchUpCompleted is called after successful catch-up execution.
	OnCatchUpCompleted(volumeID, replicaID string, achievedLSN uint64)

	// OnCatchUpFailed is called when catch-up execution fails with a
	// classified reason that the host may need to surface into core events.
	OnCatchUpFailed(volumeID, replicaID, reason string)

	// OnRebuildCompleted is called after successful rebuild execution.
	// The host should read the post-rebuild snapshot and emit the
	// appropriate core event.
	OnRebuildCompleted(volumeID, replicaID string, plan *engine.RecoveryPlan)
}

// ExecuteCatchUpPlan runs a catch-up plan using the supplied IO binding
// and notifies the host on completion. Returns an error if execution fails.
func ExecuteCatchUpPlan(
	driver *engine.RecoveryDriver,
	plan *engine.RecoveryPlan,
	io engine.CatchUpIO,
	volumeID string,
	replicaID string,
	callbacks RecoveryCallbacks,
) error {
	exec := engine.NewCatchUpExecutor(driver, plan)
	exec.IO = io
	if err := exec.Execute(nil, 0); err != nil {
		if callbacks != nil {
			callbacks.OnCatchUpFailed(volumeID, replicaID, classifyCatchUpFailure(err))
		}
		return err
	}
	if callbacks != nil {
		achievedLSN := plan.CatchUpTarget
		if achievedLSN == 0 {
			achievedLSN = plan.CatchUpStartLSN
		}
		callbacks.OnRecoveryProgress(volumeID, replicaID, achievedLSN)
		callbacks.OnCatchUpCompleted(volumeID, replicaID, achievedLSN)
	}
	return nil
}

// ExecuteRebuildPlan runs a rebuild plan using the supplied IO binding
// and notifies the host on completion. Returns an error if execution fails.
func ExecuteRebuildPlan(
	driver *engine.RecoveryDriver,
	plan *engine.RecoveryPlan,
	io engine.RebuildIO,
	volumeID string,
	replicaID string,
	callbacks RecoveryCallbacks,
) error {
	exec := engine.NewRebuildExecutor(driver, plan)
	exec.IO = io
	if err := exec.Execute(); err != nil {
		return err
	}
	if callbacks != nil {
		callbacks.OnRecoveryProgress(volumeID, replicaID, plan.RebuildTargetLSN)
		callbacks.OnRebuildCompleted(volumeID, replicaID, plan)
	}
	return nil
}

func classifyCatchUpFailure(err error) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	switch {
	case errors.Is(err, engine.ErrTruncationUnsafe):
		return "truncation_unsafe"
	case strings.Contains(msg, "WAL recycled"):
		return "retention_lost"
	case strings.Contains(msg, "duration_exceeded"):
		return "catchup_duration_exceeded"
	case strings.Contains(msg, "progress_stalled"):
		return "catchup_progress_stalled"
	case strings.Contains(msg, "entries_limit_exceeded"):
		return "catchup_entries_limit_exceeded"
	case strings.Contains(msg, "budget violation"):
		return "catchup_budget_exceeded"
	default:
		return ""
	}
}
