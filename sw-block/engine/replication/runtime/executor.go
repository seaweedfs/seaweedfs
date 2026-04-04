package runtime

import engine "github.com/seaweedfs/seaweedfs/sw-block/engine/replication"

// RecoveryCallbacks is the host-side callback interface for recovery execution.
// The runtime helper drives plan execution; the host supplies concrete
// IO bindings and receives completion notifications.
type RecoveryCallbacks interface {
	// OnCatchUpCompleted is called after successful catch-up execution.
	OnCatchUpCompleted(volumeID string, achievedLSN uint64)

	// OnRebuildCompleted is called after successful rebuild execution.
	// The host should read the post-rebuild snapshot and emit the
	// appropriate core event.
	OnRebuildCompleted(volumeID string, plan *engine.RecoveryPlan)
}

// ExecuteCatchUpPlan runs a catch-up plan using the supplied IO binding
// and notifies the host on completion. Returns an error if execution fails.
func ExecuteCatchUpPlan(
	driver *engine.RecoveryDriver,
	plan *engine.RecoveryPlan,
	io engine.CatchUpIO,
	volumeID string,
	callbacks RecoveryCallbacks,
) error {
	exec := engine.NewCatchUpExecutor(driver, plan)
	exec.IO = io
	if err := exec.Execute(nil, 0); err != nil {
		return err
	}
	if callbacks != nil {
		achievedLSN := plan.CatchUpTarget
		if achievedLSN == 0 {
			achievedLSN = plan.CatchUpStartLSN
		}
		callbacks.OnCatchUpCompleted(volumeID, achievedLSN)
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
	callbacks RecoveryCallbacks,
) error {
	exec := engine.NewRebuildExecutor(driver, plan)
	exec.IO = io
	if err := exec.Execute(); err != nil {
		return err
	}
	if callbacks != nil {
		callbacks.OnRebuildCompleted(volumeID, plan)
	}
	return nil
}
