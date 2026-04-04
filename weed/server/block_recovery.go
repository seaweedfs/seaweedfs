package weed_server

import (
	"context"
	"fmt"
	"sync"

	engine "github.com/seaweedfs/seaweedfs/sw-block/engine/replication"
	rt "github.com/seaweedfs/seaweedfs/sw-block/engine/replication/runtime"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/v2bridge"
)

// recoveryTask tracks a live recovery goroutine for one replica target.
// The task pointer serves as identity token — only the goroutine that owns
// THIS pointer may mark it as done.
type recoveryTask struct {
	replicaID string
	cancel    context.CancelFunc
	done      chan struct{} // closed when the goroutine exits
}

// RecoveryManager owns live recovery execution for all replica targets.
//
// Ownership model:
//   - At most one recovery goroutine per replicaID at any time.
//   - On supersede/replace: the old goroutine is cancelled AND drained
//     before the replacement starts. No overlap.
//   - Cancellation: context cancel + session invalidation (for removal/shutdown).
//     For supersede: context cancel only (engine already attached replacement session).
type RecoveryManager struct {
	bs *BlockService

	mu    sync.Mutex
	tasks map[string]*recoveryTask
	coord *rt.PendingCoordinator
	wg    sync.WaitGroup

	// TestHook: if set, called before execution starts. Tests use this
	// to hold the goroutine alive for serialized-replacement proofs.
	OnBeforeExecute func(replicaID string)

	// TestHook: if set, may adjust a freshly cached pending execution before
	// the core event is emitted. Used only by focused ownership tests.
	OnPendingExecution func(volumeID string, pending *rt.PendingExecution)
}

func NewRecoveryManager(bs *BlockService) *RecoveryManager {
	rm := &RecoveryManager{
		bs:    bs,
		tasks: make(map[string]*recoveryTask),
	}
	rm.coord = rt.NewPendingCoordinator(func(pe *rt.PendingExecution, reason string) {
		if pe != nil && pe.Driver != nil && pe.Plan != nil {
			pe.Driver.CancelPlan(pe.Plan, reason)
		}
	})
	return rm
}

// === LEGACY NO-CORE COMPATIBILITY ===
//
// The following methods (HandleAssignmentResult, HandleRemovedAssignments)
// preserve pre-Phase-16 behavior for no-core paths and older tests.
// Core-present paths use StartRecoveryTask + ExecutePendingCatchUp/Rebuild
// instead. These legacy entry points should NOT be strengthened into
// semantic-authority proofs — they are compatibility guards only.

// HandleAssignmentResult preserves the pre-16D behavior for no-core paths and
// older tests: session creation/supersede results directly start recovery
// goroutines. Core-present paths should use StartRecoveryTask instead.
func (rm *RecoveryManager) HandleAssignmentResult(result engine.AssignmentResult, assignments []blockvol.BlockVolumeAssignment) {
	for _, replicaID := range result.Removed {
		rm.cancelAndDrain(replicaID, true)
	}
	for _, replicaID := range result.SessionsSuperseded {
		rm.cancelAndDrain(replicaID, false)
		rm.startTask(replicaID, assignments)
	}
	for _, replicaID := range result.SessionsCreated {
		rm.cancelAndDrain(replicaID, false)
		rm.startTask(replicaID, assignments)
	}
}

// HandleRemovedAssignments drains tasks for senders removed by registry
// reconciliation. Recovery task startup is handled separately by core command
// execution on the bounded live path.
func (rm *RecoveryManager) HandleRemovedAssignments(result engine.AssignmentResult) {
	for _, replicaID := range result.Removed {
		rm.cancelAndDrain(replicaID, true)
	}
}

// StartRecoveryTask starts one bounded recovery goroutine from a core-emitted
// command. Any stale task for the same replica is drained first.
func (rm *RecoveryManager) StartRecoveryTask(replicaID string, assignments []blockvol.BlockVolumeAssignment) {
	rm.cancelAndDrain(replicaID, false)
	rm.startTask(replicaID, assignments)
}

// cancelAndDrain cancels a running task and WAITS for it to exit.
// This ensures no overlap between old and new owners.
func (rm *RecoveryManager) cancelAndDrain(replicaID string, invalidateSession bool) {
	rm.mu.Lock()
	task, ok := rm.tasks[replicaID]
	if !ok {
		rm.mu.Unlock()
		return
	}
	glog.V(1).Infof("recovery: cancelling+draining task for %s (invalidate=%v)", replicaID, invalidateSession)
	task.cancel()
	if invalidateSession && rm.bs.v2Orchestrator != nil {
		if s := rm.bs.v2Orchestrator.Registry.Sender(replicaID); s != nil {
			s.InvalidateSession("recovery_removed", engine.StateDisconnected)
		}
	}
	delete(rm.tasks, replicaID)
	doneCh := task.done
	rm.mu.Unlock()

	// Wait for the old goroutine to exit OUTSIDE the lock.
	// This serializes replacement: new task cannot start until old is fully drained.
	<-doneCh
}

// startTask creates and starts a new recovery goroutine. Caller must ensure
// no existing task for this replicaID (call cancelAndDrain first).
func (rm *RecoveryManager) startTask(replicaID string, assignments []blockvol.BlockVolumeAssignment) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	task := &recoveryTask{
		replicaID: replicaID,
		cancel:    cancel,
		done:      make(chan struct{}),
	}
	rm.tasks[replicaID] = task

	rm.wg.Add(1)
	go rm.runRecovery(ctx, task, assignments)
}

// Shutdown cancels all active recovery tasks and waits for drain.
func (rm *RecoveryManager) Shutdown() {
	rm.mu.Lock()
	for _, task := range rm.tasks {
		task.cancel()
		if rm.bs.v2Orchestrator != nil {
			if s := rm.bs.v2Orchestrator.Registry.Sender(task.replicaID); s != nil {
				s.InvalidateSession("recovery_shutdown", engine.StateDisconnected)
			}
		}
	}
	rm.tasks = make(map[string]*recoveryTask)
	rm.mu.Unlock()
	rm.coord.CancelAll("recovery_shutdown")
	rm.wg.Wait()
}

// ActiveTaskCount returns the number of active recovery tasks (for testing).
func (rm *RecoveryManager) ActiveTaskCount() int {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	return len(rm.tasks)
}

// DiagnosticSnapshot returns a bounded read-only snapshot of active recovery
// tasks for operator-visible diagnosis. Each entry shows the replicaID being
// recovered. This is the P3 diagnosability surface — read-only, no semantics.
type RecoveryDiagnostic struct {
	ActiveTasks []string // replicaIDs with active recovery work
}

func (rm *RecoveryManager) DiagnosticSnapshot() RecoveryDiagnostic {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	diag := RecoveryDiagnostic{}
	for id := range rm.tasks {
		diag.ActiveTasks = append(diag.ActiveTasks, id)
	}
	return diag
}

// runRecovery is the recovery goroutine for one replica target.
func (rm *RecoveryManager) runRecovery(ctx context.Context, task *recoveryTask, assignments []blockvol.BlockVolumeAssignment) {
	defer rm.wg.Done()
	defer close(task.done) // signal drain completion
	defer func() {
		rm.mu.Lock()
		// Only delete if we're still the active task (pointer comparison).
		if rm.tasks[task.replicaID] == task {
			delete(rm.tasks, task.replicaID)
		}
		rm.mu.Unlock()
	}()

	replicaID := task.replicaID

	if ctx.Err() != nil {
		return
	}

	orch := rm.bs.v2Orchestrator
	s := orch.Registry.Sender(replicaID)
	if s == nil {
		glog.V(1).Infof("recovery: sender %s not found, skipping", replicaID)
		return
	}

	sessSnap := s.SessionSnapshot()
	if sessSnap == nil {
		glog.V(1).Infof("recovery: sender %s has no active session, skipping", replicaID)
		return
	}

	glog.V(0).Infof("recovery: starting %s session for %s", sessSnap.Kind, replicaID)

	if rm.OnBeforeExecute != nil {
		rm.OnBeforeExecute(replicaID)
	}

	switch sessSnap.Kind {
	case engine.SessionCatchUp:
		rm.runCatchUp(ctx, replicaID, assignments)
	case engine.SessionRebuild:
		rm.runRebuild(ctx, replicaID, assignments)
	default:
		glog.V(1).Infof("recovery: unknown session kind %s for %s", sessSnap.Kind, replicaID)
	}
}

// recoveryContext holds the fully resolved context for one recovery execution.
// Built by resolveRecoveryContext from replicaID + assignments.
type recoveryContext struct {
	volPath           string
	rebuildAddr       string
	driver            *engine.RecoveryDriver
	executor          *v2bridge.Executor
	replicaFlushedLSN uint64 // catch-up start point (0 if no session)
}

// resolveRecoveryContext resolves everything needed for recovery execution:
// volume path, rebuild address, recovery bindings, and replica flushed progress.
// This is the single host-side context resolution for both catch-up and rebuild.
func (rm *RecoveryManager) resolveRecoveryContext(replicaID string, assignments []blockvol.BlockVolumeAssignment) (*recoveryContext, error) {
	volPath := rm.volumePathForReplica(replicaID)
	if volPath == "" {
		return nil, fmt.Errorf("cannot determine volume path for %s", replicaID)
	}

	rebuildAddr := rm.deriveRebuildAddr(replicaID, assignments)

	var bundle *v2bridge.RecoveryBundle
	if err := rm.bs.blockStore.WithVolume(volPath, func(vol *blockvol.BlockVol) error {
		bundle = v2bridge.BuildRecoveryBundle(vol, rebuildAddr)
		return nil
	}); err != nil {
		return nil, fmt.Errorf("cannot access volume %s: %w", volPath, err)
	}

	driver := &engine.RecoveryDriver{Orchestrator: rm.bs.v2Orchestrator, Storage: bundle.Storage}

	var replicaFlushedLSN uint64
	if s := rm.bs.v2Orchestrator.Registry.Sender(replicaID); s != nil {
		if snap := s.SessionSnapshot(); snap != nil {
			replicaFlushedLSN = snap.StartLSN
		}
	}

	return &recoveryContext{
		volPath:           volPath,
		rebuildAddr:       rebuildAddr,
		driver:            driver,
		executor:          bundle.Executor,
		replicaFlushedLSN: replicaFlushedLSN,
	}, nil
}

func (rm *RecoveryManager) runCatchUp(ctx context.Context, replicaID string, assignments []blockvol.BlockVolumeAssignment) {
	bs := rm.bs

	rctx, err := rm.resolveRecoveryContext(replicaID, assignments)
	if err != nil {
		glog.Warningf("recovery: %v", err)
		return
	}

	if ctx.Err() != nil {
		return
	}

	plan, err := rctx.driver.PlanRecovery(replicaID, rctx.replicaFlushedLSN)
	if err != nil {
		glog.Warningf("recovery: plan failed for %s: %v", replicaID, err)
		return
	}
	switch plan.Outcome {
	case engine.OutcomeCatchUp:
		if bs.v2Core == nil {
			rm.executeLegacyCatchUp(ctx, rctx.volPath, replicaID, rctx.driver, plan, rctx.executor)
			return
		}
		rm.coord.Store(rctx.volPath, &rt.PendingExecution{
			VolumeID:      rctx.volPath,
			ReplicaID:     replicaID,
			CatchUpTarget: plan.CatchUpTarget,
			Driver:        rctx.driver,
			Plan:          plan,
			CatchUpIO:     rctx.executor,
		})
		bs.applyCoreEvent(engine.CatchUpPlanned{ID: rctx.volPath, TargetLSN: plan.CatchUpTarget})
		if rm.coord.Has(rctx.volPath) {
			rm.coord.Cancel(rctx.volPath, "start_catchup_not_emitted")
			return
		}
	case engine.OutcomeNeedsRebuild:
		reason := "needs_rebuild"
		if plan.Proof != nil && plan.Proof.Reason != "" {
			reason = plan.Proof.Reason
		}
		bs.applyCoreEvent(engine.NeedsRebuildObserved{ID: rctx.volPath, Reason: reason})
		return
	}

	if ctx.Err() != nil {
		rctx.driver.CancelPlan(plan, "context_cancelled")
		return
	}
}

func (rm *RecoveryManager) runRebuild(ctx context.Context, replicaID string, assignments []blockvol.BlockVolumeAssignment) {
	bs := rm.bs

	rctx, err := rm.resolveRecoveryContext(replicaID, assignments)
	if err != nil {
		glog.Warningf("recovery: %v", err)
		return
	}

	if ctx.Err() != nil {
		return
	}

	plan, err := rctx.driver.PlanRebuild(replicaID)
	if err != nil {
		glog.Warningf("recovery: rebuild plan failed for %s: %v", replicaID, err)
		return
	}
	if bs.v2Core == nil {
		rm.executeLegacyRebuild(ctx, rctx.volPath, replicaID, rctx.driver, plan, rctx.executor)
		return
	}
	pe := &rt.PendingExecution{
		VolumeID:         rctx.volPath,
		ReplicaID:        replicaID,
		RebuildTargetLSN: plan.RebuildTargetLSN,
		Driver:           rctx.driver,
		Plan:             plan,
		RebuildIO:        rctx.executor,
	}
	rm.coord.Store(rctx.volPath, pe)
	if rm.OnPendingExecution != nil {
		rm.OnPendingExecution(rctx.volPath, pe)
	}
	bs.applyCoreEvent(engine.RebuildStarted{ID: rctx.volPath, TargetLSN: plan.RebuildTargetLSN})
	if rm.coord.Has(rctx.volPath) {
		rm.coord.Cancel(rctx.volPath, "start_rebuild_not_emitted")
	}
}

// === Core-present pending execution (delegates to runtime.PendingCoordinator) ===

func (rm *RecoveryManager) ExecutePendingCatchUp(volumeID string, targetLSN uint64) error {
	pe := rm.coord.TakeCatchUp(volumeID, targetLSN)
	if pe == nil || pe.Driver == nil || pe.Plan == nil {
		return nil
	}
	return rt.ExecuteCatchUpPlan(pe.Driver, pe.Plan, pe.CatchUpIO, volumeID, rm)
}

func (rm *RecoveryManager) ExecutePendingRebuild(volumeID string, targetLSN uint64) error {
	pe := rm.coord.TakeRebuild(volumeID, targetLSN)
	if pe == nil || pe.Driver == nil || pe.Plan == nil {
		return nil
	}
	return rt.ExecuteRebuildPlan(pe.Driver, pe.Plan, pe.RebuildIO, volumeID, rm)
}

// RecoveryCallbacks implementation — host-side completion notifications.

func (rm *RecoveryManager) OnCatchUpCompleted(volumeID string, achievedLSN uint64) {
	glog.V(0).Infof("recovery: catch-up completed for %s (achievedLSN=%d)", volumeID, achievedLSN)
	if rm.bs != nil && rm.bs.v2Core != nil {
		rm.bs.applyCoreEvent(engine.CatchUpCompleted{ID: volumeID, AchievedLSN: achievedLSN})
	}
}

func (rm *RecoveryManager) OnRebuildCompleted(volumeID string, plan *engine.RecoveryPlan) {
	glog.V(0).Infof("recovery: rebuild completed for %s", volumeID)
	if rm.bs == nil || rm.bs.v2Core == nil {
		return
	}
	status := rm.readRebuildStatus(volumeID)
	ev := rt.DeriveRebuildCommitted(volumeID, status, plan)
	rm.bs.applyCoreEvent(ev)
}

// readRebuildStatus reads post-rebuild snapshot from the backend.
// This is the thin host binding — it only fetches raw values.
func (rm *RecoveryManager) readRebuildStatus(volumeID string) rt.RebuildCompletionStatus {
	var status rt.RebuildCompletionStatus
	if err := rm.bs.blockStore.WithVolume(volumeID, func(vol *blockvol.BlockVol) error {
		snap := vol.StatusSnapshot()
		status.CommittedLSN = snap.CommittedLSN
		status.CheckpointLSN = snap.CheckpointLSN
		return nil
	}); err != nil {
		glog.Warningf("recovery: cannot read status snapshot for %s after rebuild: %v", volumeID, err)
	}
	return status
}

// === LEGACY NO-CORE COMPATIBILITY ===
//
// These methods execute recovery plans directly without going through the
// core command path. They exist only for no-core compatibility and older tests.
// Core-present paths use ExecutePendingCatchUp/ExecutePendingRebuild instead.

func (rm *RecoveryManager) executeLegacyCatchUp(ctx context.Context, volumeID, replicaID string, driver *engine.RecoveryDriver, plan *engine.RecoveryPlan, io engine.CatchUpIO) {
	err := rt.ExecuteCatchUpPlan(driver, plan, io, volumeID, rm)
	if err != nil {
		if ctx.Err() != nil {
			glog.V(1).Infof("recovery: catch-up cancelled for %s: %v", replicaID, err)
		} else {
			glog.Warningf("recovery: catch-up execution failed for %s: %v", replicaID, err)
		}
	}
}

func (rm *RecoveryManager) executeLegacyRebuild(ctx context.Context, volumeID, replicaID string, driver *engine.RecoveryDriver, plan *engine.RecoveryPlan, io engine.RebuildIO) {
	err := rt.ExecuteRebuildPlan(driver, plan, io, volumeID, rm)
	if err != nil {
		if ctx.Err() != nil {
			glog.V(1).Infof("recovery: rebuild cancelled for %s: %v", replicaID, err)
		} else {
			glog.Warningf("recovery: rebuild execution failed for %s: %v", replicaID, err)
		}
	}
}

func (rm *RecoveryManager) deriveRebuildAddr(replicaID string, assignments []blockvol.BlockVolumeAssignment) string {
	volPath := rm.volumePathForReplica(replicaID)
	for _, a := range assignments {
		if a.Path == volPath && a.RebuildAddr != "" {
			return a.RebuildAddr
		}
	}
	return ""
}

func (rm *RecoveryManager) volumePathForReplica(replicaID string) string {
	for i := len(replicaID) - 1; i >= 0; i-- {
		if replicaID[i] == '/' {
			return replicaID[:i]
		}
	}
	return ""
}

// --- Bridge shims ---

