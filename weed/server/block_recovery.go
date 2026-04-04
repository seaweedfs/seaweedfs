package weed_server

import (
	"context"
	"sync"

	bridge "github.com/seaweedfs/seaweedfs/sw-block/bridge/blockvol"
	engine "github.com/seaweedfs/seaweedfs/sw-block/engine/replication"
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

	mu      sync.Mutex
	tasks   map[string]*recoveryTask
	pending map[string]*pendingRecoveryExecution
	wg      sync.WaitGroup

	// TestHook: if set, called before execution starts. Tests use this
	// to hold the goroutine alive for serialized-replacement proofs.
	OnBeforeExecute func(replicaID string)

	// TestHook: if set, may adjust a freshly cached pending execution before
	// the core event is emitted. Used only by focused ownership tests.
	OnPendingExecution func(volumeID string, pending *pendingRecoveryExecution)
}

type pendingRecoveryExecution struct {
	volumeID  string
	replicaID string
	driver    *engine.RecoveryDriver
	plan      *engine.RecoveryPlan
	catchUpIO engine.CatchUpIO
	rebuildIO engine.RebuildIO
}

func NewRecoveryManager(bs *BlockService) *RecoveryManager {
	return &RecoveryManager{
		bs:      bs,
		tasks:   make(map[string]*recoveryTask),
		pending: make(map[string]*pendingRecoveryExecution),
	}
}

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

	rebuildAddr := rm.deriveRebuildAddr(replicaID, assignments)

	ctx, cancel := context.WithCancel(context.Background())
	task := &recoveryTask{
		replicaID: replicaID,
		cancel:    cancel,
		done:      make(chan struct{}),
	}
	rm.tasks[replicaID] = task

	rm.wg.Add(1)
	go rm.runRecovery(ctx, task, rebuildAddr)
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
	for volumeID, pending := range rm.pending {
		if pending != nil && pending.driver != nil && pending.plan != nil {
			pending.driver.CancelPlan(pending.plan, "recovery_shutdown")
		}
		delete(rm.pending, volumeID)
	}
	rm.mu.Unlock()
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
func (rm *RecoveryManager) runRecovery(ctx context.Context, task *recoveryTask, rebuildAddr string) {
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

	glog.V(0).Infof("recovery: starting %s session for %s (rebuildAddr=%s)",
		sessSnap.Kind, replicaID, rebuildAddr)

	if rm.OnBeforeExecute != nil {
		rm.OnBeforeExecute(replicaID)
	}

	switch sessSnap.Kind {
	case engine.SessionCatchUp:
		rm.runCatchUp(ctx, replicaID, rebuildAddr)
	case engine.SessionRebuild:
		rm.runRebuild(ctx, replicaID, rebuildAddr)
	default:
		glog.V(1).Infof("recovery: unknown session kind %s for %s", sessSnap.Kind, replicaID)
	}
}

func (rm *RecoveryManager) runCatchUp(ctx context.Context, replicaID, rebuildAddr string) {
	bs := rm.bs
	volPath := rm.volumePathForReplica(replicaID)
	if volPath == "" {
		glog.Warningf("recovery: cannot determine volume path for %s", replicaID)
		return
	}

	var sa engine.StorageAdapter
	var replicaFlushedLSN uint64
	var executor *v2bridge.Executor

	if err := bs.blockStore.WithVolume(volPath, func(vol *blockvol.BlockVol) error {
		reader := v2bridge.NewReader(vol)
		pinner := v2bridge.NewPinner(vol)
		sa = bridge.NewStorageAdapter(
			reader,
			pinner,
		)
		if s := bs.v2Orchestrator.Registry.Sender(replicaID); s != nil {
			if snap := s.SessionSnapshot(); snap != nil {
				replicaFlushedLSN = snap.StartLSN
			}
		}
		executor = v2bridge.NewExecutor(vol, rebuildAddr)
		return nil
	}); err != nil {
		glog.Warningf("recovery: cannot access volume %s: %v", volPath, err)
		return
	}

	if ctx.Err() != nil {
		return
	}

	driver := &engine.RecoveryDriver{Orchestrator: bs.v2Orchestrator, Storage: sa}

	plan, err := driver.PlanRecovery(replicaID, replicaFlushedLSN)
	if err != nil {
		glog.Warningf("recovery: plan failed for %s: %v", replicaID, err)
		return
	}
	switch plan.Outcome {
	case engine.OutcomeCatchUp:
		if bs.v2Core == nil {
			if err := rm.executeCatchUpPlan(volPath, replicaID, driver, plan, executor); err != nil {
				if ctx.Err() != nil {
					glog.V(1).Infof("recovery: catch-up cancelled for %s: %v", replicaID, err)
				} else {
					glog.Warningf("recovery: catch-up execution failed for %s: %v", replicaID, err)
				}
			}
			return
		}
		rm.storePendingExecution(volPath, &pendingRecoveryExecution{
			volumeID:  volPath,
			replicaID: replicaID,
			driver:    driver,
			plan:      plan,
			catchUpIO: executor,
		})
		bs.applyCoreEvent(engine.CatchUpPlanned{ID: volPath, TargetLSN: plan.CatchUpTarget})
		if rm.hasPendingExecution(volPath) {
			rm.cancelPendingExecution(volPath, "start_catchup_not_emitted")
			return
		}
	case engine.OutcomeNeedsRebuild:
		reason := "needs_rebuild"
		if plan.Proof != nil && plan.Proof.Reason != "" {
			reason = plan.Proof.Reason
		}
		bs.applyCoreEvent(engine.NeedsRebuildObserved{ID: volPath, Reason: reason})
		return
	}

	if ctx.Err() != nil {
		driver.CancelPlan(plan, "context_cancelled")
		return
	}
}

func (rm *RecoveryManager) runRebuild(ctx context.Context, replicaID, rebuildAddr string) {
	bs := rm.bs
	volPath := rm.volumePathForReplica(replicaID)
	if volPath == "" {
		glog.Warningf("recovery: cannot determine volume path for %s", replicaID)
		return
	}

	var sa engine.StorageAdapter
	var executor *v2bridge.Executor

	if err := bs.blockStore.WithVolume(volPath, func(vol *blockvol.BlockVol) error {
		reader := v2bridge.NewReader(vol)
		pinner := v2bridge.NewPinner(vol)
		sa = bridge.NewStorageAdapter(
			reader,
			pinner,
		)
		executor = v2bridge.NewExecutor(vol, rebuildAddr)
		return nil
	}); err != nil {
		glog.Warningf("recovery: cannot access volume %s: %v", volPath, err)
		return
	}

	if ctx.Err() != nil {
		return
	}

	driver := &engine.RecoveryDriver{Orchestrator: bs.v2Orchestrator, Storage: sa}

	plan, err := driver.PlanRebuild(replicaID)
	if err != nil {
		glog.Warningf("recovery: rebuild plan failed for %s: %v", replicaID, err)
		return
	}
	if bs.v2Core == nil {
		if err := rm.executeRebuildPlan(volPath, replicaID, driver, plan, executor); err != nil {
			if ctx.Err() != nil {
				glog.V(1).Infof("recovery: rebuild cancelled for %s: %v", replicaID, err)
			} else {
				glog.Warningf("recovery: rebuild execution failed for %s: %v", replicaID, err)
			}
		}
		return
	}
	rm.storePendingExecution(volPath, &pendingRecoveryExecution{
		volumeID:  volPath,
		replicaID: replicaID,
		driver:    driver,
		plan:      plan,
		rebuildIO: executor,
	})
	if rm.OnPendingExecution != nil {
		if pending, ok := rm.peekPendingExecution(volPath); ok {
			rm.OnPendingExecution(volPath, pending)
		}
	}
	bs.applyCoreEvent(engine.RebuildStarted{ID: volPath, TargetLSN: plan.RebuildTargetLSN})
	if rm.hasPendingExecution(volPath) {
		rm.cancelPendingExecution(volPath, "start_rebuild_not_emitted")
	}
}

func (rm *RecoveryManager) storePendingExecution(volumeID string, pending *pendingRecoveryExecution) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	if rm.pending == nil {
		rm.pending = make(map[string]*pendingRecoveryExecution)
	}
	rm.pending[volumeID] = pending
}

func (rm *RecoveryManager) takePendingExecution(volumeID string) (*pendingRecoveryExecution, bool) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	pending, ok := rm.pending[volumeID]
	if ok {
		delete(rm.pending, volumeID)
	}
	return pending, ok
}

func (rm *RecoveryManager) peekPendingExecution(volumeID string) (*pendingRecoveryExecution, bool) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	pending, ok := rm.pending[volumeID]
	return pending, ok
}

func (rm *RecoveryManager) hasPendingExecution(volumeID string) bool {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	_, ok := rm.pending[volumeID]
	return ok
}

func (rm *RecoveryManager) cancelPendingExecution(volumeID, reason string) {
	pending, ok := rm.takePendingExecution(volumeID)
	if !ok || pending == nil || pending.driver == nil || pending.plan == nil {
		return
	}
	pending.driver.CancelPlan(pending.plan, reason)
}

func (rm *RecoveryManager) ExecutePendingCatchUp(volumeID string, targetLSN uint64) error {
	pending, ok := rm.takePendingExecution(volumeID)
	if !ok || pending == nil || pending.plan == nil || pending.driver == nil {
		return nil
	}
	if pending.plan.CatchUpTarget != targetLSN {
		pending.driver.CancelPlan(pending.plan, "start_catchup_target_mismatch")
		return nil
	}
	return rm.executeCatchUpPlan(volumeID, pending.replicaID, pending.driver, pending.plan, pending.catchUpIO)
}

func (rm *RecoveryManager) ExecutePendingRebuild(volumeID string, targetLSN uint64) error {
	pending, ok := rm.takePendingExecution(volumeID)
	if !ok || pending == nil || pending.plan == nil || pending.driver == nil {
		return nil
	}
	if pending.plan.RebuildTargetLSN != targetLSN {
		pending.driver.CancelPlan(pending.plan, "start_rebuild_target_mismatch")
		return nil
	}
	return rm.executeRebuildPlan(volumeID, pending.replicaID, pending.driver, pending.plan, pending.rebuildIO)
}

func (rm *RecoveryManager) executeCatchUpPlan(volumeID, replicaID string, driver *engine.RecoveryDriver, plan *engine.RecoveryPlan, io engine.CatchUpIO) error {
	exec := engine.NewCatchUpExecutor(driver, plan)
	exec.IO = io
	if err := exec.Execute(nil, 0); err != nil {
		return err
	}
	glog.V(0).Infof("recovery: catch-up completed for %s", replicaID)
	if rm.bs != nil && rm.bs.v2Core != nil {
		achievedLSN := plan.CatchUpTarget
		if achievedLSN == 0 {
			achievedLSN = plan.CatchUpStartLSN
		}
		rm.bs.applyCoreEvent(engine.CatchUpCompleted{ID: volumeID, AchievedLSN: achievedLSN})
	}
	return nil
}

func (rm *RecoveryManager) executeRebuildPlan(volumeID, replicaID string, driver *engine.RecoveryDriver, plan *engine.RecoveryPlan, io engine.RebuildIO) error {
	exec := engine.NewRebuildExecutor(driver, plan)
	exec.IO = io
	if err := exec.Execute(); err != nil {
		return err
	}
	glog.V(0).Infof("recovery: rebuild completed for %s", replicaID)
	if rm.bs == nil || rm.bs.v2Core == nil {
		return nil
	}
	var snap blockvol.V2StatusSnapshot
	if err := rm.bs.blockStore.WithVolume(volumeID, func(vol *blockvol.BlockVol) error {
		snap = vol.StatusSnapshot()
		return nil
	}); err != nil {
		glog.Warningf("recovery: cannot read status snapshot for %s after rebuild: %v", volumeID, err)
	}
	flushedLSN := snap.CommittedLSN
	if flushedLSN == 0 {
		flushedLSN = plan.RebuildTargetLSN
	}
	checkpointLSN := snap.CheckpointLSN
	if checkpointLSN == 0 {
		checkpointLSN = plan.RebuildTargetLSN
	}
	achievedLSN := flushedLSN
	if checkpointLSN > achievedLSN {
		achievedLSN = checkpointLSN
	}
	rm.bs.applyCoreEvent(engine.RebuildCommitted{
		ID:            volumeID,
		AchievedLSN:   achievedLSN,
		FlushedLSN:    flushedLSN,
		CheckpointLSN: checkpointLSN,
	})
	return nil
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

