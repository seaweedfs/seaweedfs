package weed_server

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

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

type recoverySyncFact struct {
	Source         recoverySyncFactSource
	Kind           recoverySyncFactKind
	VolumeID       string
	ReplicaID      string
	AckKind        engine.SyncAckKind
	TargetLSN      uint64
	PrimaryTailLSN uint64
	DurableLSN     uint64
	AppliedLSN     uint64
	Reason         string
}

type recoverySyncFactSource string

const (
	recoverySyncFactSourcePlan     recoverySyncFactSource = "plan"
	recoverySyncFactSourceCallback recoverySyncFactSource = "callback"
)

type recoverySyncFactKind string

const (
	recoverySyncFactKindSyncReplayRequired  recoverySyncFactKind = "sync_replay_required"
	recoverySyncFactKindSyncRebuildRequired recoverySyncFactKind = "sync_rebuild_required"
	recoverySyncFactKindSyncReplayFailed    recoverySyncFactKind = "sync_replay_failed"
	recoverySyncFactKindSyncQuorumAcked     recoverySyncFactKind = "sync_quorum_acked"
	recoverySyncFactKindSyncQuorumTimedOut  recoverySyncFactKind = "sync_quorum_timed_out"
)

func newPlanCatchUpSyncFact(volumeID, replicaID string, plan *engine.RecoveryPlan, replicaDurableLSN uint64) recoverySyncFact {
	return recoverySyncFact{
		Source:         recoverySyncFactSourcePlan,
		Kind:           recoverySyncFactKindSyncReplayRequired,
		VolumeID:       volumeID,
		ReplicaID:      replicaID,
		AckKind:        engine.SyncAckTimedOut,
		TargetLSN:      plan.CatchUpTarget,
		PrimaryTailLSN: plan.Proof.TailLSN,
		DurableLSN:     replicaDurableLSN,
		AppliedLSN:     plan.Proof.ReplicaFlushedLSN,
		Reason:         plan.Proof.Reason,
	}
}

func newPlanNeedsRebuildSyncFact(volumeID, replicaID string, proof *engine.RecoverabilityProof) recoverySyncFact {
	reason := "needs_rebuild"
	if proof != nil && proof.Reason != "" {
		reason = proof.Reason
	}
	return recoverySyncFact{
		Source:         recoverySyncFactSourcePlan,
		Kind:           recoverySyncFactKindSyncRebuildRequired,
		VolumeID:       volumeID,
		ReplicaID:      replicaID,
		AckKind:        engine.SyncAckTimedOut,
		TargetLSN:      proof.CommittedLSN,
		PrimaryTailLSN: proof.TailLSN,
		DurableLSN:     proof.ReplicaFlushedLSN,
		AppliedLSN:     proof.ReplicaFlushedLSN,
		Reason:         reason,
	}
}

func newCatchUpFailureSyncFact(volumeID, replicaID, reason string) recoverySyncFact {
	return recoverySyncFact{
		Source:    recoverySyncFactSourceCallback,
		Kind:      recoverySyncFactKindSyncReplayFailed,
		VolumeID:  volumeID,
		ReplicaID: replicaID,
		AckKind:   engine.SyncAckTransportLost,
		Reason:    reason,
	}
}

func newBarrierAcceptedSyncFact(volumeID string, flushedLSN uint64) recoverySyncFact {
	return recoverySyncFact{
		Source:     recoverySyncFactSourceCallback,
		Kind:       recoverySyncFactKindSyncQuorumAcked,
		VolumeID:   volumeID,
		AckKind:    engine.SyncAckQuorum,
		TargetLSN:  flushedLSN,
		DurableLSN: flushedLSN,
	}
}

func newBarrierRejectedSyncFact(volumeID, replicaID, reason string) recoverySyncFact {
	return recoverySyncFact{
		Source:    recoverySyncFactSourceCallback,
		Kind:      recoverySyncFactKindSyncQuorumTimedOut,
		VolumeID:  volumeID,
		ReplicaID: replicaID,
		AckKind:   engine.SyncAckTimedOut,
		Reason:    reason,
	}
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
		rm.cancelAndDrainWithReason(replicaID, true, "recovery_removed")
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
		rm.cancelAndDrainWithReason(replicaID, true, "recovery_removed")
	}
}

// StartRecoveryTask starts one bounded recovery goroutine from a core-emitted
// command. Any stale task for the same replica is drained first.
func (rm *RecoveryManager) StartRecoveryTask(replicaID string, assignments []blockvol.BlockVolumeAssignment) {
	rm.cancelAndDrain(replicaID, false)
	rm.startTask(replicaID, assignments)
}

// DrainRecoveryTask drains removed recovery work from an explicit core-owned
// command seam on the core-present path.
func (rm *RecoveryManager) DrainRecoveryTask(replicaID, reason string) {
	if reason == "" {
		reason = "recovery_removed"
	}
	rm.cancelAndDrainWithReason(replicaID, true, reason)
}

// cancelAndDrain cancels a running task and WAITS for it to exit.
// This ensures no overlap between old and new owners.
func (rm *RecoveryManager) cancelAndDrain(replicaID string, invalidateSession bool) {
	reason := ""
	if invalidateSession {
		reason = "recovery_removed"
	}
	rm.cancelAndDrainWithReason(replicaID, invalidateSession, reason)
}

func (rm *RecoveryManager) cancelAndDrainWithReason(replicaID string, invalidateSession bool, reason string) {
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
			s.InvalidateSession(reason, engine.StateDisconnected)
		}
	}
	delete(rm.tasks, replicaID)
	doneCh := task.done
	rm.mu.Unlock()

	// Wait for the old goroutine to exit OUTSIDE the lock, with a bounded
	// timeout. If the goroutine is stuck on a blocking dial/read that
	// doesn't respect context cancellation, we abandon it after 5s and
	// proceed. This prevents a stuck catch-up from blocking a rebuild.
	select {
	case <-doneCh:
		// Clean exit.
	case <-time.After(5 * time.Second):
		glog.Warningf("recovery: drain timeout for %s — abandoning stuck goroutine", replicaID)
	}
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
		bundle = v2bridge.BuildRecoveryBundle(vol, rebuildAddr, replicaID)
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
	if rm.applyRecoveryPlanFromFacts(ctx, rctx, replicaID, assignments, plan) {
		return
	}

	if ctx.Err() != nil {
		rctx.driver.CancelPlan(plan, "context_cancelled")
		return
	}
}

func (rm *RecoveryManager) applyRecoveryPlanFromFacts(ctx context.Context, rctx *recoveryContext, replicaID string, assignments []blockvol.BlockVolumeAssignment, plan *engine.RecoveryPlan) bool {
	if rm == nil || rm.bs == nil || rctx == nil || plan == nil {
		return false
	}
	bs := rm.bs

	switch plan.Outcome {
	case engine.OutcomeCatchUp:
		if plan.Proof == nil {
			glog.Warningf("recovery: missing recoverability proof for catch-up plan %s", replicaID)
			return true
		}
		if bs.v2Core == nil {
			rm.executeLegacyCatchUp(ctx, rctx.volPath, replicaID, rctx.driver, plan, rctx.executor)
			return true
		}
		rm.coord.Store(replicaID, &rt.PendingExecution{
			VolumeID:      rctx.volPath,
			ReplicaID:     replicaID,
			CatchUpTarget: plan.CatchUpTarget,
			Driver:        rctx.driver,
			Plan:          plan,
			CatchUpIO:     rctx.executor,
		})
		if rm.OnPendingExecution != nil {
			rm.OnPendingExecution(rctx.volPath, rm.coord.Peek(replicaID))
		}
		bs.applyRecoverySyncFact(newPlanCatchUpSyncFact(rctx.volPath, replicaID, plan, rctx.replicaFlushedLSN))
		bs.applyCoreEvent(engine.CatchUpPlanned{ID: rctx.volPath, ReplicaID: replicaID, TargetLSN: plan.CatchUpTarget})
		if rm.coord.Has(replicaID) {
			rm.coord.Cancel(replicaID, "start_catchup_not_emitted")
		}
		return true
	case engine.OutcomeNeedsRebuild:
		if plan.Proof == nil {
			glog.Warningf("recovery: missing recoverability proof for rebuild plan %s", replicaID)
			return true
		}
		bs.applyRecoverySyncFact(newPlanNeedsRebuildSyncFact(rctx.volPath, replicaID, plan.Proof))
		if ctx.Err() != nil {
			return true
		}
		if err := rm.installSession(replicaID, engine.SessionRebuild); err != nil {
			glog.Warningf("recovery: install rebuild session failed for %s: %v", replicaID, err)
			return true
		}
		rm.runRebuild(ctx, replicaID, assignments)
		return true
	default:
		return false
	}
}

func (rm *RecoveryManager) installSession(replicaID string, kind engine.SessionKind) error {
	if rm == nil || rm.bs == nil || rm.bs.v2Orchestrator == nil {
		return fmt.Errorf("recovery: orchestrator unavailable")
	}
	sender := rm.bs.v2Orchestrator.Registry.Sender(replicaID)
	if sender == nil {
		return fmt.Errorf("recovery: sender %s not found", replicaID)
	}
	if snap := sender.SessionSnapshot(); snap != nil && snap.Active && snap.Kind == kind {
		return nil
	}
	all := rm.bs.v2Orchestrator.Registry.All()
	replicas := make([]engine.ReplicaAssignment, 0, len(all))
	for _, s := range all {
		replicas = append(replicas, engine.ReplicaAssignment{
			ReplicaID: s.ReplicaID(),
			Endpoint:  s.Endpoint(),
		})
	}
	result := rm.bs.v2Orchestrator.ProcessAssignment(engine.AssignmentIntent{
		Replicas: replicas,
		Epoch:    sender.Epoch(),
		RecoveryTargets: map[string]engine.SessionKind{
			replicaID: kind,
		},
	})
	for _, id := range append(append([]string(nil), result.SessionsCreated...), result.SessionsSuperseded...) {
		if id == replicaID {
			return nil
		}
	}
	snap := sender.SessionSnapshot()
	if snap != nil && snap.Active && snap.Kind == kind {
		return nil
	}
	return fmt.Errorf("recovery: %s session not installed for %s", kind, replicaID)
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
	rm.coord.Store(replicaID, pe)
	if rm.OnPendingExecution != nil {
		rm.OnPendingExecution(rctx.volPath, pe)
	}
	bs.applyCoreEvent(engine.RebuildStarted{ID: rctx.volPath, ReplicaID: replicaID, TargetLSN: plan.RebuildTargetLSN})
	if rm.coord.Has(replicaID) {
		rm.coord.Cancel(replicaID, "start_rebuild_not_emitted")
	}
}

// === Core-present pending execution (delegates to runtime.PendingCoordinator) ===

func (rm *RecoveryManager) ExecutePendingCatchUp(replicaID string, targetLSN uint64) error {
	pe := rm.coord.TakeCatchUp(replicaID, targetLSN)
	if pe == nil || pe.Driver == nil || pe.Plan == nil {
		return nil
	}
	return rt.ExecuteCatchUpPlan(pe.Driver, pe.Plan, pe.CatchUpIO, pe.VolumeID, pe.ReplicaID, rm)
}

func (rm *RecoveryManager) ExecutePendingRebuild(replicaID string, targetLSN uint64) error {
	pe := rm.coord.TakeRebuild(replicaID, targetLSN)
	if pe == nil || pe.Driver == nil || pe.Plan == nil {
		return nil
	}
	return rt.ExecuteRebuildPlan(pe.Driver, pe.Plan, pe.RebuildIO, pe.VolumeID, pe.ReplicaID, rm)
}

// RecoveryCallbacks implementation — host-side completion notifications.

func (rm *RecoveryManager) OnRecoveryProgress(volumeID, replicaID string, achievedLSN uint64) {
	if rm.bs == nil || rm.bs.v2Core == nil {
		return
	}
	rm.bs.applyCoreEvent(engine.RecoveryProgressObserved{
		ID:          volumeID,
		ReplicaID:   replicaID,
		AchievedLSN: achievedLSN,
	})
}

func (rm *RecoveryManager) OnCatchUpCompleted(volumeID, replicaID string, achievedLSN uint64) {
	glog.V(0).Infof("recovery: catch-up completed for %s via %s (achievedLSN=%d)", volumeID, replicaID, achievedLSN)
	if rm.bs != nil && rm.bs.v2Core != nil {
		rm.bs.applyCoreEvent(engine.CatchUpCompleted{ID: volumeID, ReplicaID: replicaID, AchievedLSN: achievedLSN})
	}
}

func (rm *RecoveryManager) OnCatchUpFailed(volumeID, replicaID, reason string) {
	if rm.bs == nil || rm.bs.v2Core == nil || reason == "" {
		return
	}
	glog.V(0).Infof("recovery: catch-up failed for %s via %s (%s)", volumeID, replicaID, reason)
	fact := newCatchUpFailureSyncFact(volumeID, replicaID, reason)
	rm.bs.applyRecoverySyncFact(fact)
	rm.reenterFromFact(fact)
}

func (rm *RecoveryManager) ReenterFromSyncTimeout(fact recoverySyncFact) {
	rm.reenterFromFact(fact)
}

func (rm *RecoveryManager) OnRebuildCompleted(volumeID, replicaID string, plan *engine.RecoveryPlan) {
	glog.V(0).Infof("recovery: rebuild completed for %s via %s", volumeID, replicaID)
	if rm.bs == nil || rm.bs.v2Core == nil {
		return
	}
	status := rm.readRebuildStatus(volumeID)
	ev := rt.DeriveRebuildCommitted(volumeID, replicaID, status, plan)
	rm.bs.applyCoreEvent(ev)
}

func (bs *BlockService) applyRecoverySyncFact(fact recoverySyncFact) {
	if bs == nil || bs.v2Core == nil || fact.VolumeID == "" || fact.AckKind == "" {
		return
	}
	bs.applyCoreEvent(engine.SyncAckObserved{
		ID:             fact.VolumeID,
		ReplicaID:      fact.ReplicaID,
		AckKind:        fact.AckKind,
		TargetLSN:      fact.TargetLSN,
		PrimaryTailLSN: fact.PrimaryTailLSN,
		DurableLSN:     fact.DurableLSN,
		AppliedLSN:     fact.AppliedLSN,
		Reason:         fact.Reason,
	})
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
	err := rt.ExecuteCatchUpPlan(driver, plan, io, volumeID, replicaID, rm)
	if err != nil {
		if ctx.Err() != nil {
			glog.V(1).Infof("recovery: catch-up cancelled for %s: %v", replicaID, err)
		} else {
			glog.Warningf("recovery: catch-up execution failed for %s: %v", replicaID, err)
		}
	}
}

func (rm *RecoveryManager) executeLegacyRebuild(ctx context.Context, volumeID, replicaID string, driver *engine.RecoveryDriver, plan *engine.RecoveryPlan, io engine.RebuildIO) {
	err := rt.ExecuteRebuildPlan(driver, plan, io, volumeID, replicaID, rm)
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
	if rm == nil || rm.bs == nil || volPath == "" {
		return ""
	}
	host := rm.bs.advertisedHost
	if host == "" {
		if parsedHost, _, err := net.SplitHostPort(rm.bs.listenAddr); err == nil {
			host = parsedHost
		}
	}
	switch host {
	case "", "0.0.0.0", "::":
		host = "127.0.0.1"
	}
	_, _, rebuildPort := rm.bs.ReplicationPorts(volPath)
	if rebuildPort <= 0 {
		return ""
	}
	return net.JoinHostPort(host, strconv.Itoa(rebuildPort))
}

func shouldReenterRecoveryFromFailure(reason string) bool {
	switch reason {
	case "recoverability_lost", "retention_lost", "truncation_unsafe":
		return true
	default:
		return false
	}
}

func (rm *RecoveryManager) reenterFromFact(fact recoverySyncFact) {
	if !shouldReenterRecoveryFromFact(fact) || rm == nil || rm.bs == nil || rm.bs.v2Core == nil || fact.ReplicaID == "" {
		return
	}
	sender := rm.bs.v2Orchestrator.Registry.Sender(fact.ReplicaID)
	if sender != nil {
		if snap := sender.SessionSnapshot(); snap != nil && snap.Active && snap.Kind == engine.SessionRebuild {
			return
		}
	}
	if err := rm.installSession(fact.ReplicaID, engine.SessionCatchUp); err != nil {
		glog.Warningf("recovery: re-enter catchup session install failed for %s (%s): %v", fact.ReplicaID, fact.Reason, err)
		return
	}
	rm.runCatchUp(context.Background(), fact.ReplicaID, nil)
}

func shouldReenterRecoveryFromFact(fact recoverySyncFact) bool {
	switch fact.Kind {
	case recoverySyncFactKindSyncQuorumTimedOut:
		return true
	case recoverySyncFactKindSyncReplayFailed:
		return shouldReenterRecoveryFromFailure(fact.Reason)
	default:
		return false
	}
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
