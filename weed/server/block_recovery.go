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

	mu    sync.Mutex
	tasks map[string]*recoveryTask
	wg    sync.WaitGroup

	// TestHook: if set, called before execution starts. Tests use this
	// to hold the goroutine alive for serialized-replacement proofs.
	OnBeforeExecute func(replicaID string)
}

func NewRecoveryManager(bs *BlockService) *RecoveryManager {
	return &RecoveryManager{
		bs:    bs,
		tasks: make(map[string]*recoveryTask),
	}
}

// HandleAssignmentResult processes the engine's assignment result.
//
// Engine result semantics:
//   - SessionsCreated: new session, start goroutine
//   - SessionsSuperseded: old replaced by new — cancel+drain old, start new
//   - Removed: sender gone — cancel+drain, invalidate session
func (rm *RecoveryManager) HandleAssignmentResult(result engine.AssignmentResult, assignments []blockvol.BlockVolumeAssignment) {
	// Removed: cancel + invalidate + drain.
	for _, replicaID := range result.Removed {
		rm.cancelAndDrain(replicaID, true)
	}

	// Superseded: cancel + drain (no invalidate — engine has replacement session),
	// then start new.
	for _, replicaID := range result.SessionsSuperseded {
		rm.cancelAndDrain(replicaID, false)
		rm.startTask(replicaID, assignments)
	}

	// Created: start new (cancel stale defensively).
	for _, replicaID := range result.SessionsCreated {
		rm.cancelAndDrain(replicaID, false)
		rm.startTask(replicaID, assignments)
	}
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
			&readerShimForRecovery{reader},
			&pinnerShimForRecovery{pinner},
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

	if ctx.Err() != nil {
		driver.CancelPlan(plan, "context_cancelled")
		return
	}

	exec := engine.NewCatchUpExecutor(driver, plan)
	exec.IO = executor

	if execErr := exec.Execute(nil, 0); execErr != nil {
		if ctx.Err() != nil {
			glog.V(1).Infof("recovery: catch-up cancelled for %s: %v", replicaID, execErr)
		} else {
			glog.Warningf("recovery: catch-up execution failed for %s: %v", replicaID, execErr)
		}
		return
	}

	glog.V(0).Infof("recovery: catch-up completed for %s", replicaID)
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
			&readerShimForRecovery{reader},
			&pinnerShimForRecovery{pinner},
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

	if ctx.Err() != nil {
		driver.CancelPlan(plan, "context_cancelled")
		return
	}

	exec := engine.NewRebuildExecutor(driver, plan)
	exec.IO = executor

	if execErr := exec.Execute(); execErr != nil {
		if ctx.Err() != nil {
			glog.V(1).Infof("recovery: rebuild cancelled for %s: %v", replicaID, execErr)
		} else {
			glog.Warningf("recovery: rebuild execution failed for %s: %v", replicaID, execErr)
		}
		return
	}

	glog.V(0).Infof("recovery: rebuild completed for %s", replicaID)
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

type readerShimForRecovery struct{ r *v2bridge.Reader }

func (s *readerShimForRecovery) ReadState() bridge.BlockVolState {
	rs := s.r.ReadState()
	return bridge.BlockVolState{
		WALHeadLSN:        rs.WALHeadLSN,
		WALTailLSN:        rs.WALTailLSN,
		CommittedLSN:      rs.CommittedLSN,
		CheckpointLSN:     rs.CheckpointLSN,
		CheckpointTrusted: rs.CheckpointTrusted,
	}
}

type pinnerShimForRecovery struct{ p *v2bridge.Pinner }

func (s *pinnerShimForRecovery) HoldWALRetention(startLSN uint64) (func(), error) {
	return s.p.HoldWALRetention(startLSN)
}
func (s *pinnerShimForRecovery) HoldSnapshot(checkpointLSN uint64) (func(), error) {
	return s.p.HoldSnapshot(checkpointLSN)
}
func (s *pinnerShimForRecovery) HoldFullBase(committedLSN uint64) (func(), error) {
	return s.p.HoldFullBase(committedLSN)
}
