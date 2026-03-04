package plugin

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

func TestLoadSchedulerPolicyUsesAdminConfig(t *testing.T) {
	t.Parallel()

	pluginSvc, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pluginSvc.Shutdown()

	err = pluginSvc.SaveJobTypeConfig(&plugin_pb.PersistedJobTypeConfig{
		JobType: "vacuum",
		AdminRuntime: &plugin_pb.AdminRuntimeConfig{
			Enabled:                       true,
			DetectionIntervalSeconds:      30,
			DetectionTimeoutSeconds:       20,
			MaxJobsPerDetection:           123,
			GlobalExecutionConcurrency:    5,
			PerWorkerExecutionConcurrency: 2,
			RetryLimit:                    4,
			RetryBackoffSeconds:           7,
		},
	})
	if err != nil {
		t.Fatalf("SaveJobTypeConfig: %v", err)
	}

	policy, enabled, err := pluginSvc.loadSchedulerPolicy("vacuum")
	if err != nil {
		t.Fatalf("loadSchedulerPolicy: %v", err)
	}
	if !enabled {
		t.Fatalf("expected enabled policy")
	}
	if policy.MaxResults != 123 {
		t.Fatalf("unexpected max results: got=%d", policy.MaxResults)
	}
	if policy.ExecutionConcurrency != 5 {
		t.Fatalf("unexpected global concurrency: got=%d", policy.ExecutionConcurrency)
	}
	if policy.PerWorkerConcurrency != 2 {
		t.Fatalf("unexpected per-worker concurrency: got=%d", policy.PerWorkerConcurrency)
	}
	if policy.RetryLimit != 4 {
		t.Fatalf("unexpected retry limit: got=%d", policy.RetryLimit)
	}
	if policy.MaxJobTypeDuration != defaultMaxJobTypeDuration {
		t.Fatalf("unexpected max job type duration: got=%v", policy.MaxJobTypeDuration)
	}
}

func TestLoadSchedulerPolicyUsesDescriptorDefaultsWhenConfigMissing(t *testing.T) {
	t.Parallel()

	pluginSvc, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pluginSvc.Shutdown()

	err = pluginSvc.store.SaveDescriptor("ec", &plugin_pb.JobTypeDescriptor{
		JobType: "ec",
		AdminRuntimeDefaults: &plugin_pb.AdminRuntimeDefaults{
			Enabled:                       true,
			DetectionIntervalSeconds:      60,
			DetectionTimeoutSeconds:       25,
			MaxJobsPerDetection:           30,
			GlobalExecutionConcurrency:    4,
			PerWorkerExecutionConcurrency: 2,
			RetryLimit:                    3,
			RetryBackoffSeconds:           6,
		},
	})
	if err != nil {
		t.Fatalf("SaveDescriptor: %v", err)
	}

	policy, enabled, err := pluginSvc.loadSchedulerPolicy("ec")
	if err != nil {
		t.Fatalf("loadSchedulerPolicy: %v", err)
	}
	if !enabled {
		t.Fatalf("expected enabled policy from descriptor defaults")
	}
	if policy.MaxResults != 30 {
		t.Fatalf("unexpected max results: got=%d", policy.MaxResults)
	}
	if policy.ExecutionConcurrency != 4 {
		t.Fatalf("unexpected global concurrency: got=%d", policy.ExecutionConcurrency)
	}
	if policy.PerWorkerConcurrency != 2 {
		t.Fatalf("unexpected per-worker concurrency: got=%d", policy.PerWorkerConcurrency)
	}
}

func TestReserveScheduledExecutorRespectsPerWorkerLimit(t *testing.T) {
	t.Parallel()

	pluginSvc, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pluginSvc.Shutdown()

	pluginSvc.registry.UpsertFromHello(&plugin_pb.WorkerHello{
		WorkerId: "worker-a",
		Capabilities: []*plugin_pb.JobTypeCapability{
			{JobType: "balance", CanExecute: true, MaxExecutionConcurrency: 4},
		},
	})
	pluginSvc.registry.UpsertFromHello(&plugin_pb.WorkerHello{
		WorkerId: "worker-b",
		Capabilities: []*plugin_pb.JobTypeCapability{
			{JobType: "balance", CanExecute: true, MaxExecutionConcurrency: 2},
		},
	})

	policy := schedulerPolicy{
		PerWorkerConcurrency:   1,
		ExecutorReserveBackoff: time.Millisecond,
	}

	executor1, release1, err := pluginSvc.reserveScheduledExecutor(context.Background(), "balance", policy)
	if err != nil {
		t.Fatalf("reserve executor 1: %v", err)
	}
	defer release1()

	executor2, release2, err := pluginSvc.reserveScheduledExecutor(context.Background(), "balance", policy)
	if err != nil {
		t.Fatalf("reserve executor 2: %v", err)
	}
	defer release2()

	if executor1.WorkerID == executor2.WorkerID {
		t.Fatalf("expected different executors due per-worker limit, got same worker %s", executor1.WorkerID)
	}
}

func TestFilterScheduledProposalsDedupe(t *testing.T) {
	t.Parallel()

	pluginSvc, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pluginSvc.Shutdown()

	proposals := []*plugin_pb.JobProposal{
		{ProposalId: "p1", DedupeKey: "d1"},
		{ProposalId: "p2", DedupeKey: "d1"}, // same dedupe key
		{ProposalId: "p3", DedupeKey: "d3"},
		{ProposalId: "p3"}, // fallback dedupe by proposal id
		{ProposalId: "p4"},
		{ProposalId: "p4"}, // same proposal id, no dedupe key
	}

	filtered := pluginSvc.filterScheduledProposals(proposals)
	if len(filtered) != 4 {
		t.Fatalf("unexpected filtered size: got=%d want=4", len(filtered))
	}

	filtered2 := pluginSvc.filterScheduledProposals(proposals)
	if len(filtered2) != 4 {
		t.Fatalf("expected second run dedupe to be per-run only, got=%d", len(filtered2))
	}
}

func TestBuildScheduledJobSpecDoesNotReuseProposalID(t *testing.T) {
	t.Parallel()

	proposal := &plugin_pb.JobProposal{
		ProposalId: "vacuum-2",
		DedupeKey:  "vacuum:2",
		JobType:    "vacuum",
	}

	jobA := buildScheduledJobSpec("vacuum", proposal, 0)
	jobB := buildScheduledJobSpec("vacuum", proposal, 1)

	if jobA.JobId == proposal.ProposalId {
		t.Fatalf("scheduled job id must not reuse proposal id: %s", jobA.JobId)
	}
	if jobB.JobId == proposal.ProposalId {
		t.Fatalf("scheduled job id must not reuse proposal id: %s", jobB.JobId)
	}
	if jobA.JobId == jobB.JobId {
		t.Fatalf("scheduled job ids must be unique across jobs: %s", jobA.JobId)
	}
}

func TestFilterProposalsWithActiveJobs(t *testing.T) {
	t.Parallel()

	pluginSvc, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pluginSvc.Shutdown()

	pluginSvc.trackExecutionStart("req-1", "worker-a", &plugin_pb.JobSpec{
		JobId:     "job-1",
		JobType:   "vacuum",
		DedupeKey: "vacuum:k1",
	}, 1)
	pluginSvc.trackExecutionStart("req-2", "worker-b", &plugin_pb.JobSpec{
		JobId:   "job-2",
		JobType: "vacuum",
	}, 1)
	pluginSvc.trackExecutionQueued(&plugin_pb.JobSpec{
		JobId:     "job-3",
		JobType:   "vacuum",
		DedupeKey: "vacuum:k4",
	})

	filtered, skipped := pluginSvc.filterProposalsWithActiveJobs("vacuum", []*plugin_pb.JobProposal{
		{ProposalId: "proposal-1", JobType: "vacuum", DedupeKey: "vacuum:k1"},
		{ProposalId: "job-2", JobType: "vacuum"},
		{ProposalId: "proposal-2b", JobType: "vacuum", DedupeKey: "vacuum:k4"},
		{ProposalId: "proposal-3", JobType: "vacuum", DedupeKey: "vacuum:k3"},
		{ProposalId: "proposal-4", JobType: "balance", DedupeKey: "balance:k1"},
	})
	if skipped != 3 {
		t.Fatalf("unexpected skipped count: got=%d want=3", skipped)
	}
	if len(filtered) != 2 {
		t.Fatalf("unexpected filtered size: got=%d want=2", len(filtered))
	}
	if filtered[0].ProposalId != "proposal-3" || filtered[1].ProposalId != "proposal-4" {
		t.Fatalf("unexpected filtered proposals: got=%s,%s", filtered[0].ProposalId, filtered[1].ProposalId)
	}
}

func TestReserveScheduledExecutorTimesOutWhenNoExecutor(t *testing.T) {
	t.Parallel()

	pluginSvc, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pluginSvc.Shutdown()

	policy := schedulerPolicy{
		ExecutionTimeout:       30 * time.Millisecond,
		ExecutorReserveBackoff: 5 * time.Millisecond,
		PerWorkerConcurrency:   1,
	}

	start := time.Now()
	pluginSvc.Shutdown()
	_, _, err = pluginSvc.reserveScheduledExecutor(context.Background(), "missing-job-type", policy)
	if err == nil {
		t.Fatalf("expected reservation shutdown error")
	}
	if time.Since(start) > 50*time.Millisecond {
		t.Fatalf("reservation returned too late after shutdown: duration=%v", time.Since(start))
	}
}

func TestReserveScheduledExecutorWaitsForWorkerCapacity(t *testing.T) {
	t.Parallel()

	pluginSvc, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pluginSvc.Shutdown()

	pluginSvc.registry.UpsertFromHello(&plugin_pb.WorkerHello{
		WorkerId: "worker-a",
		Capabilities: []*plugin_pb.JobTypeCapability{
			{JobType: "balance", CanExecute: true, MaxExecutionConcurrency: 1},
		},
	})

	policy := schedulerPolicy{
		ExecutionTimeout:       time.Second,
		PerWorkerConcurrency:   8,
		ExecutorReserveBackoff: 5 * time.Millisecond,
	}

	_, release1, err := pluginSvc.reserveScheduledExecutor(context.Background(), "balance", policy)
	if err != nil {
		t.Fatalf("reserve executor 1: %v", err)
	}
	defer release1()

	type reserveResult struct {
		err error
	}
	secondReserveCh := make(chan reserveResult, 1)
	go func() {
		_, release2, reserveErr := pluginSvc.reserveScheduledExecutor(context.Background(), "balance", policy)
		if release2 != nil {
			release2()
		}
		secondReserveCh <- reserveResult{err: reserveErr}
	}()

	select {
	case result := <-secondReserveCh:
		t.Fatalf("expected second reservation to wait for capacity, got=%v", result.err)
	case <-time.After(25 * time.Millisecond):
		// Expected: still waiting.
	}

	release1()

	select {
	case result := <-secondReserveCh:
		if result.err != nil {
			t.Fatalf("second reservation error: %v", result.err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("second reservation did not acquire after capacity release")
	}
}

func TestShouldSkipDetectionForWaitingJobs(t *testing.T) {
	t.Parallel()

	pluginSvc, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pluginSvc.Shutdown()

	policy := schedulerPolicy{
		ExecutionConcurrency: 2,
		MaxResults:           100,
	}
	threshold := waitingBacklogThreshold(policy)
	if threshold <= 0 {
		t.Fatalf("expected positive waiting threshold")
	}

	for i := 0; i < threshold; i++ {
		pluginSvc.trackExecutionQueued(&plugin_pb.JobSpec{
			JobId:     fmt.Sprintf("job-waiting-%d", i),
			JobType:   "vacuum",
			DedupeKey: fmt.Sprintf("vacuum:%d", i),
		})
	}

	skip, waitingCount, waitingThreshold := pluginSvc.shouldSkipDetectionForWaitingJobs("vacuum", policy)
	if !skip {
		t.Fatalf("expected detection to skip when waiting backlog reaches threshold")
	}
	if waitingCount != threshold {
		t.Fatalf("unexpected waiting count: got=%d want=%d", waitingCount, threshold)
	}
	if waitingThreshold != threshold {
		t.Fatalf("unexpected waiting threshold: got=%d want=%d", waitingThreshold, threshold)
	}
}

func TestWaitingBacklogThresholdHonorsMaxResultsCap(t *testing.T) {
	t.Parallel()

	policy := schedulerPolicy{
		ExecutionConcurrency: 8,
		MaxResults:           6,
	}
	threshold := waitingBacklogThreshold(policy)
	if threshold != 6 {
		t.Fatalf("expected threshold to be capped by max results, got=%d", threshold)
	}
}

func TestListSchedulerStatesIncludesPolicyAndState(t *testing.T) {
	t.Parallel()

	pluginSvc, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pluginSvc.Shutdown()

	const jobType = "vacuum"
	err = pluginSvc.SaveJobTypeConfig(&plugin_pb.PersistedJobTypeConfig{
		JobType: jobType,
		AdminRuntime: &plugin_pb.AdminRuntimeConfig{
			Enabled:                       true,
			DetectionIntervalSeconds:      45,
			DetectionTimeoutSeconds:       30,
			MaxJobsPerDetection:           80,
			GlobalExecutionConcurrency:    3,
			PerWorkerExecutionConcurrency: 2,
			RetryLimit:                    1,
			RetryBackoffSeconds:           9,
		},
	})
	if err != nil {
		t.Fatalf("SaveJobTypeConfig: %v", err)
	}

	pluginSvc.registry.UpsertFromHello(&plugin_pb.WorkerHello{
		WorkerId: "worker-a",
		Capabilities: []*plugin_pb.JobTypeCapability{
			{JobType: jobType, CanDetect: true, CanExecute: true},
		},
	})

	// Mark this job type as currently processing to test DetectionInFlight.
	pluginSvc.schedulerMu.Lock()
	pluginSvc.currentJobType = jobType
	pluginSvc.schedulerMu.Unlock()

	states, err := pluginSvc.ListSchedulerStates()
	if err != nil {
		t.Fatalf("ListSchedulerStates: %v", err)
	}

	state := findSchedulerState(states, jobType)
	if state == nil {
		t.Fatalf("missing scheduler state for %s", jobType)
	}
	if !state.Enabled {
		t.Fatalf("expected enabled scheduler state")
	}
	if state.PolicyError != "" {
		t.Fatalf("unexpected policy error: %s", state.PolicyError)
	}
	if !state.DetectionInFlight {
		t.Fatalf("expected detection in flight when current job type matches")
	}
	if state.DetectionTimeoutSeconds != 30 {
		t.Fatalf("unexpected detection timeout: got=%d", state.DetectionTimeoutSeconds)
	}
	if state.ExecutionTimeoutSeconds != 90 {
		t.Fatalf("unexpected execution timeout: got=%d", state.ExecutionTimeoutSeconds)
	}
	if state.MaxJobTypeDurationSeconds != int32(defaultMaxJobTypeDuration/time.Second) {
		t.Fatalf("unexpected max job type duration: got=%d", state.MaxJobTypeDurationSeconds)
	}
	if state.MaxJobsPerDetection != 80 {
		t.Fatalf("unexpected max jobs per detection: got=%d", state.MaxJobsPerDetection)
	}
	if state.GlobalExecutionConcurrency != 3 {
		t.Fatalf("unexpected global execution concurrency: got=%d", state.GlobalExecutionConcurrency)
	}
	if state.PerWorkerExecutionConcurrency != 2 {
		t.Fatalf("unexpected per worker execution concurrency: got=%d", state.PerWorkerExecutionConcurrency)
	}
	if state.RetryLimit != 1 {
		t.Fatalf("unexpected retry limit: got=%d", state.RetryLimit)
	}
	if state.RetryBackoffSeconds != 9 {
		t.Fatalf("unexpected retry backoff: got=%d", state.RetryBackoffSeconds)
	}
	if !state.DetectorAvailable || state.DetectorWorkerID != "worker-a" {
		t.Fatalf("unexpected detector assignment: available=%v worker=%s", state.DetectorAvailable, state.DetectorWorkerID)
	}
	if state.ExecutorWorkerCount != 1 {
		t.Fatalf("unexpected executor worker count: got=%d", state.ExecutorWorkerCount)
	}

	// Clear the current job type and verify DetectionInFlight is false.
	pluginSvc.schedulerMu.Lock()
	pluginSvc.currentJobType = ""
	pluginSvc.schedulerMu.Unlock()

	states2, err := pluginSvc.ListSchedulerStates()
	if err != nil {
		t.Fatalf("ListSchedulerStates (2): %v", err)
	}
	state2 := findSchedulerState(states2, jobType)
	if state2 == nil {
		t.Fatalf("missing scheduler state for %s (2)", jobType)
	}
	if state2.DetectionInFlight {
		t.Fatalf("expected detection not in flight when current job type is empty")
	}
}

func TestListSchedulerStatesShowsDisabledWhenNoPolicy(t *testing.T) {
	t.Parallel()

	pluginSvc, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pluginSvc.Shutdown()

	const jobType = "balance"
	pluginSvc.registry.UpsertFromHello(&plugin_pb.WorkerHello{
		WorkerId: "worker-b",
		Capabilities: []*plugin_pb.JobTypeCapability{
			{JobType: jobType, CanDetect: true, CanExecute: true},
		},
	})

	states, err := pluginSvc.ListSchedulerStates()
	if err != nil {
		t.Fatalf("ListSchedulerStates: %v", err)
	}

	state := findSchedulerState(states, jobType)
	if state == nil {
		t.Fatalf("missing scheduler state for %s", jobType)
	}
	if state.Enabled {
		t.Fatalf("expected disabled scheduler state")
	}
	if state.PolicyError != "" {
		t.Fatalf("unexpected policy error: %s", state.PolicyError)
	}
	if !state.DetectorAvailable || state.DetectorWorkerID != "worker-b" {
		t.Fatalf("unexpected detector details: available=%v worker=%s", state.DetectorAvailable, state.DetectorWorkerID)
	}
	if state.ExecutorWorkerCount != 1 {
		t.Fatalf("unexpected executor worker count: got=%d", state.ExecutorWorkerCount)
	}
}

func findSchedulerState(states []SchedulerJobTypeState, jobType string) *SchedulerJobTypeState {
	for i := range states {
		if states[i].JobType == jobType {
			return &states[i]
		}
	}
	return nil
}

func TestPickDetectorPrefersLeasedWorker(t *testing.T) {
	t.Parallel()

	pluginSvc, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pluginSvc.Shutdown()

	pluginSvc.registry.UpsertFromHello(&plugin_pb.WorkerHello{
		WorkerId: "worker-a",
		Capabilities: []*plugin_pb.JobTypeCapability{
			{JobType: "vacuum", CanDetect: true},
		},
	})
	pluginSvc.registry.UpsertFromHello(&plugin_pb.WorkerHello{
		WorkerId: "worker-b",
		Capabilities: []*plugin_pb.JobTypeCapability{
			{JobType: "vacuum", CanDetect: true},
		},
	})

	pluginSvc.setDetectorLease("vacuum", "worker-b")

	detector, err := pluginSvc.pickDetector("vacuum")
	if err != nil {
		t.Fatalf("pickDetector: %v", err)
	}
	if detector.WorkerID != "worker-b" {
		t.Fatalf("expected leased detector worker-b, got=%s", detector.WorkerID)
	}
}

func TestPickDetectorReassignsWhenLeaseIsStale(t *testing.T) {
	t.Parallel()

	pluginSvc, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pluginSvc.Shutdown()

	pluginSvc.registry.UpsertFromHello(&plugin_pb.WorkerHello{
		WorkerId: "worker-a",
		Capabilities: []*plugin_pb.JobTypeCapability{
			{JobType: "vacuum", CanDetect: true},
		},
	})
	pluginSvc.setDetectorLease("vacuum", "worker-stale")

	detector, err := pluginSvc.pickDetector("vacuum")
	if err != nil {
		t.Fatalf("pickDetector: %v", err)
	}
	if detector.WorkerID != "worker-a" {
		t.Fatalf("expected reassigned detector worker-a, got=%s", detector.WorkerID)
	}

	lease := pluginSvc.getDetectorLease("vacuum")
	if lease != "worker-a" {
		t.Fatalf("expected detector lease to be updated to worker-a, got=%s", lease)
	}
}

// mockLockManager records lock/release calls for testing.
type mockLockManager struct {
	mu           sync.Mutex
	acquireCount int
	releaseCount int
	failAcquire  bool
}

func (m *mockLockManager) Acquire(reason string) (func(), error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failAcquire {
		return nil, fmt.Errorf("mock lock acquisition failed")
	}
	m.acquireCount++
	return func() {
		m.mu.Lock()
		m.releaseCount++
		m.mu.Unlock()
	}, nil
}

func (m *mockLockManager) Status() interface{} {
	return nil
}

func (m *mockLockManager) getAcquireCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.acquireCount
}

func (m *mockLockManager) getReleaseCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.releaseCount
}

func TestIterationAcquiresLockOnce(t *testing.T) {
	t.Parallel()

	lock := &mockLockManager{}
	pluginSvc, err := New(Options{
		LockManager: lock,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pluginSvc.Shutdown()

	// Register two enabled job types.
	for _, jt := range []string{"vacuum", "balance"} {
		err = pluginSvc.SaveJobTypeConfig(&plugin_pb.PersistedJobTypeConfig{
			JobType: jt,
			AdminRuntime: &plugin_pb.AdminRuntimeConfig{
				Enabled:                  true,
				DetectionTimeoutSeconds:  5,
				MaxJobsPerDetection:      10,
				GlobalExecutionConcurrency: 1,
				PerWorkerExecutionConcurrency: 1,
			},
		})
		if err != nil {
			t.Fatalf("SaveJobTypeConfig(%s): %v", jt, err)
		}
		pluginSvc.registry.UpsertFromHello(&plugin_pb.WorkerHello{
			WorkerId: "worker-" + jt,
			Capabilities: []*plugin_pb.JobTypeCapability{
				{JobType: jt, CanDetect: true, CanExecute: true},
			},
		})
	}

	// runSchedulerIteration requires a cluster context provider.
	pluginSvc.clusterContextProvider = func(_ context.Context) (*plugin_pb.ClusterContext, error) {
		return &plugin_pb.ClusterContext{}, nil
	}

	pluginSvc.runSchedulerIteration()

	// Lock should have been acquired exactly once (not per-job-type).
	if lock.getAcquireCount() != 1 {
		t.Fatalf("expected 1 lock acquisition, got %d", lock.getAcquireCount())
	}
	if lock.getReleaseCount() != 1 {
		t.Fatalf("expected 1 lock release, got %d", lock.getReleaseCount())
	}
}

func TestIterationReturnsfalseWhenLockFails(t *testing.T) {
	t.Parallel()

	lock := &mockLockManager{failAcquire: true}
	pluginSvc, err := New(Options{
		LockManager: lock,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pluginSvc.Shutdown()

	err = pluginSvc.SaveJobTypeConfig(&plugin_pb.PersistedJobTypeConfig{
		JobType: "vacuum",
		AdminRuntime: &plugin_pb.AdminRuntimeConfig{
			Enabled:                  true,
			DetectionTimeoutSeconds:  5,
			MaxJobsPerDetection:      10,
			GlobalExecutionConcurrency: 1,
			PerWorkerExecutionConcurrency: 1,
		},
	})
	if err != nil {
		t.Fatalf("SaveJobTypeConfig: %v", err)
	}
	pluginSvc.registry.UpsertFromHello(&plugin_pb.WorkerHello{
		WorkerId: "worker-a",
		Capabilities: []*plugin_pb.JobTypeCapability{
			{JobType: "vacuum", CanDetect: true, CanExecute: true},
		},
	})

	pluginSvc.clusterContextProvider = func(_ context.Context) (*plugin_pb.ClusterContext, error) {
		return &plugin_pb.ClusterContext{}, nil
	}

	result := pluginSvc.runSchedulerIteration()
	if result {
		t.Fatalf("expected false when lock acquisition fails")
	}
}

func TestSchedulerPhaseTracking(t *testing.T) {
	t.Parallel()

	pluginSvc, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pluginSvc.Shutdown()

	// Initial phase should be idle.
	pluginSvc.schedulerMu.Lock()
	phase := pluginSvc.schedulerPhase
	pluginSvc.schedulerMu.Unlock()

	if phase != "idle" {
		t.Fatalf("expected initial phase to be idle, got=%s", phase)
	}

	pluginSvc.setSchedulerPhase("processing", "vacuum")

	pluginSvc.schedulerMu.Lock()
	phase = pluginSvc.schedulerPhase
	jobType := pluginSvc.currentJobType
	pluginSvc.schedulerMu.Unlock()

	if phase != "processing" {
		t.Fatalf("expected phase processing, got=%s", phase)
	}
	if jobType != "vacuum" {
		t.Fatalf("expected current job type vacuum, got=%s", jobType)
	}

	pluginSvc.finishIteration(true)

	pluginSvc.schedulerMu.Lock()
	phase = pluginSvc.schedulerPhase
	jobType = pluginSvc.currentJobType
	workDetected := pluginSvc.lastIterationWorkDetected
	lastEnded := pluginSvc.lastIterationEndedAt
	pluginSvc.schedulerMu.Unlock()

	if phase != "idle" {
		t.Fatalf("expected phase idle after finish, got=%s", phase)
	}
	if jobType != "" {
		t.Fatalf("expected empty job type after finish, got=%s", jobType)
	}
	if !workDetected {
		t.Fatalf("expected last iteration work detected to be true")
	}
	if lastEnded.IsZero() {
		t.Fatalf("expected last iteration ended at to be set")
	}
}

func TestGetSchedulerStatusIncludesIterationFields(t *testing.T) {
	t.Parallel()

	pluginSvc, err := New(Options{
		IdleSleepDuration: 10 * time.Minute,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pluginSvc.Shutdown()

	now := time.Now().UTC()
	pluginSvc.schedulerMu.Lock()
	pluginSvc.schedulerPhase = "processing"
	pluginSvc.currentJobType = "vacuum"
	pluginSvc.iterationStartedAt = now.Add(-5 * time.Second)
	pluginSvc.lastIterationEndedAt = now.Add(-20 * time.Second)
	pluginSvc.lastIterationWorkDetected = true
	pluginSvc.schedulerMu.Unlock()

	status := pluginSvc.GetSchedulerStatus()

	if status.Phase != "processing" {
		t.Fatalf("expected phase processing, got=%s", status.Phase)
	}
	if status.CurrentJobType != "vacuum" {
		t.Fatalf("expected current job type vacuum, got=%s", status.CurrentJobType)
	}
	if status.IdleSleepSeconds != 600 {
		t.Fatalf("expected idle sleep 600s, got=%d", status.IdleSleepSeconds)
	}
	if status.IterationStartedAt == nil {
		t.Fatalf("expected iteration started at to be set")
	}
	if status.LastIterationEndedAt == nil {
		t.Fatalf("expected last iteration ended at to be set")
	}
	if !status.LastIterationWorkDetected {
		t.Fatalf("expected last iteration work detected to be true")
	}
	// SchedulerTickSeconds should match IdleSleepSeconds for backward compat.
	if status.SchedulerTickSeconds != 600 {
		t.Fatalf("expected scheduler tick seconds to match idle sleep, got=%d", status.SchedulerTickSeconds)
	}
}

func TestGracefulShutdownDuringIteration(t *testing.T) {
	t.Parallel()

	pluginSvc, err := New(Options{
		IdleSleepDuration: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// Register an enabled job type.
	err = pluginSvc.SaveJobTypeConfig(&plugin_pb.PersistedJobTypeConfig{
		JobType: "vacuum",
		AdminRuntime: &plugin_pb.AdminRuntimeConfig{
			Enabled:                  true,
			DetectionTimeoutSeconds:  5,
			MaxJobsPerDetection:      10,
			GlobalExecutionConcurrency: 1,
			PerWorkerExecutionConcurrency: 1,
		},
	})
	if err != nil {
		t.Fatalf("SaveJobTypeConfig: %v", err)
	}
	pluginSvc.registry.UpsertFromHello(&plugin_pb.WorkerHello{
		WorkerId: "worker-a",
		Capabilities: []*plugin_pb.JobTypeCapability{
			{JobType: "vacuum", CanDetect: true, CanExecute: true},
		},
	})

	// Shutdown immediately — the scheduler loop should exit cleanly.
	done := make(chan struct{})
	go func() {
		pluginSvc.Shutdown()
		close(done)
	}()

	select {
	case <-done:
		// Good — clean shutdown.
	case <-time.After(5 * time.Second):
		t.Fatalf("shutdown did not complete in time")
	}
}

func TestIdleSleepDurationDefault(t *testing.T) {
	t.Parallel()

	pluginSvc, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pluginSvc.Shutdown()

	if pluginSvc.idleSleepDuration != defaultIdleSleepDuration {
		t.Fatalf("expected default idle sleep %v, got=%v", defaultIdleSleepDuration, pluginSvc.idleSleepDuration)
	}
}

func TestIdleSleepDurationCustom(t *testing.T) {
	t.Parallel()

	customDuration := 5 * time.Minute
	pluginSvc, err := New(Options{
		IdleSleepDuration: customDuration,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pluginSvc.Shutdown()

	if pluginSvc.idleSleepDuration != customDuration {
		t.Fatalf("expected custom idle sleep %v, got=%v", customDuration, pluginSvc.idleSleepDuration)
	}
}
