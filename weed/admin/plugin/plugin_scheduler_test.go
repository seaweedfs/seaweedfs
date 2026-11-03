package plugin

import (
	"context"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"google.golang.org/protobuf/types/known/timestamppb"
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
			DetectionIntervalMinutes:      30,
			DetectionTimeoutSeconds:       20,
			MaxJobsPerDetection:           123,
			GlobalExecutionConcurrency:    5,
			PerWorkerExecutionConcurrency: 2,
			RetryLimit:                    4,
			RetryBackoffSeconds:           7,
			JobTypeMaxRuntimeSeconds:      1800,
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
	if policy.JobTypeMaxRuntime != 30*time.Minute {
		t.Fatalf("unexpected max runtime: got=%v", policy.JobTypeMaxRuntime)
	}
	if policy.LocalQueuePin != defaultLocalQueuePin {
		t.Fatalf("unexpected local queue pin: got=%d", policy.LocalQueuePin)
	}
	if policy.OrphanRehomeGrace != defaultOrphanRehomeGrace {
		t.Fatalf("unexpected orphan rehome grace: got=%v", policy.OrphanRehomeGrace)
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
			DetectionIntervalMinutes:      60,
			DetectionTimeoutSeconds:       25,
			MaxJobsPerDetection:           30,
			GlobalExecutionConcurrency:    4,
			PerWorkerExecutionConcurrency: 2,
			RetryLimit:                    3,
			RetryBackoffSeconds:           6,
			JobTypeMaxRuntimeSeconds:      1200,
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
	if policy.JobTypeMaxRuntime != 20*time.Minute {
		t.Fatalf("unexpected max runtime: got=%v", policy.JobTypeMaxRuntime)
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

	executor1, release1, err := pluginSvc.reserveScheduledExecutor(context.Background(), "balance", policy, nil)
	if err != nil {
		t.Fatalf("reserve executor 1: %v", err)
	}
	defer release1()

	executor2, release2, err := pluginSvc.reserveScheduledExecutor(context.Background(), "balance", policy, nil)
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
	_, _, err = pluginSvc.reserveScheduledExecutor(context.Background(), "missing-job-type", policy, nil)
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

	_, release1, err := pluginSvc.reserveScheduledExecutor(context.Background(), "balance", policy, nil)
	if err != nil {
		t.Fatalf("reserve executor 1: %v", err)
	}
	defer release1()

	type reserveResult struct {
		err error
	}
	secondReserveCh := make(chan reserveResult, 1)
	go func() {
		_, release2, reserveErr := pluginSvc.reserveScheduledExecutor(context.Background(), "balance", policy, nil)
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
			DetectionIntervalMinutes:      45,
			DetectionTimeoutSeconds:       30,
			MaxJobsPerDetection:           80,
			GlobalExecutionConcurrency:    3,
			PerWorkerExecutionConcurrency: 2,
			RetryLimit:                    1,
			RetryBackoffSeconds:           9,
			JobTypeMaxRuntimeSeconds:      900,
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

	nextDetectionAt := time.Now().UTC().Add(2 * time.Minute).Round(time.Second)
	pluginSvc.schedulerMu.Lock()
	pluginSvc.nextDetectionAt[jobType] = nextDetectionAt
	pluginSvc.detectionInFlight[jobType] = true
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
		t.Fatalf("expected detection in flight")
	}
	if state.NextDetectionAt == nil {
		t.Fatalf("expected next detection time")
	}
	if state.NextDetectionAt.Unix() != nextDetectionAt.Unix() {
		t.Fatalf("unexpected next detection time: got=%v want=%v", state.NextDetectionAt, nextDetectionAt)
	}
	if state.DetectionIntervalMinutes != 45 {
		t.Fatalf("unexpected detection interval: got=%d", state.DetectionIntervalMinutes)
	}
	if state.DetectionTimeoutSeconds != 30 {
		t.Fatalf("unexpected detection timeout: got=%d", state.DetectionTimeoutSeconds)
	}
	if state.ExecutionTimeoutSeconds != 90 {
		t.Fatalf("unexpected execution timeout: got=%d", state.ExecutionTimeoutSeconds)
	}
	if state.JobTypeMaxRuntimeSeconds != 900 {
		t.Fatalf("unexpected job type max runtime: got=%d", state.JobTypeMaxRuntimeSeconds)
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

	pluginSvc.putSession(&streamSession{workerID: "worker-a", outgoing: make(chan *plugin_pb.AdminToWorkerMessage, 1), done: make(chan struct{})})
	pluginSvc.putSession(&streamSession{workerID: "worker-b", outgoing: make(chan *plugin_pb.AdminToWorkerMessage, 1), done: make(chan struct{})})
	pluginSvc.setDetectorLease("vacuum", "worker-b")

	detector, err := pluginSvc.pickDetector("vacuum", nil)
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
	pluginSvc.putSession(&streamSession{workerID: "worker-a", outgoing: make(chan *plugin_pb.AdminToWorkerMessage, 1), done: make(chan struct{})})
	pluginSvc.setDetectorLease("vacuum", "worker-stale")

	detector, err := pluginSvc.pickDetector("vacuum", nil)
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

// trackingLockManager records whether Acquire was called and how many times.
type trackingLockManager struct {
	mu       sync.Mutex
	acquired int
}

func (m *trackingLockManager) Acquire(reason string) (func(), error) {
	m.mu.Lock()
	m.acquired++
	m.mu.Unlock()
	return func() {}, nil
}

func (m *trackingLockManager) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.acquired
}

func TestRunLaneSchedulerIterationLockBehavior(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		lane     SchedulerLane
		jobType  string
		wantLock bool
	}{
		{"Default", LaneDefault, "vacuum", true},
		{"Iceberg", LaneIceberg, "iceberg_maintenance", false},
		{"Lifecycle", LaneLifecycle, "s3_lifecycle", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			lm := &trackingLockManager{}
			// Construct without a cluster-context provider so the background
			// lane loops do not start: they call runLaneSchedulerIteration on
			// the same lane and would race this test's own manual call,
			// consuming the due job before it observes the lock. The provider
			// is set afterward so the manual iteration can still detect.
			pluginSvc, err := New(Options{LockManager: lm})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			defer pluginSvc.Shutdown()
			pluginSvc.clusterContextProvider = func(context.Context) (*plugin_pb.ClusterContext, error) {
				return &plugin_pb.ClusterContext{}, nil
			}

			// Register a detectable worker for the job type.
			pluginSvc.registry.UpsertFromHello(&plugin_pb.WorkerHello{
				WorkerId: "worker-a",
				Capabilities: []*plugin_pb.JobTypeCapability{
					{JobType: tt.jobType, CanDetect: true},
				},
			})

			// Enable the job type so the scheduler picks it up.
			err = pluginSvc.SaveJobTypeConfig(&plugin_pb.PersistedJobTypeConfig{
				JobType: tt.jobType,
				AdminRuntime: &plugin_pb.AdminRuntimeConfig{
					Enabled:                  true,
					DetectionIntervalMinutes: 1,
				},
			})
			if err != nil {
				t.Fatalf("SaveJobTypeConfig: %v", err)
			}

			// Make the job type due immediately so the iteration reaches
			// detection; the lock is only taken around due work now.
			pluginSvc.schedulerMu.Lock()
			pluginSvc.nextDetectionAt[tt.jobType] = time.Now().UTC().Add(-time.Second)
			pluginSvc.schedulerMu.Unlock()

			ls := pluginSvc.lanes[tt.lane]
			pluginSvc.runLaneSchedulerIteration(ls)

			if got := lm.count(); (got > 0) != tt.wantLock {
				t.Errorf("lock acquired %d times, wantLock=%v", got, tt.wantLock)
			}
		})
	}
}

func TestDispatchScheduledProposalsLocksPerJob(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		jobType   string
		wantLocks int
	}{
		{"DefaultLaneLocksEachJob", "volume_balance", 3},
		{"LifecycleLaneNeedsNoLock", "s3_lifecycle", 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			lm := &trackingLockManager{}
			pluginSvc, err := New(Options{LockManager: lm})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			defer pluginSvc.Shutdown()

			// The worker has capabilities but no connected session, so each
			// job reserves capacity, fails to send, and moves on.
			pluginSvc.registry.UpsertFromHello(&plugin_pb.WorkerHello{
				WorkerId: "worker-a",
				Capabilities: []*plugin_pb.JobTypeCapability{
					{JobType: tt.jobType, CanExecute: true, MaxExecutionConcurrency: 4},
				},
			})

			policy := schedulerPolicy{
				ExecutionConcurrency:   1,
				PerWorkerConcurrency:   1,
				ExecutionTimeout:       time.Second,
				ExecutorReserveBackoff: time.Millisecond,
			}
			proposals := []*plugin_pb.JobProposal{
				{ProposalId: "p1", JobType: tt.jobType, DedupeKey: "k1"},
				{ProposalId: "p2", JobType: tt.jobType, DedupeKey: "k2"},
				{ProposalId: "p3", JobType: tt.jobType, DedupeKey: "k3"},
			}

			success, errors, canceled := pluginSvc.dispatchScheduledProposals(
				context.Background(), tt.jobType, proposals, &plugin_pb.ClusterContext{}, policy)
			if success != 0 || canceled != 0 || errors != 3 {
				t.Fatalf("unexpected dispatch counts: success=%d errors=%d canceled=%d", success, errors, canceled)
			}
			if got := lm.count(); got != tt.wantLocks {
				t.Fatalf("lock acquired %d times, want %d", got, tt.wantLocks)
			}
		})
	}
}

func TestScheduledAttemptContextDrainGrace(t *testing.T) {
	t.Parallel()

	assertDeadline := func(t *testing.T, ctx context.Context, want time.Time) {
		t.Helper()
		got, ok := ctx.Deadline()
		if !ok {
			t.Fatal("expected attempt context to carry a deadline")
		}
		if diff := got.Sub(want); diff < -5*time.Second || diff > 5*time.Second {
			t.Fatalf("unexpected attempt deadline: got=%v want~%v", got, want)
		}
	}

	windowEnd := time.Now().Add(time.Minute)
	window, cancelWindow := context.WithDeadline(context.Background(), windowEnd)
	defer cancelWindow()

	// Fits inside the window: keeps its own deadline.
	ctx, cancel := scheduledAttemptContext(window, 30*time.Second, 0)
	assertDeadline(t, ctx, time.Now().Add(30*time.Second))
	cancel()

	// Longer: capped at window close plus the grace.
	ctx, cancel = scheduledAttemptContext(window, 3*time.Hour, 0)
	assertDeadline(t, ctx, windowEnd.Add(scheduledExecutionDrainGrace))
	cancel()

	// Estimated runtime: keeps its full deadline.
	ctx, cancel = scheduledAttemptContext(window, 3*time.Hour, 3*time.Hour)
	assertDeadline(t, ctx, time.Now().Add(3*time.Hour))
	cancel()

	// An estimate below the timeout floors the drain cap.
	ctx, cancel = scheduledAttemptContext(window, 3*time.Hour, 45*time.Minute)
	assertDeadline(t, ctx, time.Now().Add(45*time.Minute))
	cancel()

	// No window deadline: the timeout stands alone.
	ctx, cancel = scheduledAttemptContext(context.Background(), 2*time.Hour, 0)
	assertDeadline(t, ctx, time.Now().Add(2*time.Hour))
	cancel()
}

func TestScheduledEstimatedRuntime(t *testing.T) {
	t.Parallel()

	estimate := func(v int64) map[string]*plugin_pb.ConfigValue {
		return map[string]*plugin_pb.ConfigValue{
			"estimated_runtime_seconds": {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: v}},
		}
	}

	if got := scheduledEstimatedRuntime(nil); got != 0 {
		t.Fatalf("nil parameters: got=%v want=0", got)
	}
	if got := scheduledEstimatedRuntime(estimate(-5)); got != 0 {
		t.Fatalf("negative estimate: got=%v want=0", got)
	}
	if got := scheduledEstimatedRuntime(estimate(600)); got != 10*time.Minute {
		t.Fatalf("normal estimate: got=%v want=10m", got)
	}
	// Oversized seconds cap before the Duration conversion can overflow.
	if got := scheduledEstimatedRuntime(estimate(math.MaxInt64)); got != maxEstimatedRuntimeCap {
		t.Fatalf("oversized estimate: got=%v want=%v", got, maxEstimatedRuntimeCap)
	}
}

func TestDispatchScheduledProposalsDrainsStartedJobOnWindowClose(t *testing.T) {
	t.Parallel()

	pluginSvc, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pluginSvc.Shutdown()

	const workerID = "worker-drain"
	const jobType = "s3_lifecycle" // lifecycle lane: no admin lock needed
	pluginSvc.registry.UpsertFromHello(&plugin_pb.WorkerHello{
		WorkerId: workerID,
		Capabilities: []*plugin_pb.JobTypeCapability{
			{JobType: jobType, CanExecute: true, MaxExecutionConcurrency: 1},
		},
	})
	session := &streamSession{workerID: workerID, outgoing: make(chan *plugin_pb.AdminToWorkerMessage, 8), done: make(chan struct{})}
	pluginSvc.putSession(session)

	windowCtx, closeWindow := context.WithCancel(context.Background())
	defer closeWindow()

	policy := schedulerPolicy{
		ExecutionConcurrency:   1,
		PerWorkerConcurrency:   1,
		ExecutionTimeout:       time.Hour,
		ExecutorReserveBackoff: time.Millisecond,
	}
	proposals := []*plugin_pb.JobProposal{
		{ProposalId: "p1", JobType: jobType, DedupeKey: "k1"},
		{ProposalId: "p2", JobType: jobType, DedupeKey: "k2"},
	}

	resultCh := make(chan [3]int, 1)
	go func() {
		success, errCount, canceled := pluginSvc.dispatchScheduledProposals(
			windowCtx, jobType, proposals, &plugin_pb.ClusterContext{}, policy)
		resultCh <- [3]int{success, errCount, canceled}
	}()

	first := <-session.outgoing
	execReq := first.GetExecuteJobRequest()
	if execReq == nil {
		t.Fatalf("expected execute_job_request, got %+v", first)
	}

	// Close the window mid-job: the job must drain to completion while the
	// still-queued second proposal is canceled.
	closeWindow()
	pluginSvc.handleJobCompleted(&plugin_pb.JobCompleted{
		RequestId:   first.RequestId,
		JobId:       execReq.Job.JobId,
		JobType:     jobType,
		Success:     true,
		CompletedAt: timestamppb.Now(),
	})

	result := <-resultCh
	if result[0] != 1 || result[1] != 0 || result[2] != 1 {
		t.Fatalf("unexpected dispatch counts: success=%d errors=%d canceled=%d", result[0], result[1], result[2])
	}

	select {
	case unexpected := <-session.outgoing:
		t.Fatalf("expected no further worker messages (no cancel for the draining job), got %+v", unexpected)
	default:
	}
}

// ---------- lane-scoped prune ----------

func TestPruneSchedulerState_DefaultLaneKeepsForeignLanesAndPrunesOwnStale(t *testing.T) {
	p, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer p.Shutdown()

	now := time.Now().UTC()
	p.schedulerMu.Lock()
	p.nextDetectionAt["s3_lifecycle"] = now.Add(24 * time.Hour) // lifecycle lane
	p.nextDetectionAt["vacuum"] = now.Add(time.Minute)          // default lane, active
	p.nextDetectionAt["ec_balance"] = now.Add(time.Minute)      // default lane, stale
	p.detectionInFlight["ec_balance"] = true
	p.schedulerMu.Unlock()

	// Default-lane iteration prunes with only its own active job types.
	p.pruneSchedulerState(LaneDefault, map[string]struct{}{"vacuum": {}})

	p.schedulerMu.Lock()
	defer p.schedulerMu.Unlock()
	if _, ok := p.nextDetectionAt["s3_lifecycle"]; !ok {
		t.Fatal("default-lane prune must not delete lifecycle-lane nextDetectionAt[s3_lifecycle]")
	}
	if _, ok := p.nextDetectionAt["vacuum"]; !ok {
		t.Fatal("active default-lane job (vacuum) must be kept")
	}
	if _, ok := p.nextDetectionAt["ec_balance"]; ok {
		t.Fatal("stale default-lane job (ec_balance) must still be pruned within its own lane")
	}
	if _, ok := p.detectionInFlight["ec_balance"]; ok {
		t.Fatal("pruned job must also drop its detectionInFlight entry")
	}
}

func TestPruneSchedulerState_LifecycleLaneLeavesDefaultLane(t *testing.T) {
	p, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer p.Shutdown()

	now := time.Now().UTC()
	p.schedulerMu.Lock()
	p.nextDetectionAt["vacuum"] = now.Add(time.Minute)          // default lane
	p.nextDetectionAt["s3_lifecycle"] = now.Add(24 * time.Hour) // lifecycle lane, active
	p.schedulerMu.Unlock()

	p.pruneSchedulerState(LaneLifecycle, map[string]struct{}{"s3_lifecycle": {}})

	p.schedulerMu.Lock()
	defer p.schedulerMu.Unlock()
	if _, ok := p.nextDetectionAt["vacuum"]; !ok {
		t.Fatal("lifecycle-lane prune must not delete default-lane nextDetectionAt[vacuum]")
	}
	if _, ok := p.nextDetectionAt["s3_lifecycle"]; !ok {
		t.Fatal("active lifecycle job (s3_lifecycle) must be kept")
	}
}

func TestPruneDetectorLeases_IsLaneScoped(t *testing.T) {
	p, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer p.Shutdown()

	p.detectorLeaseMu.Lock()
	p.detectorLeases["s3_lifecycle"] = "worker-a" // lifecycle lane
	p.detectorLeases["vacuum"] = "worker-b"       // default lane, active
	p.detectorLeases["ec_balance"] = "worker-c"   // default lane, stale
	p.detectorLeaseMu.Unlock()

	p.pruneDetectorLeases(LaneDefault, map[string]struct{}{"vacuum": {}})

	p.detectorLeaseMu.Lock()
	defer p.detectorLeaseMu.Unlock()
	if _, ok := p.detectorLeases["s3_lifecycle"]; !ok {
		t.Fatal("default-lane prune must not delete lifecycle-lane detector lease")
	}
	if _, ok := p.detectorLeases["vacuum"]; !ok {
		t.Fatal("active default-lane detector lease (vacuum) must be kept")
	}
	if _, ok := p.detectorLeases["ec_balance"]; ok {
		t.Fatal("stale default-lane detector lease (ec_balance) must still be pruned within its own lane")
	}
}

func TestLaneStatus_LifecycleNextDetectionSurvivesDefaultLanePrune(t *testing.T) {
	p, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer p.Shutdown()

	now := time.Now().UTC()
	expected := now.Add(24 * time.Hour)
	p.schedulerMu.Lock()
	p.nextDetectionAt["s3_lifecycle"] = expected
	p.nextDetectionAt["vacuum"] = now.Add(time.Minute)
	p.schedulerMu.Unlock()

	p.pruneSchedulerState(LaneDefault, map[string]struct{}{"vacuum": {}})

	status := p.GetLaneSchedulerStatus(LaneLifecycle)
	if status.NextDetectionAt == nil {
		t.Fatal("lifecycle lane status lost next_detection_at after a default-lane prune")
	}
	if !status.NextDetectionAt.Equal(expected) {
		t.Fatalf("next_detection_at = %v, want %v (must be the 24h schedule, not the idle-sleep fallback)",
			status.NextDetectionAt, expected)
	}
}

func TestReserveWaterfillsPreferredHosts(t *testing.T) {
	t.Parallel()

	pluginSvc, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pluginSvc.Shutdown()

	ids := []string{"worker-a", "worker-b", "worker-c", "worker-d"}
	for _, id := range ids {
		pluginSvc.registry.UpsertFromHello(&plugin_pb.WorkerHello{
			WorkerId: id,
			Address:  "10.0.0.1:9334",
			Capabilities: []*plugin_pb.JobTypeCapability{
				{JobType: "erasure_coding", CanExecute: true, MaxExecutionConcurrency: 8},
			},
		})
	}

	policy := schedulerPolicy{
		PerWorkerConcurrency:   8,
		ExecutorReserveBackoff: time.Millisecond,
		LocalQueuePin:          5,
	}
	preferred := []string{"10.0.0.1:8080"}

	seen := map[string]int{}
	var releases []func()
	defer func() {
		for _, release := range releases {
			release()
		}
	}()
	for i := 0; i < 4; i++ {
		exec, release, err := pluginSvc.reserveScheduledExecutor(context.Background(), "erasure_coding", policy, preferred)
		if err != nil {
			t.Fatalf("reserve %d: %v", i, err)
		}
		releases = append(releases, release)
		seen[exec.WorkerID]++
	}
	if len(seen) != 4 {
		t.Fatalf("want one job per worker, got %v", seen)
	}
}

// ---------- executor capacity ----------

// registerBusyWorker connects one worker that can run every job type in
// caps with four slots, one of which the worker reports taken by running
// work of busyType that this admin never dispatched.
func registerBusyWorker(t *testing.T, pluginSvc *Plugin, workerID, busyType, busyJobID string, caps ...string) {
	t.Helper()
	hello := &plugin_pb.WorkerHello{WorkerId: workerID}
	for _, jobType := range caps {
		hello.Capabilities = append(hello.Capabilities, &plugin_pb.JobTypeCapability{
			JobType: jobType, CanExecute: true, MaxExecutionConcurrency: 4,
		})
	}
	pluginSvc.registry.UpsertFromHello(hello)
	heartbeat := &plugin_pb.WorkerHeartbeat{
		WorkerId:            workerID,
		ExecutionSlotsUsed:  1,
		ExecutionSlotsTotal: 4,
		RunningWork: []*plugin_pb.RunningWork{{
			WorkId:  busyJobID,
			Kind:    plugin_pb.WorkKind_WORK_KIND_EXECUTION,
			JobType: busyType,
			State:   plugin_pb.JobState_JOB_STATE_RUNNING,
			Stage:   "starting",
		}},
	}
	pluginSvc.registry.UpdateHeartbeat(workerID, heartbeat)
}

// The field case: the only admin_script-capable worker has 4 slots, one held
// by an s3_lifecycle job the worker reports running that this admin never
// dispatched, and admin_script runs at global 1 / per-worker 1.
// per_worker_execution_concurrency bounds admin_script work, so the
// lifecycle job must not make admin_script undispatchable; the limit still
// holds within admin_script itself and against foreign work of the same type.
func TestExecutorCapacityCountsOnlyTheJobTypeBeingDispatched(t *testing.T) {
	t.Parallel()

	pluginSvc, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pluginSvc.Shutdown()

	const workerID = "w-penso--7aa8"
	registerBusyWorker(t, pluginSvc, workerID, "s3_lifecycle", "s3_lifecycle-scheduled-1793477386659262943-0",
		"admin_script", "s3_lifecycle", "vacuum", "ec_balance")
	policy := schedulerPolicy{ExecutionConcurrency: 1, PerWorkerConcurrency: 1, LocalQueuePin: 5}

	job := &plugin_pb.JobSpec{JobId: "admin_script-scheduled-1793738609215134308-0", JobType: "admin_script", DedupeKey: "admin_script"}
	qs := pluginSvc.assignJobsToWorkerQueues("admin_script", []*plugin_pb.JobSpec{job}, policy)
	item, target := pluginSvc.takeQueuedJob(qs, "admin_script", policy)
	if item == nil || target != workerID {
		t.Fatalf("admin_script should dispatch to %s past the foreign s3_lifecycle job, got item=%v target=%q", workerID, item, target)
	}
	executor, release, err := pluginSvc.reserveExecutorByID("admin_script", policy, target, job.JobId)
	if err != nil {
		t.Fatalf("reserve admin_script: %v", err)
	}
	defer release()

	// The per-type limit of 1 is now spent on admin_script's own job.
	if pluginSvc.executorHasCapacity(executor, "admin_script", policy) {
		t.Fatalf("a second admin_script job must wait for the first")
	}
	// Unrelated types on the same worker still have their own per-type budget.
	if !pluginSvc.executorHasCapacity(executor, "vacuum", policy) {
		t.Fatalf("vacuum should still fit: 2 of 4 slots used, none by vacuum")
	}
	// Foreign work of the same type counts against that type's limit.
	if pluginSvc.executorHasCapacity(executor, "s3_lifecycle", policy) {
		t.Fatalf("a second s3_lifecycle job must not fit beside the foreign one at per-worker 1")
	}
}

// Per-type budgets never add up to more than the worker's slot total: a
// worker whose slots are all taken by other job types takes nothing, whether
// it itemized that work or only reported the count.
func TestExecutorCapacityHonorsWorkerSlotTotalAcrossJobTypes(t *testing.T) {
	t.Parallel()

	pluginSvc, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pluginSvc.Shutdown()

	const workerID = "worker-a"
	pluginSvc.registry.UpsertFromHello(&plugin_pb.WorkerHello{
		WorkerId: workerID,
		Capabilities: []*plugin_pb.JobTypeCapability{
			{JobType: "admin_script", CanExecute: true, MaxExecutionConcurrency: 4},
			{JobType: "s3_lifecycle", CanExecute: true, MaxExecutionConcurrency: 4},
		},
	})
	policy := schedulerPolicy{ExecutionConcurrency: 1, PerWorkerConcurrency: 1}
	executor := func() *WorkerSession {
		session, ok := pluginSvc.registry.Get(workerID)
		if !ok {
			t.Fatalf("worker missing")
		}
		return session
	}

	pluginSvc.registry.UpdateHeartbeat(workerID, &plugin_pb.WorkerHeartbeat{
		WorkerId: workerID, ExecutionSlotsUsed: 1, ExecutionSlotsTotal: 1,
		RunningWork: []*plugin_pb.RunningWork{{
			WorkId: "lc-1", Kind: plugin_pb.WorkKind_WORK_KIND_EXECUTION, JobType: "s3_lifecycle",
		}},
	})
	if pluginSvc.executorHasCapacity(executor(), "admin_script", policy) {
		t.Fatalf("the worker's only slot is taken by s3_lifecycle")
	}

	pluginSvc.registry.UpdateHeartbeat(workerID, &plugin_pb.WorkerHeartbeat{
		WorkerId: workerID, ExecutionSlotsUsed: 1, ExecutionSlotsTotal: 1,
	})
	if pluginSvc.executorHasCapacity(executor(), "admin_script", policy) {
		t.Fatalf("a slot the worker counts as used is used even when not itemized")
	}

	pluginSvc.registry.UpdateHeartbeat(workerID, &plugin_pb.WorkerHeartbeat{
		WorkerId: workerID, ExecutionSlotsUsed: 0, ExecutionSlotsTotal: 1,
	})
	if !pluginSvc.executorHasCapacity(executor(), "admin_script", policy) {
		t.Fatalf("a free worker should take the job")
	}
}

// A job this admin dispatched shows up twice, as its reservation and in the
// worker's heartbeat. It is one slot, and must not be charged as two.
func TestExecutorCapacityDoesNotDoubleCountReservedJobsEchoedByHeartbeat(t *testing.T) {
	t.Parallel()

	pluginSvc, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pluginSvc.Shutdown()

	const workerID = "worker-a"
	pluginSvc.registry.UpsertFromHello(&plugin_pb.WorkerHello{
		WorkerId: workerID,
		Capabilities: []*plugin_pb.JobTypeCapability{
			{JobType: "vacuum", CanExecute: true, MaxExecutionConcurrency: 4},
		},
	})
	policy := schedulerPolicy{ExecutionConcurrency: 2, PerWorkerConcurrency: 2}

	_, release, err := pluginSvc.reserveExecutorByID("vacuum", policy, workerID, "vacuum-1")
	if err != nil {
		t.Fatalf("reserve vacuum-1: %v", err)
	}
	defer release()

	pluginSvc.registry.UpdateHeartbeat(workerID, &plugin_pb.WorkerHeartbeat{
		WorkerId: workerID, ExecutionSlotsUsed: 1, ExecutionSlotsTotal: 4,
		RunningWork: []*plugin_pb.RunningWork{{
			WorkId: "vacuum-1", Kind: plugin_pb.WorkKind_WORK_KIND_EXECUTION, JobType: "vacuum",
		}},
	})
	executor, _ := pluginSvc.registry.Get(workerID)
	if got := pluginSvc.executorWorkerLoad(executor); got != 1 {
		t.Fatalf("worker load = %d, want 1: the reservation and its heartbeat echo are one slot", got)
	}
	if !pluginSvc.executorHasCapacity(executor, "vacuum", policy) {
		t.Fatalf("one vacuum job in flight leaves room for a second at per-worker 2")
	}
}

// ---------- undispatchable jobs do not hold the lane ----------

// A worker whose slots are all held by work this admin never dispatched
// will not free them on any schedule the lane can see. The run gives the
// job up at once, says why, and leaves the window for the lane's other
// job types instead of idling until job_type_max_runtime.
func TestDispatchGivesUpWhenCapacityIsHeldByForeignWork(t *testing.T) {
	t.Parallel()

	pluginSvc, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pluginSvc.Shutdown()

	const workerID = "worker-a"
	pluginSvc.registry.UpsertFromHello(&plugin_pb.WorkerHello{
		WorkerId: workerID,
		Capabilities: []*plugin_pb.JobTypeCapability{
			{JobType: "admin_script", CanExecute: true, MaxExecutionConcurrency: 1},
		},
	})
	pluginSvc.registry.UpdateHeartbeat(workerID, &plugin_pb.WorkerHeartbeat{
		WorkerId: workerID, ExecutionSlotsUsed: 1, ExecutionSlotsTotal: 1,
		RunningWork: []*plugin_pb.RunningWork{{
			WorkId: "lc-1", Kind: plugin_pb.WorkKind_WORK_KIND_EXECUTION, JobType: "s3_lifecycle",
		}},
	})

	window, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	policy := schedulerPolicy{
		ExecutionConcurrency:   1,
		PerWorkerConcurrency:   1,
		ExecutionTimeout:       time.Second,
		ExecutorReserveBackoff: time.Millisecond,
		OrphanRehomeGrace:      time.Second,
	}
	proposals := []*plugin_pb.JobProposal{{ProposalId: "p1", JobType: "admin_script", DedupeKey: "k1"}}

	started := time.Now()
	success, errCount, canceled := pluginSvc.dispatchScheduledProposals(window, "admin_script", proposals, &plugin_pb.ClusterContext{}, policy)
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("dispatch held the lane for %v with no executor able to take the job", elapsed)
	}
	if success != 0 || errCount != 0 || canceled != 1 {
		t.Fatalf("unexpected dispatch counts: success=%d errors=%d canceled=%d", success, errCount, canceled)
	}

	activity := findActivity(pluginSvc.ListActivities("admin_script", 100), "admin_scheduler", "skipped_no_executor")
	if activity == nil {
		t.Fatalf("expected a skipped_no_executor activity, got %+v", pluginSvc.ListActivities("admin_script", 100))
	}
	want := "no executor capacity for admin_script: worker worker-a has 1/1 slots used by s3_lifecycle this admin did not dispatch"
	if activity.Message != want {
		t.Fatalf("activity message = %q, want %q", activity.Message, want)
	}
	for _, job := range pluginSvc.ListTrackedJobs("admin_script", "", 10) {
		if job.State != StateCanceled || job.Message != want {
			t.Fatalf("queued job should be canceled with the reason, got %+v", job)
		}
	}
}

// Capacity held by the lane's own jobs is a different matter: they finish
// and release, so the run waits for them as before.
func TestDispatchWaitsForItsOwnLaneToReleaseCapacity(t *testing.T) {
	t.Parallel()

	pluginSvc, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pluginSvc.Shutdown()

	const workerID = "worker-a"
	pluginSvc.registry.UpsertFromHello(&plugin_pb.WorkerHello{
		WorkerId: workerID,
		Capabilities: []*plugin_pb.JobTypeCapability{
			{JobType: "admin_script", CanExecute: true, MaxExecutionConcurrency: 1},
			{JobType: "vacuum", CanExecute: true, MaxExecutionConcurrency: 1},
		},
	})
	pluginSvc.registry.UpdateHeartbeat(workerID, &plugin_pb.WorkerHeartbeat{
		WorkerId: workerID, ExecutionSlotsUsed: 0, ExecutionSlotsTotal: 1,
	})
	session := &streamSession{workerID: workerID, outgoing: make(chan *plugin_pb.AdminToWorkerMessage, 8), done: make(chan struct{})}
	pluginSvc.putSession(session)

	policy := schedulerPolicy{
		ExecutionConcurrency:   1,
		PerWorkerConcurrency:   1,
		ExecutionTimeout:       time.Hour,
		ExecutorReserveBackoff: time.Millisecond,
		OrphanRehomeGrace:      time.Second,
	}
	// A vacuum job of the same (default) lane holds the worker's only slot.
	_, releaseVacuum, err := pluginSvc.reserveExecutorByID("vacuum", policy, workerID, "vacuum-1")
	if err != nil {
		t.Fatalf("reserve vacuum: %v", err)
	}

	resultCh := make(chan [3]int, 1)
	go func() {
		success, errCount, canceled := pluginSvc.dispatchScheduledProposals(
			context.Background(), "admin_script", []*plugin_pb.JobProposal{{ProposalId: "p1", JobType: "admin_script", DedupeKey: "k1"}},
			&plugin_pb.ClusterContext{}, policy)
		resultCh <- [3]int{success, errCount, canceled}
	}()

	select {
	case result := <-resultCh:
		t.Fatalf("dispatch gave up on capacity its own lane holds: %v", result)
	case <-time.After(50 * time.Millisecond):
	}

	releaseVacuum()
	first := <-session.outgoing
	execReq := first.GetExecuteJobRequest()
	if execReq == nil {
		t.Fatalf("expected execute_job_request after the vacuum slot was released, got %+v", first)
	}
	pluginSvc.handleJobCompleted(&plugin_pb.JobCompleted{
		RequestId: first.RequestId, JobId: execReq.Job.JobId, JobType: "admin_script", Success: true, CompletedAt: timestamppb.Now(),
	})
	if result := <-resultCh; result != [3]int{1, 0, 0} {
		t.Fatalf("unexpected dispatch counts: success=%d errors=%d canceled=%d", result[0], result[1], result[2])
	}
}

// With no capable worker connected the run waits out the reconnect grace,
// then gives up rather than holding the lane for the whole window.
func TestDispatchGivesUpWhenNoExecutorReturnsWithinGrace(t *testing.T) {
	t.Parallel()

	pluginSvc, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pluginSvc.Shutdown()

	window, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	policy := schedulerPolicy{
		ExecutionConcurrency:   1,
		PerWorkerConcurrency:   1,
		ExecutorReserveBackoff: time.Millisecond,
		OrphanRehomeGrace:      20 * time.Millisecond,
	}

	started := time.Now()
	_, _, canceled := pluginSvc.dispatchScheduledProposals(window, "admin_script",
		[]*plugin_pb.JobProposal{{ProposalId: "p1", JobType: "admin_script", DedupeKey: "k1"}}, &plugin_pb.ClusterContext{}, policy)
	if elapsed := time.Since(started); elapsed < policy.OrphanRehomeGrace || elapsed > 5*time.Second {
		t.Fatalf("dispatch took %v, want about the %v grace", elapsed, policy.OrphanRehomeGrace)
	}
	if canceled != 1 {
		t.Fatalf("canceled = %d, want 1", canceled)
	}
	activity := findActivity(pluginSvc.ListActivities("admin_script", 100), "admin_scheduler", "skipped_no_executor")
	if activity == nil || activity.Message != "no executor worker available for admin_script" {
		t.Fatalf("expected the no-worker reason, got %+v", activity)
	}
}

// The worker reports a job's completion before the heartbeat that frees its
// slot, so for a moment the lane's own finished job still shows as running,
// itemized by the Go worker and only as a slot count by the Rust one. That
// echo is the lane's slot coming back, not foreign work.
func TestDispatchWaitsForTheEchoOfItsOwnCompletedJob(t *testing.T) {
	t.Parallel()
	for _, itemized := range []bool{true, false} {
		itemized := itemized
		t.Run(fmt.Sprintf("itemized=%v", itemized), func(t *testing.T) {
			t.Parallel()
			pluginSvc, err := New(Options{})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			defer pluginSvc.Shutdown()

			const workerID = "worker-a"
			pluginSvc.registry.UpsertFromHello(&plugin_pb.WorkerHello{
				WorkerId: workerID,
				Capabilities: []*plugin_pb.JobTypeCapability{
					{JobType: "vacuum", CanExecute: true, MaxExecutionConcurrency: 1},
				},
			})
			idle := &plugin_pb.WorkerHeartbeat{WorkerId: workerID, ExecutionSlotsUsed: 0, ExecutionSlotsTotal: 1}
			pluginSvc.registry.UpdateHeartbeat(workerID, idle)
			session := &streamSession{workerID: workerID, outgoing: make(chan *plugin_pb.AdminToWorkerMessage, 8), done: make(chan struct{})}
			pluginSvc.putSession(session)

			policy := schedulerPolicy{
				ExecutionConcurrency:   1,
				PerWorkerConcurrency:   1,
				ExecutionTimeout:       time.Hour,
				ExecutorReserveBackoff: time.Millisecond,
				OrphanRehomeGrace:      time.Second,
			}
			proposals := []*plugin_pb.JobProposal{
				{ProposalId: "p1", JobType: "vacuum", DedupeKey: "k1"},
				{ProposalId: "p2", JobType: "vacuum", DedupeKey: "k2"},
			}
			resultCh := make(chan [3]int, 1)
			go func() {
				success, errCount, canceled := pluginSvc.dispatchScheduledProposals(
					context.Background(), "vacuum", proposals, &plugin_pb.ClusterContext{}, policy)
				resultCh <- [3]int{success, errCount, canceled}
			}()

			complete := func(message *plugin_pb.AdminToWorkerMessage) {
				t.Helper()
				execReq := message.GetExecuteJobRequest()
				if execReq == nil {
					t.Fatalf("expected execute_job_request, got %+v", message)
				}
				running := &plugin_pb.WorkerHeartbeat{WorkerId: workerID, ExecutionSlotsUsed: 1, ExecutionSlotsTotal: 1}
				if itemized {
					running.RunningWork = []*plugin_pb.RunningWork{{
						WorkId: execReq.Job.JobId, Kind: plugin_pb.WorkKind_WORK_KIND_EXECUTION, JobType: "vacuum",
					}}
				}
				pluginSvc.registry.UpdateHeartbeat(workerID, running)
				pluginSvc.handleJobCompleted(&plugin_pb.JobCompleted{
					RequestId: message.RequestId, JobId: execReq.Job.JobId, JobType: "vacuum", Success: true, CompletedAt: timestamppb.Now(),
				})
			}

			complete(<-session.outgoing)
			select {
			case result := <-resultCh:
				t.Fatalf("dispatch gave up on the echo of its own completed job: %v", result)
			case <-time.After(50 * time.Millisecond):
			}

			pluginSvc.registry.UpdateHeartbeat(workerID, idle)
			complete(<-session.outgoing)
			pluginSvc.registry.UpdateHeartbeat(workerID, idle)
			if result := <-resultCh; result != [3]int{2, 0, 0} {
				t.Fatalf("unexpected dispatch counts: success=%d errors=%d canceled=%d", result[0], result[1], result[2])
			}
		})
	}
}

// A worker may count a slot before it itemizes the work in it (the Go worker
// for a moment after taking a job, the Rust worker always). The admin's own
// jobs claim such slots first, so they are not charged twice; what is left
// is of unknown type and holds every type's per-worker limit.
func TestExecutorCapacityCountsUnitemizedSlots(t *testing.T) {
	t.Parallel()

	pluginSvc, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pluginSvc.Shutdown()

	const workerID = "worker-a"
	pluginSvc.registry.UpsertFromHello(&plugin_pb.WorkerHello{
		WorkerId: workerID,
		Capabilities: []*plugin_pb.JobTypeCapability{
			{JobType: "vacuum", CanExecute: true, MaxExecutionConcurrency: 4},
		},
	})
	executor := func() *WorkerSession {
		session, ok := pluginSvc.registry.Get(workerID)
		if !ok {
			t.Fatalf("worker missing")
		}
		return session
	}
	oneUsed := &plugin_pb.WorkerHeartbeat{WorkerId: workerID, ExecutionSlotsUsed: 1, ExecutionSlotsTotal: 4}
	pluginSvc.registry.UpdateHeartbeat(workerID, oneUsed)
	if pluginSvc.executorHasCapacity(executor(), "vacuum", schedulerPolicy{PerWorkerConcurrency: 1}) {
		t.Fatalf("a slot the worker counts but has not itemized may hold vacuum work")
	}

	policy := schedulerPolicy{PerWorkerConcurrency: 2}
	pluginSvc.registry.UpdateHeartbeat(workerID, &plugin_pb.WorkerHeartbeat{WorkerId: workerID, ExecutionSlotsTotal: 4})
	_, release, err := pluginSvc.reserveExecutorByID("vacuum", policy, workerID, "vacuum-1")
	if err != nil {
		t.Fatalf("reserve vacuum-1: %v", err)
	}
	defer release()
	pluginSvc.registry.UpdateHeartbeat(workerID, oneUsed)
	if got := pluginSvc.executorWorkerLoad(executor()); got != 1 {
		t.Fatalf("worker load = %d, want 1: the unitemized slot is the reserved job", got)
	}
	if !pluginSvc.executorHasCapacity(executor(), "vacuum", policy) {
		t.Fatalf("one vacuum job in flight leaves room for a second at per-worker 2")
	}
}

// A job homed on a replica-host worker stays pinned there while its home
// queue is short, so a free remote worker cannot take it. When the home is
// held by work the lane cannot free, the run gives the job up after the
// orphan grace instead of waiting out the window beside the free worker.
func TestDispatchGivesUpPinnedJobsWhoseHomeIsHeldByForeignWork(t *testing.T) {
	t.Parallel()

	pluginSvc, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pluginSvc.Shutdown()

	registerECWorkers(t, pluginSvc, map[string]string{
		"worker-local":  "10.0.0.1:9334",
		"worker-remote": "10.0.0.2:9334",
	})
	pluginSvc.registry.UpdateHeartbeat("worker-local", &plugin_pb.WorkerHeartbeat{
		WorkerId: "worker-local", ExecutionSlotsUsed: 1, ExecutionSlotsTotal: 1,
		RunningWork: []*plugin_pb.RunningWork{{
			WorkId: "vac-1", Kind: plugin_pb.WorkKind_WORK_KIND_EXECUTION, JobType: "vacuum",
		}},
	})
	pluginSvc.registry.UpdateHeartbeat("worker-remote", &plugin_pb.WorkerHeartbeat{
		WorkerId: "worker-remote", ExecutionSlotsUsed: 0, ExecutionSlotsTotal: 1,
	})

	window, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	policy := schedulerPolicy{
		ExecutionConcurrency:   1,
		PerWorkerConcurrency:   1,
		LocalQueuePin:          5,
		ExecutionTimeout:       time.Hour,
		ExecutorReserveBackoff: time.Millisecond,
		OrphanRehomeGrace:      50 * time.Millisecond,
	}
	proposals := []*plugin_pb.JobProposal{{
		ProposalId: "p1", JobType: "erasure_coding", DedupeKey: "k1",
		Parameters: replicaJob("j1", "10.0.0.1:8080").Parameters,
	}}

	started := time.Now()
	success, errCount, canceled := pluginSvc.dispatchScheduledProposals(window, "erasure_coding", proposals, &plugin_pb.ClusterContext{}, policy)
	if elapsed := time.Since(started); elapsed < policy.OrphanRehomeGrace || elapsed > 4*time.Second {
		t.Fatalf("dispatch took %v, want about the %v grace", elapsed, policy.OrphanRehomeGrace)
	}
	if success != 0 || errCount != 0 || canceled != 1 {
		t.Fatalf("unexpected dispatch counts: success=%d errors=%d canceled=%d", success, errCount, canceled)
	}
	activity := findActivity(pluginSvc.ListActivities("erasure_coding", 100), "admin_scheduler", "skipped_no_executor")
	want := "no executor capacity for erasure_coding: worker worker-local has 1/1 slots used by vacuum this admin did not dispatch"
	if activity == nil || activity.Message != want {
		t.Fatalf("expected %q, got %+v", want, activity)
	}
}

// A job the worker turned away is released and queued again under the same
// id; its new reservation replaces what the first one left behind, so the
// running job is one slot.
func TestExecutorCapacityCountsARedispatchedJobOnce(t *testing.T) {
	t.Parallel()

	pluginSvc, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pluginSvc.Shutdown()

	const workerID = "worker-a"
	pluginSvc.registry.UpsertFromHello(&plugin_pb.WorkerHello{
		WorkerId: workerID,
		Capabilities: []*plugin_pb.JobTypeCapability{
			{JobType: "vacuum", CanExecute: true, MaxExecutionConcurrency: 4},
		},
	})
	policy := schedulerPolicy{PerWorkerConcurrency: 2}
	_, rejected, err := pluginSvc.reserveExecutorByID("vacuum", policy, workerID, "vacuum-1")
	if err != nil {
		t.Fatalf("reserve vacuum-1: %v", err)
	}
	rejected()
	_, release, err := pluginSvc.reserveExecutorByID("vacuum", policy, workerID, "vacuum-1")
	if err != nil {
		t.Fatalf("reserve vacuum-1 again: %v", err)
	}
	defer release()

	pluginSvc.registry.UpdateHeartbeat(workerID, &plugin_pb.WorkerHeartbeat{
		WorkerId: workerID, ExecutionSlotsUsed: 1, ExecutionSlotsTotal: 4,
		RunningWork: []*plugin_pb.RunningWork{{
			WorkId: "vacuum-1", Kind: plugin_pb.WorkKind_WORK_KIND_EXECUTION, JobType: "vacuum",
		}},
	})
	executor, _ := pluginSvc.registry.Get(workerID)
	if got := pluginSvc.executorWorkerLoad(executor); got != 1 {
		t.Fatalf("worker load = %d, want 1", got)
	}
	if !pluginSvc.executorHasCapacity(executor, "vacuum", policy) {
		t.Fatalf("one vacuum job in flight leaves room for a second at per-worker 2")
	}
}

// A count-only heartbeat sent after a job completed no longer holds that
// job: a slot it still counts belongs to other work, of unknown type.
func TestExecutorCapacityStopsCreditingACompletedJobAfterTheNextHeartbeat(t *testing.T) {
	t.Parallel()

	pluginSvc, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pluginSvc.Shutdown()

	const workerID = "worker-a"
	pluginSvc.registry.UpsertFromHello(&plugin_pb.WorkerHello{
		WorkerId: workerID,
		Capabilities: []*plugin_pb.JobTypeCapability{
			{JobType: "vacuum", CanExecute: true, MaxExecutionConcurrency: 4},
			{JobType: "balance", CanExecute: true, MaxExecutionConcurrency: 4},
		},
	})
	policy := schedulerPolicy{PerWorkerConcurrency: 1}
	oneUsed := &plugin_pb.WorkerHeartbeat{WorkerId: workerID, ExecutionSlotsUsed: 1, ExecutionSlotsTotal: 4}
	_, release, err := pluginSvc.reserveExecutorByID("vacuum", policy, workerID, "vacuum-1")
	if err != nil {
		t.Fatalf("reserve vacuum-1: %v", err)
	}
	pluginSvc.registry.UpdateHeartbeat(workerID, oneUsed)
	release()

	executor, _ := pluginSvc.registry.Get(workerID)
	if !pluginSvc.executorHasCapacity(executor, "balance", policy) {
		t.Fatalf("the counted slot is still vacuum-1's echo, not balance work")
	}
	pluginSvc.registry.UpdateHeartbeat(workerID, oneUsed)
	executor, _ = pluginSvc.registry.Get(workerID)
	if pluginSvc.executorHasCapacity(executor, "balance", policy) {
		t.Fatalf("a slot counted after vacuum-1 completed may hold balance work")
	}
}

// A count-only heartbeat taken before a reservation cannot count that job,
// so the slot it counted is still unknown work rather than the new job's.
// A reconnect's hello keeps that heartbeat and does not change this.
func TestExecutorCapacityKeepsSlotsCountedBeforeAReservation(t *testing.T) {
	t.Parallel()

	pluginSvc, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pluginSvc.Shutdown()

	const workerID = "worker-a"
	hello := &plugin_pb.WorkerHello{
		WorkerId: workerID,
		Capabilities: []*plugin_pb.JobTypeCapability{
			{JobType: "vacuum", CanExecute: true, MaxExecutionConcurrency: 4},
		},
	}
	pluginSvc.registry.UpsertFromHello(hello)
	policy := schedulerPolicy{PerWorkerConcurrency: 2}
	oneUsed := &plugin_pb.WorkerHeartbeat{WorkerId: workerID, ExecutionSlotsUsed: 1, ExecutionSlotsTotal: 4}
	pluginSvc.registry.UpdateHeartbeat(workerID, oneUsed)
	if _, _, err := pluginSvc.reserveExecutorByID("vacuum", policy, workerID, "vacuum-1"); err != nil {
		t.Fatalf("reserve vacuum-1: %v", err)
	}
	pluginSvc.registry.UpsertFromHello(hello)

	executor, _ := pluginSvc.registry.Get(workerID)
	if pluginSvc.executorHasCapacity(executor, "vacuum", policy) {
		t.Fatalf("the slot counted before vacuum-1 was reserved may hold vacuum work")
	}
	pluginSvc.registry.UpdateHeartbeat(workerID, oneUsed)
	executor, _ = pluginSvc.registry.Get(workerID)
	if !pluginSvc.executorHasCapacity(executor, "vacuum", policy) {
		t.Fatalf("a heartbeat after the reservation counts vacuum-1 itself")
	}
}

// A job that completes before any heartbeat counts it has no echo: the slot
// the last heartbeat counted is still unknown work, not the job's.
func TestExecutorCapacityKeepsSlotsCountedBeforeACompletedJob(t *testing.T) {
	t.Parallel()

	pluginSvc, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pluginSvc.Shutdown()

	const workerID = "worker-a"
	pluginSvc.registry.UpsertFromHello(&plugin_pb.WorkerHello{
		WorkerId: workerID,
		Capabilities: []*plugin_pb.JobTypeCapability{
			{JobType: "vacuum", CanExecute: true, MaxExecutionConcurrency: 4},
			{JobType: "balance", CanExecute: true, MaxExecutionConcurrency: 4},
		},
	})
	oneUsed := &plugin_pb.WorkerHeartbeat{WorkerId: workerID, ExecutionSlotsUsed: 1, ExecutionSlotsTotal: 4}
	pluginSvc.registry.UpdateHeartbeat(workerID, oneUsed)
	_, release, err := pluginSvc.reserveExecutorByID("vacuum", schedulerPolicy{PerWorkerConcurrency: 2}, workerID, "vacuum-1")
	if err != nil {
		t.Fatalf("reserve vacuum-1: %v", err)
	}
	release()

	executor, _ := pluginSvc.registry.Get(workerID)
	if pluginSvc.executorHasCapacity(executor, "balance", schedulerPolicy{PerWorkerConcurrency: 1}) {
		t.Fatalf("the slot counted before vacuum-1 was reserved may hold balance work")
	}
}

// The reason a dispatch gives up names every holder of the worker's slots,
// the unitemized ones included.
func TestCapacityReasonNamesUnitemizedSlots(t *testing.T) {
	t.Parallel()

	pluginSvc, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pluginSvc.Shutdown()

	const workerID = "worker-a"
	pluginSvc.registry.UpsertFromHello(&plugin_pb.WorkerHello{
		WorkerId: workerID,
		Capabilities: []*plugin_pb.JobTypeCapability{
			{JobType: "admin_script", CanExecute: true, MaxExecutionConcurrency: 2},
		},
	})
	pluginSvc.registry.UpdateHeartbeat(workerID, &plugin_pb.WorkerHeartbeat{
		WorkerId: workerID, ExecutionSlotsUsed: 2, ExecutionSlotsTotal: 2,
		RunningWork: []*plugin_pb.RunningWork{{
			WorkId: "lc-1", Kind: plugin_pb.WorkKind_WORK_KIND_EXECUTION, JobType: "s3_lifecycle",
		}},
	})

	wait, reason := pluginSvc.executorCapacityOutlook("admin_script", schedulerPolicy{PerWorkerConcurrency: 2}, 0)
	want := "no executor capacity for admin_script: worker worker-a has 2/2 slots used by s3_lifecycle, unitemized work this admin did not dispatch"
	if wait || reason != want {
		t.Fatalf("outlook = (%v, %q), want (false, %q)", wait, reason, want)
	}
}
