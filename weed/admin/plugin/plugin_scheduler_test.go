package plugin

import (
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

	executor1, release1, err := pluginSvc.reserveScheduledExecutor("balance", policy)
	if err != nil {
		t.Fatalf("reserve executor 1: %v", err)
	}
	defer release1()

	executor2, release2, err := pluginSvc.reserveScheduledExecutor("balance", policy)
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
	_, _, err = pluginSvc.reserveScheduledExecutor("missing-job-type", policy)
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

	_, release1, err := pluginSvc.reserveScheduledExecutor("balance", policy)
	if err != nil {
		t.Fatalf("reserve executor 1: %v", err)
	}
	defer release1()

	type reserveResult struct {
		err error
	}
	secondReserveCh := make(chan reserveResult, 1)
	go func() {
		_, release2, reserveErr := pluginSvc.reserveScheduledExecutor("balance", policy)
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
	if state.DetectionIntervalSeconds != 45 {
		t.Fatalf("unexpected detection interval: got=%d", state.DetectionIntervalSeconds)
	}
	if state.DetectionTimeoutSeconds != 30 {
		t.Fatalf("unexpected detection timeout: got=%d", state.DetectionTimeoutSeconds)
	}
	if state.ExecutionTimeoutSeconds != 90 {
		t.Fatalf("unexpected execution timeout: got=%d", state.ExecutionTimeoutSeconds)
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
