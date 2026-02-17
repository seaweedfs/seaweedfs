package plugin

import (
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

func TestLoadSchedulerPolicyUsesAdminRuntime(t *testing.T) {
	t.Parallel()

	runtime, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer runtime.Shutdown()

	err = runtime.SaveJobTypeConfig(&plugin_pb.PersistedJobTypeConfig{
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

	policy, enabled, err := runtime.loadSchedulerPolicy("vacuum")
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

	runtime, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer runtime.Shutdown()

	err = runtime.store.SaveDescriptor("ec", &plugin_pb.JobTypeDescriptor{
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

	policy, enabled, err := runtime.loadSchedulerPolicy("ec")
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

	runtime, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer runtime.Shutdown()

	runtime.registry.UpsertFromHello(&plugin_pb.WorkerHello{
		WorkerId: "worker-a",
		Capabilities: []*plugin_pb.JobTypeCapability{
			{JobType: "balance", CanExecute: true, MaxExecutionConcurrency: 4},
		},
	})
	runtime.registry.UpsertFromHello(&plugin_pb.WorkerHello{
		WorkerId: "worker-b",
		Capabilities: []*plugin_pb.JobTypeCapability{
			{JobType: "balance", CanExecute: true, MaxExecutionConcurrency: 2},
		},
	})

	policy := schedulerPolicy{
		PerWorkerConcurrency:   1,
		ExecutorReserveBackoff: time.Millisecond,
	}

	limiters := make(map[string]chan struct{})
	var limiterMu sync.Mutex

	executor1, release1, err := runtime.reserveScheduledExecutor("balance", limiters, &limiterMu, policy)
	if err != nil {
		t.Fatalf("reserve executor 1: %v", err)
	}
	defer release1()

	executor2, release2, err := runtime.reserveScheduledExecutor("balance", limiters, &limiterMu, policy)
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

	runtime, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer runtime.Shutdown()

	proposals := []*plugin_pb.JobProposal{
		{ProposalId: "p1", DedupeKey: "d1"},
		{ProposalId: "p2", DedupeKey: "d1"}, // same dedupe key
		{ProposalId: "p3", DedupeKey: "d3"},
		{ProposalId: "p3"}, // fallback dedupe by proposal id
		{ProposalId: "p4"},
		{ProposalId: "p4"}, // same proposal id, no dedupe key
	}

	filtered := runtime.filterScheduledProposals("vacuum", proposals, time.Hour)
	if len(filtered) != 4 {
		t.Fatalf("unexpected filtered size: got=%d want=4", len(filtered))
	}

	filtered2 := runtime.filterScheduledProposals("vacuum", proposals, time.Hour)
	if len(filtered2) != 0 {
		t.Fatalf("expected second run to be fully deduped, got=%d", len(filtered2))
	}
}

func TestReserveScheduledExecutorTimesOutWhenNoExecutor(t *testing.T) {
	t.Parallel()

	runtime, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer runtime.Shutdown()

	policy := schedulerPolicy{
		ExecutionTimeout:       30 * time.Millisecond,
		ExecutorReserveBackoff: 5 * time.Millisecond,
		PerWorkerConcurrency:   1,
	}

	start := time.Now()
	_, _, err = runtime.reserveScheduledExecutor("missing-job-type", map[string]chan struct{}{}, &sync.Mutex{}, policy)
	if err == nil {
		t.Fatalf("expected reservation timeout error")
	}
	if time.Since(start) < 20*time.Millisecond {
		t.Fatalf("reservation returned too early: duration=%v", time.Since(start))
	}
}

func TestListSchedulerStatesIncludesPolicyAndRuntimeState(t *testing.T) {
	t.Parallel()

	runtime, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer runtime.Shutdown()

	const jobType = "vacuum"
	err = runtime.SaveJobTypeConfig(&plugin_pb.PersistedJobTypeConfig{
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

	runtime.registry.UpsertFromHello(&plugin_pb.WorkerHello{
		WorkerId: "worker-a",
		Capabilities: []*plugin_pb.JobTypeCapability{
			{JobType: jobType, CanDetect: true, CanExecute: true},
		},
	})

	nextDetectionAt := time.Now().UTC().Add(2 * time.Minute).Round(time.Second)
	runtime.schedulerMu.Lock()
	runtime.nextDetectionAt[jobType] = nextDetectionAt
	runtime.detectionInFlight[jobType] = true
	runtime.schedulerMu.Unlock()

	states, err := runtime.ListSchedulerStates()
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

	runtime, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer runtime.Shutdown()

	const jobType = "balance"
	runtime.registry.UpsertFromHello(&plugin_pb.WorkerHello{
		WorkerId: "worker-b",
		Capabilities: []*plugin_pb.JobTypeCapability{
			{JobType: jobType, CanDetect: true, CanExecute: true},
		},
	})

	states, err := runtime.ListSchedulerStates()
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
