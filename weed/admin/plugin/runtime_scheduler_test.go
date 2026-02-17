package plugin

import (
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

func TestLoadSchedulerPolicyUsesAdminRuntime(t *testing.T) {
	t.Parallel()

	runtime, err := NewRuntime(RuntimeOptions{})
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
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

	runtime, err := NewRuntime(RuntimeOptions{})
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
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

	runtime, err := NewRuntime(RuntimeOptions{})
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
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
