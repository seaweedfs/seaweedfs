package plugin

import (
	"reflect"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

func TestRegistryPickDetectorPrefersMoreFreeSlots(t *testing.T) {
	t.Parallel()

	r := NewRegistry()

	r.UpsertFromHello(&plugin_pb.WorkerHello{
		WorkerId: "worker-a",
		Capabilities: []*plugin_pb.JobTypeCapability{
			{JobType: "vacuum", CanDetect: true, CanExecute: true, MaxDetectionConcurrency: 2, MaxExecutionConcurrency: 2},
		},
	})
	r.UpsertFromHello(&plugin_pb.WorkerHello{
		WorkerId: "worker-b",
		Capabilities: []*plugin_pb.JobTypeCapability{
			{JobType: "vacuum", CanDetect: true, CanExecute: true, MaxDetectionConcurrency: 4, MaxExecutionConcurrency: 4},
		},
	})

	r.UpdateHeartbeat("worker-a", &plugin_pb.WorkerHeartbeat{
		WorkerId:            "worker-a",
		DetectionSlotsUsed:  1,
		DetectionSlotsTotal: 2,
	})
	r.UpdateHeartbeat("worker-b", &plugin_pb.WorkerHeartbeat{
		WorkerId:            "worker-b",
		DetectionSlotsUsed:  1,
		DetectionSlotsTotal: 4,
	})

	picked, err := r.PickDetector("vacuum")
	if err != nil {
		t.Fatalf("PickDetector: %v", err)
	}
	if picked.WorkerID != "worker-b" {
		t.Fatalf("unexpected detector picked: got %s want worker-b", picked.WorkerID)
	}
}

func TestRegistryPickExecutorAllowsSameWorker(t *testing.T) {
	t.Parallel()

	r := NewRegistry()
	r.UpsertFromHello(&plugin_pb.WorkerHello{
		WorkerId: "worker-x",
		Capabilities: []*plugin_pb.JobTypeCapability{
			{JobType: "balance", CanDetect: true, CanExecute: true, MaxDetectionConcurrency: 1, MaxExecutionConcurrency: 1},
		},
	})

	detector, err := r.PickDetector("balance")
	if err != nil {
		t.Fatalf("PickDetector: %v", err)
	}
	executor, err := r.PickExecutor("balance")
	if err != nil {
		t.Fatalf("PickExecutor: %v", err)
	}

	if detector.WorkerID != "worker-x" || executor.WorkerID != "worker-x" {
		t.Fatalf("expected same worker for detect/execute, got detector=%s executor=%s", detector.WorkerID, executor.WorkerID)
	}
}

func TestRegistryDetectableJobTypes(t *testing.T) {
	t.Parallel()

	r := NewRegistry()
	r.UpsertFromHello(&plugin_pb.WorkerHello{
		WorkerId: "worker-a",
		Capabilities: []*plugin_pb.JobTypeCapability{
			{JobType: "vacuum", CanDetect: true, CanExecute: true},
			{JobType: "balance", CanDetect: false, CanExecute: true},
		},
	})
	r.UpsertFromHello(&plugin_pb.WorkerHello{
		WorkerId: "worker-b",
		Capabilities: []*plugin_pb.JobTypeCapability{
			{JobType: "ec", CanDetect: true, CanExecute: false},
			{JobType: "vacuum", CanDetect: true, CanExecute: false},
		},
	})

	got := r.DetectableJobTypes()
	want := []string{"ec", "vacuum"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected detectable job types: got=%v want=%v", got, want)
	}
}

func TestRegistryJobTypes(t *testing.T) {
	t.Parallel()

	r := NewRegistry()
	r.UpsertFromHello(&plugin_pb.WorkerHello{
		WorkerId: "worker-a",
		Capabilities: []*plugin_pb.JobTypeCapability{
			{JobType: "vacuum", CanDetect: true},
			{JobType: "balance", CanExecute: true},
		},
	})
	r.UpsertFromHello(&plugin_pb.WorkerHello{
		WorkerId: "worker-b",
		Capabilities: []*plugin_pb.JobTypeCapability{
			{JobType: "ec", CanDetect: true},
		},
	})

	got := r.JobTypes()
	want := []string{"balance", "ec", "vacuum"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected job types: got=%v want=%v", got, want)
	}
}

func TestRegistryListExecutorsSortedBySlots(t *testing.T) {
	t.Parallel()

	r := NewRegistry()
	r.UpsertFromHello(&plugin_pb.WorkerHello{
		WorkerId: "worker-a",
		Capabilities: []*plugin_pb.JobTypeCapability{
			{JobType: "balance", CanExecute: true, MaxExecutionConcurrency: 2},
		},
	})
	r.UpsertFromHello(&plugin_pb.WorkerHello{
		WorkerId: "worker-b",
		Capabilities: []*plugin_pb.JobTypeCapability{
			{JobType: "balance", CanExecute: true, MaxExecutionConcurrency: 4},
		},
	})

	r.UpdateHeartbeat("worker-a", &plugin_pb.WorkerHeartbeat{
		WorkerId:            "worker-a",
		ExecutionSlotsUsed:  1,
		ExecutionSlotsTotal: 2,
	})
	r.UpdateHeartbeat("worker-b", &plugin_pb.WorkerHeartbeat{
		WorkerId:            "worker-b",
		ExecutionSlotsUsed:  1,
		ExecutionSlotsTotal: 4,
	})

	executors, err := r.ListExecutors("balance")
	if err != nil {
		t.Fatalf("ListExecutors: %v", err)
	}
	if len(executors) != 2 {
		t.Fatalf("unexpected candidate count: got=%d", len(executors))
	}
	if executors[0].WorkerID != "worker-b" || executors[1].WorkerID != "worker-a" {
		t.Fatalf("unexpected executor order: got=%s,%s", executors[0].WorkerID, executors[1].WorkerID)
	}
}

func TestRegistryPickExecutorRoundRobinForTopTie(t *testing.T) {
	t.Parallel()

	r := NewRegistry()
	for _, workerID := range []string{"worker-a", "worker-b", "worker-c"} {
		r.UpsertFromHello(&plugin_pb.WorkerHello{
			WorkerId: workerID,
			Capabilities: []*plugin_pb.JobTypeCapability{
				{JobType: "balance", CanExecute: true, MaxExecutionConcurrency: 1},
			},
		})
	}

	got := make([]string, 0, 6)
	for i := 0; i < 6; i++ {
		executor, err := r.PickExecutor("balance")
		if err != nil {
			t.Fatalf("PickExecutor: %v", err)
		}
		got = append(got, executor.WorkerID)
	}

	want := []string{"worker-a", "worker-b", "worker-c", "worker-a", "worker-b", "worker-c"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected pick order: got=%v want=%v", got, want)
	}
}

func TestRegistryListExecutorsRoundRobinForTopTie(t *testing.T) {
	t.Parallel()

	r := NewRegistry()
	r.UpsertFromHello(&plugin_pb.WorkerHello{
		WorkerId: "worker-a",
		Capabilities: []*plugin_pb.JobTypeCapability{
			{JobType: "balance", CanExecute: true, MaxExecutionConcurrency: 2},
		},
	})
	r.UpsertFromHello(&plugin_pb.WorkerHello{
		WorkerId: "worker-b",
		Capabilities: []*plugin_pb.JobTypeCapability{
			{JobType: "balance", CanExecute: true, MaxExecutionConcurrency: 2},
		},
	})
	r.UpsertFromHello(&plugin_pb.WorkerHello{
		WorkerId: "worker-c",
		Capabilities: []*plugin_pb.JobTypeCapability{
			{JobType: "balance", CanExecute: true, MaxExecutionConcurrency: 1},
		},
	})

	r.UpdateHeartbeat("worker-a", &plugin_pb.WorkerHeartbeat{
		WorkerId:            "worker-a",
		ExecutionSlotsUsed:  0,
		ExecutionSlotsTotal: 2,
	})
	r.UpdateHeartbeat("worker-b", &plugin_pb.WorkerHeartbeat{
		WorkerId:            "worker-b",
		ExecutionSlotsUsed:  0,
		ExecutionSlotsTotal: 2,
	})
	r.UpdateHeartbeat("worker-c", &plugin_pb.WorkerHeartbeat{
		WorkerId:            "worker-c",
		ExecutionSlotsUsed:  0,
		ExecutionSlotsTotal: 1,
	})

	firstCall, err := r.ListExecutors("balance")
	if err != nil {
		t.Fatalf("ListExecutors first call: %v", err)
	}
	secondCall, err := r.ListExecutors("balance")
	if err != nil {
		t.Fatalf("ListExecutors second call: %v", err)
	}
	thirdCall, err := r.ListExecutors("balance")
	if err != nil {
		t.Fatalf("ListExecutors third call: %v", err)
	}

	if firstCall[0].WorkerID != "worker-a" || firstCall[1].WorkerID != "worker-b" || firstCall[2].WorkerID != "worker-c" {
		t.Fatalf("unexpected first executor order: got=%s,%s,%s", firstCall[0].WorkerID, firstCall[1].WorkerID, firstCall[2].WorkerID)
	}
	if secondCall[0].WorkerID != "worker-b" || secondCall[1].WorkerID != "worker-a" || secondCall[2].WorkerID != "worker-c" {
		t.Fatalf("unexpected second executor order: got=%s,%s,%s", secondCall[0].WorkerID, secondCall[1].WorkerID, secondCall[2].WorkerID)
	}
	if thirdCall[0].WorkerID != "worker-a" || thirdCall[1].WorkerID != "worker-b" || thirdCall[2].WorkerID != "worker-c" {
		t.Fatalf("unexpected third executor order: got=%s,%s,%s", thirdCall[0].WorkerID, thirdCall[1].WorkerID, thirdCall[2].WorkerID)
	}
}

func TestRegistrySkipsStaleWorkersForSelectionAndListing(t *testing.T) {
	t.Parallel()

	r := NewRegistry()
	r.staleAfter = 2 * time.Second

	r.UpsertFromHello(&plugin_pb.WorkerHello{
		WorkerId: "worker-stale",
		Capabilities: []*plugin_pb.JobTypeCapability{
			{JobType: "vacuum", CanDetect: true, CanExecute: true},
		},
	})
	r.UpsertFromHello(&plugin_pb.WorkerHello{
		WorkerId: "worker-fresh",
		Capabilities: []*plugin_pb.JobTypeCapability{
			{JobType: "vacuum", CanDetect: true, CanExecute: true},
		},
	})

	r.mu.Lock()
	r.sessions["worker-stale"].LastSeenAt = time.Now().Add(-10 * time.Second)
	r.sessions["worker-fresh"].LastSeenAt = time.Now()
	r.mu.Unlock()

	picked, err := r.PickDetector("vacuum")
	if err != nil {
		t.Fatalf("PickDetector: %v", err)
	}
	if picked.WorkerID != "worker-fresh" {
		t.Fatalf("unexpected detector: got=%s want=worker-fresh", picked.WorkerID)
	}

	if _, ok := r.Get("worker-stale"); ok {
		t.Fatalf("expected stale worker to be hidden from Get")
	}
	if _, ok := r.Get("worker-fresh"); !ok {
		t.Fatalf("expected fresh worker from Get")
	}

	listed := r.List()
	if len(listed) != 1 || listed[0].WorkerID != "worker-fresh" {
		t.Fatalf("unexpected listed workers: %+v", listed)
	}
}

func TestRegistryReturnsNoDetectorWhenAllWorkersStale(t *testing.T) {
	t.Parallel()

	r := NewRegistry()
	r.staleAfter = 2 * time.Second

	r.UpsertFromHello(&plugin_pb.WorkerHello{
		WorkerId: "worker-a",
		Capabilities: []*plugin_pb.JobTypeCapability{
			{JobType: "vacuum", CanDetect: true},
		},
	})

	r.mu.Lock()
	r.sessions["worker-a"].LastSeenAt = time.Now().Add(-10 * time.Second)
	r.mu.Unlock()

	if _, err := r.PickDetector("vacuum"); err == nil {
		t.Fatalf("expected no detector when all workers are stale")
	}
}

func TestRegistryHasCapableWorkerWithDetectCapability(t *testing.T) {
	t.Parallel()

	r := NewRegistry()
	r.UpsertFromHello(&plugin_pb.WorkerHello{
		WorkerId: "worker-a",
		Capabilities: []*plugin_pb.JobTypeCapability{
			{JobType: "vacuum", CanDetect: true, CanExecute: false},
		},
	})

	if !r.HasCapableWorker("vacuum") {
		t.Fatalf("expected HasCapableWorker to return true for detect-capable worker")
	}
	if r.HasCapableWorker("balance") {
		t.Fatalf("expected HasCapableWorker to return false for unknown job type")
	}
}

func TestRegistryHasCapableWorkerWithExecuteCapability(t *testing.T) {
	t.Parallel()

	r := NewRegistry()
	r.UpsertFromHello(&plugin_pb.WorkerHello{
		WorkerId: "worker-a",
		Capabilities: []*plugin_pb.JobTypeCapability{
			{JobType: "vacuum", CanDetect: false, CanExecute: true},
		},
	})

	if !r.HasCapableWorker("vacuum") {
		t.Fatalf("expected HasCapableWorker to return true for execute-capable worker")
	}
}

func TestRegistryHasCapableWorkerReturnsFalseWhenNoWorkers(t *testing.T) {
	t.Parallel()

	r := NewRegistry()
	if r.HasCapableWorker("vacuum") {
		t.Fatalf("expected HasCapableWorker to return false with no workers")
	}
}

func TestRegistryHasCapableWorkerSkipsStaleWorkers(t *testing.T) {
	t.Parallel()

	r := NewRegistry()
	r.staleAfter = 2 * time.Second

	r.UpsertFromHello(&plugin_pb.WorkerHello{
		WorkerId: "worker-stale",
		Capabilities: []*plugin_pb.JobTypeCapability{
			{JobType: "vacuum", CanDetect: true, CanExecute: true},
		},
	})

	r.mu.Lock()
	r.sessions["worker-stale"].LastSeenAt = time.Now().Add(-10 * time.Second)
	r.mu.Unlock()

	if r.HasCapableWorker("vacuum") {
		t.Fatalf("expected HasCapableWorker to return false when all workers are stale")
	}
}

func TestRegistryHasCapableWorkerIgnoresNilCapability(t *testing.T) {
	t.Parallel()

	r := NewRegistry()
	r.UpsertFromHello(&plugin_pb.WorkerHello{
		WorkerId: "worker-a",
		Capabilities: []*plugin_pb.JobTypeCapability{
			{JobType: "vacuum", CanDetect: false, CanExecute: false},
		},
	})

	if r.HasCapableWorker("vacuum") {
		t.Fatalf("expected HasCapableWorker to return false when capability has no detect or execute")
	}
}

func TestListExecutorsPrefersMatchingReplicaHost(t *testing.T) {
	t.Parallel()

	r := NewRegistry()
	r.UpsertFromHello(&plugin_pb.WorkerHello{
		WorkerId: "worker-remote",
		Address:  "10.87.0.4:33646",
		Capabilities: []*plugin_pb.JobTypeCapability{
			{JobType: "erasure_coding", CanExecute: true, MaxExecutionConcurrency: 8},
		},
	})
	r.UpsertFromHello(&plugin_pb.WorkerHello{
		WorkerId: "worker-local",
		Address:  "10.87.0.3:33646",
		Capabilities: []*plugin_pb.JobTypeCapability{
			{JobType: "erasure_coding", CanExecute: true, MaxExecutionConcurrency: 1},
		},
	})
	r.UpdateHeartbeat("worker-remote", &plugin_pb.WorkerHeartbeat{
		WorkerId: "worker-remote", ExecutionSlotsUsed: 0, ExecutionSlotsTotal: 8,
	})
	r.UpdateHeartbeat("worker-local", &plugin_pb.WorkerHeartbeat{
		WorkerId: "worker-local", ExecutionSlotsUsed: 0, ExecutionSlotsTotal: 1,
	})

	// Without preference the emptier remote worker wins on slots.
	plain, err := r.ListExecutors("erasure_coding")
	if err != nil {
		t.Fatalf("ListExecutors: %v", err)
	}
	if plain[0].WorkerID != "worker-remote" {
		t.Fatalf("without preference want remote first, got %s", plain[0].WorkerID)
	}

	// Replica on :8080 of the same host as worker-local.
	pref, err := r.ListExecutors("erasure_coding", "10.87.0.3:8080")
	if err != nil {
		t.Fatalf("ListExecutors preferred: %v", err)
	}
	if pref[0].WorkerID != "worker-local" {
		t.Fatalf("preferred host want worker-local first, got %s", pref[0].WorkerID)
	}
	if pref[1].WorkerID != "worker-remote" {
		t.Fatalf("remote should still be fallback, got %s", pref[1].WorkerID)
	}
}

func TestJobReplicaHostsFromSourcesAndReplicaServers(t *testing.T) {
	t.Parallel()
	job := &plugin_pb.JobSpec{
		Parameters: map[string]*plugin_pb.ConfigValue{
			"source_server": {
				Kind: &plugin_pb.ConfigValue_StringValue{StringValue: "10.87.0.1:8080"},
			},
			"replica_servers": {
				Kind: &plugin_pb.ConfigValue_StringList{
					StringList: &plugin_pb.StringList{Values: []string{"10.87.0.1:8080", "10.87.0.2:8081"}},
				},
			},
		},
	}
	got := JobReplicaHosts(job)
	if len(got) != 2 {
		t.Fatalf("got %v", got)
	}
}
