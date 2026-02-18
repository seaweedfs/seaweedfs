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
