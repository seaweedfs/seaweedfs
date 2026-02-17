package plugin

import (
	"reflect"
	"testing"

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
