package plugin

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

func TestGetSchedulerStatusIncludesInProcessJobs(t *testing.T) {
	pluginSvc, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pluginSvc.Shutdown()

	pluginSvc.trackExecutionStart("req-1", "worker-a", &plugin_pb.JobSpec{
		JobId:   "job-1",
		JobType: "vacuum",
	}, 1)

	status := pluginSvc.GetSchedulerStatus()
	if len(status.InProcessJobs) != 1 {
		t.Fatalf("expected one in-process job, got %d", len(status.InProcessJobs))
	}
	if status.InProcessJobs[0].JobID != "job-1" {
		t.Fatalf("unexpected job id: %s", status.InProcessJobs[0].JobID)
	}
}

func TestGetSchedulerStatusIncludesLastDetectionCount(t *testing.T) {
	pluginSvc, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pluginSvc.Shutdown()

	const jobType = "vacuum"
	pluginSvc.registry.UpsertFromHello(&plugin_pb.WorkerHello{
		WorkerId: "worker-a",
		Capabilities: []*plugin_pb.JobTypeCapability{
			{JobType: jobType, CanDetect: true},
		},
	})

	pluginSvc.recordSchedulerDetectionSuccess(jobType, 3)

	status := pluginSvc.GetSchedulerStatus()
	found := false
	for _, jt := range status.JobTypes {
		if jt.JobType != jobType {
			continue
		}
		found = true
		if jt.LastDetectedCount != 3 {
			t.Fatalf("unexpected last detected count: got=%d want=3", jt.LastDetectedCount)
		}
		if jt.LastDetectedAt == nil {
			t.Fatalf("expected last detected at to be set")
		}
	}
	if !found {
		t.Fatalf("expected job type status for %s", jobType)
	}
}
