package plugin

import (
	"context"
	"testing"
	"time"

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

func TestGetLaneSchedulerStatusShowsActiveConcurrentLaneWork(t *testing.T) {
	clusterContextStarted := make(chan struct{})
	releaseClusterContext := make(chan struct{})

	// Create the Plugin without a ClusterContextProvider so no background
	// scheduler goroutines are started; they would race with the direct
	// runJobTypeIteration call below.
	pluginSvc, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pluginSvc.Shutdown()

	// Set the provider after construction so runJobTypeIteration can use it.
	pluginSvc.clusterContextProvider = func(context.Context) (*plugin_pb.ClusterContext, error) {
		close(clusterContextStarted)
		<-releaseClusterContext
		return nil, context.Canceled
	}

	const jobType = "s3_lifecycle"
	err = pluginSvc.SaveJobTypeConfig(&plugin_pb.PersistedJobTypeConfig{
		JobType: jobType,
		AdminRuntime: &plugin_pb.AdminRuntimeConfig{
			Enabled:                  true,
			DetectionIntervalSeconds: 30,
			DetectionTimeoutSeconds:  15,
		},
	})
	if err != nil {
		t.Fatalf("SaveJobTypeConfig: %v", err)
	}

	policy, enabled, err := pluginSvc.loadSchedulerPolicy(jobType)
	if err != nil {
		t.Fatalf("loadSchedulerPolicy: %v", err)
	}
	if !enabled {
		t.Fatalf("expected enabled policy")
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		pluginSvc.runJobTypeIteration(jobType, policy)
	}()

	select {
	case <-clusterContextStarted:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for job type iteration to start")
	}

	var laneStatus SchedulerStatus
	var aggregateStatus SchedulerStatus
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		laneStatus = pluginSvc.GetLaneSchedulerStatus(LaneLifecycle)
		aggregateStatus = pluginSvc.GetSchedulerStatus()
		if laneStatus.CurrentJobType == jobType && laneStatus.CurrentPhase == "detecting" &&
			aggregateStatus.CurrentJobType == jobType && aggregateStatus.CurrentPhase == "detecting" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if laneStatus.CurrentJobType != jobType || laneStatus.CurrentPhase != "detecting" {
		t.Fatalf("unexpected lane status while work is active: job=%q phase=%q", laneStatus.CurrentJobType, laneStatus.CurrentPhase)
	}
	if aggregateStatus.CurrentJobType != jobType || aggregateStatus.CurrentPhase != "detecting" {
		t.Fatalf("unexpected aggregate status while work is active: job=%q phase=%q", aggregateStatus.CurrentJobType, aggregateStatus.CurrentPhase)
	}

	close(releaseClusterContext)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for job type iteration to finish")
	}
}
