package plugin

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"github.com/stretchr/testify/require"
)

func TestSchedulerCooldownUsesSavedPolicyAndHistory(t *testing.T) {
	p, err := New(Options{})
	require.NoError(t, err)
	defer p.Shutdown()
	const jobType = "vacuum"
	p.registry.UpsertFromHello(&plugin_pb.WorkerHello{
		WorkerId: "worker-a", Capabilities: []*plugin_pb.JobTypeCapability{{JobType: jobType, CanDetect: true}},
	})
	require.NoError(t, p.store.SaveDescriptor(jobType, &plugin_pb.JobTypeDescriptor{
		JobType: jobType, DetectionCooldownConfigField: "cooldown",
		WorkerDefaultValues: map[string]*plugin_pb.ConfigValue{"cooldown": {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 36000}}},
	}))
	last := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	require.NoError(t, p.store.AppendRunRecord(jobType, &JobRunRecord{Outcome: RunOutcomeSuccess, CompletedAt: &last}))
	p.markJobTypeInFlight(jobType)
	for _, seconds := range []int64{36000, 600, 0, 7200} {
		require.NoError(t, p.SaveJobTypeConfig(&plugin_pb.PersistedJobTypeConfig{
			JobType: jobType, AdminRuntime: &plugin_pb.AdminRuntimeConfig{Enabled: true, DetectionIntervalMinutes: 10},
			WorkerConfigValues: map[string]*plugin_pb.ConfigValue{"cooldown": {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: seconds}}},
		}))
		for _, status := range []SchedulerStatus{p.GetLaneSchedulerStatus(LaneDefault), p.GetSchedulerStatus()} {
			require.Len(t, status.JobTypes, 1)
			job := status.JobTypes[0]
			require.Empty(t, job.CooldownError)
			require.Equal(t, seconds, job.CooldownSeconds)
			require.Equal(t, last, *job.LastSuccessfulRunAt)
			if seconds == 0 {
				require.Nil(t, job.DetectionEligibleAt)
			} else {
				require.Equal(t, last.Add(time.Duration(seconds)*time.Second), *job.DetectionEligibleAt)
			}
			var cooldown, inFlight bool
			for _, wait := range status.Waiting {
				cooldown = cooldown || wait.Reason == "cooldown"
				inFlight = inFlight || wait.Reason == "detection_in_flight"
			}
			require.Equal(t, seconds > 3600, cooldown)
			require.True(t, inFlight, "cooldown must not hide another blocker")
		}
	}
	require.NoError(t, p.store.SaveJobTypeConfig(&plugin_pb.PersistedJobTypeConfig{JobType: jobType}))
	job := SchedulerJobTypeStatus{JobType: jobType}
	p.populateSchedulerCooldown(&job)
	require.Equal(t, int64(36000), job.CooldownSeconds, "missing saved value uses descriptor default")
}
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
			DetectionIntervalMinutes: 30,
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
