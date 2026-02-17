package plugin

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestPluginLoadsPersistedMonitorStateOnStart(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	store, err := NewConfigStore(dataDir)
	if err != nil {
		t.Fatalf("NewConfigStore: %v", err)
	}

	seedJobs := []TrackedJob{
		{
			JobID:     "job-seeded",
			JobType:   "vacuum",
			State:     "running",
			CreatedAt: time.Now().UTC().Add(-2 * time.Minute),
			UpdatedAt: time.Now().UTC().Add(-1 * time.Minute),
		},
	}
	seedActivities := []JobActivity{
		{
			JobID:      "job-seeded",
			JobType:    "vacuum",
			Source:     "worker_progress",
			Message:    "seeded",
			OccurredAt: time.Now().UTC().Add(-30 * time.Second),
		},
	}

	if err := store.SaveTrackedJobs(seedJobs); err != nil {
		t.Fatalf("SaveTrackedJobs: %v", err)
	}
	if err := store.SaveActivities(seedActivities); err != nil {
		t.Fatalf("SaveActivities: %v", err)
	}

	pluginSvc, err := New(Options{DataDir: dataDir})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pluginSvc.Shutdown()

	gotJobs := pluginSvc.ListTrackedJobs("", "", 0)
	if len(gotJobs) != 1 || gotJobs[0].JobID != "job-seeded" {
		t.Fatalf("unexpected loaded jobs: %+v", gotJobs)
	}

	gotActivities := pluginSvc.ListActivities("", 0)
	if len(gotActivities) != 1 || gotActivities[0].Message != "seeded" {
		t.Fatalf("unexpected loaded activities: %+v", gotActivities)
	}
}

func TestPluginPersistsMonitorStateAfterJobUpdates(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	pluginSvc, err := New(Options{DataDir: dataDir})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pluginSvc.Shutdown()

	job := &plugin_pb.JobSpec{
		JobId:   "job-persist",
		JobType: "vacuum",
		Summary: "persist test",
	}
	pluginSvc.trackExecutionStart("req-persist", "worker-a", job, 1)

	pluginSvc.trackExecutionCompletion(&plugin_pb.JobCompleted{
		RequestId:   "req-persist",
		JobId:       "job-persist",
		JobType:     "vacuum",
		Success:     true,
		Result:      &plugin_pb.JobResult{Summary: "done"},
		CompletedAt: timestamppb.New(time.Now().UTC()),
	})

	store, err := NewConfigStore(dataDir)
	if err != nil {
		t.Fatalf("NewConfigStore: %v", err)
	}

	trackedJobs, err := store.LoadTrackedJobs()
	if err != nil {
		t.Fatalf("LoadTrackedJobs: %v", err)
	}
	if len(trackedJobs) == 0 {
		t.Fatalf("expected persisted tracked jobs")
	}

	found := false
	for _, tracked := range trackedJobs {
		if tracked.JobID == "job-persist" {
			found = true
			if tracked.State == "" {
				t.Fatalf("persisted job state should not be empty")
			}
		}
	}
	if !found {
		t.Fatalf("persisted tracked jobs missing job-persist")
	}

	activities, err := store.LoadActivities()
	if err != nil {
		t.Fatalf("LoadActivities: %v", err)
	}
	if len(activities) == 0 {
		t.Fatalf("expected persisted activities")
	}
}

func TestHandleJobProgressUpdateCarriesWorkerIDInActivities(t *testing.T) {
	t.Parallel()

	pluginSvc, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pluginSvc.Shutdown()

	job := &plugin_pb.JobSpec{
		JobId:   "job-progress-worker",
		JobType: "vacuum",
	}
	pluginSvc.trackExecutionStart("req-progress-worker", "worker-a", job, 1)

	pluginSvc.handleJobProgressUpdate(&plugin_pb.JobProgressUpdate{
		RequestId:       "req-progress-worker",
		JobId:           "job-progress-worker",
		JobType:         "vacuum",
		State:           plugin_pb.JobState_JOB_STATE_RUNNING,
		ProgressPercent: 42.0,
		Stage:           "scan",
		Message:         "in progress",
		Activities: []*plugin_pb.ActivityEvent{
			{
				Source:  plugin_pb.ActivitySource_ACTIVITY_SOURCE_EXECUTOR,
				Message: "volume scanned",
				Stage:   "scan",
			},
		},
	})

	activities := pluginSvc.ListActivities("vacuum", 0)
	if len(activities) == 0 {
		t.Fatalf("expected activity entries")
	}

	foundProgress := false
	foundEvent := false
	for _, activity := range activities {
		if activity.Source == "worker_progress" && activity.Message == "in progress" {
			foundProgress = true
			if activity.WorkerID != "worker-a" {
				t.Fatalf("worker_progress activity worker mismatch: got=%q want=%q", activity.WorkerID, "worker-a")
			}
		}
		if activity.Message == "volume scanned" {
			foundEvent = true
			if activity.WorkerID != "worker-a" {
				t.Fatalf("worker event worker mismatch: got=%q want=%q", activity.WorkerID, "worker-a")
			}
		}
	}

	if !foundProgress {
		t.Fatalf("expected worker_progress activity")
	}
	if !foundEvent {
		t.Fatalf("expected worker activity event")
	}
}

func TestHandleJobCompletedCarriesWorkerIDInActivitiesAndRunHistory(t *testing.T) {
	t.Parallel()

	pluginSvc, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pluginSvc.Shutdown()

	job := &plugin_pb.JobSpec{
		JobId:   "job-complete-worker",
		JobType: "vacuum",
	}
	pluginSvc.trackExecutionStart("req-complete-worker", "worker-b", job, 1)

	pluginSvc.handleJobCompleted(&plugin_pb.JobCompleted{
		RequestId: "req-complete-worker",
		JobId:     "job-complete-worker",
		JobType:   "vacuum",
		Success:   true,
		Activities: []*plugin_pb.ActivityEvent{
			{
				Source:  plugin_pb.ActivitySource_ACTIVITY_SOURCE_EXECUTOR,
				Message: "finalizer done",
				Stage:   "finalize",
			},
		},
		CompletedAt: timestamppb.Now(),
	})

	activities := pluginSvc.ListActivities("vacuum", 0)
	foundWorkerEvent := false
	for _, activity := range activities {
		if activity.Message == "finalizer done" {
			foundWorkerEvent = true
			if activity.WorkerID != "worker-b" {
				t.Fatalf("worker completion event worker mismatch: got=%q want=%q", activity.WorkerID, "worker-b")
			}
		}
	}
	if !foundWorkerEvent {
		t.Fatalf("expected completion worker event activity")
	}

	history, err := pluginSvc.LoadRunHistory("vacuum")
	if err != nil {
		t.Fatalf("LoadRunHistory: %v", err)
	}
	if history == nil || len(history.SuccessfulRuns) == 0 {
		t.Fatalf("expected successful run history entry")
	}
	if history.SuccessfulRuns[0].WorkerID != "worker-b" {
		t.Fatalf("run history worker mismatch: got=%q want=%q", history.SuccessfulRuns[0].WorkerID, "worker-b")
	}
}
