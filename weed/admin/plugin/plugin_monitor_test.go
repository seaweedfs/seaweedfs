package plugin

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"google.golang.org/protobuf/proto"
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

func TestTrackExecutionQueuedMarksPendingState(t *testing.T) {
	t.Parallel()

	pluginSvc, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pluginSvc.Shutdown()

	pluginSvc.trackExecutionQueued(&plugin_pb.JobSpec{
		JobId:     "job-pending-1",
		JobType:   "vacuum",
		DedupeKey: "vacuum:1",
		Summary:   "pending queue item",
	})

	jobs := pluginSvc.ListTrackedJobs("vacuum", "", 10)
	if len(jobs) != 1 {
		t.Fatalf("expected one tracked pending job, got=%d", len(jobs))
	}
	job := jobs[0]
	if job.JobID != "job-pending-1" {
		t.Fatalf("unexpected pending job id: %s", job.JobID)
	}
	if job.State != "job_state_pending" {
		t.Fatalf("unexpected pending job state: %s", job.State)
	}
	if job.Stage != "queued" {
		t.Fatalf("unexpected pending job stage: %s", job.Stage)
	}

	activities := pluginSvc.ListActivities("vacuum", 50)
	found := false
	for _, activity := range activities {
		if activity.JobID == "job-pending-1" && activity.Stage == "queued" && activity.Source == "admin_scheduler" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected queued activity for pending job")
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

	pluginSvc.handleJobProgressUpdate("worker-a", &plugin_pb.JobProgressUpdate{
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

func TestHandleJobProgressUpdateWithoutJobIDTracksDetectionActivities(t *testing.T) {
	t.Parallel()

	pluginSvc, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pluginSvc.Shutdown()

	pluginSvc.handleJobProgressUpdate("worker-detector", &plugin_pb.JobProgressUpdate{
		RequestId: "detect-req-1",
		JobType:   "vacuum",
		State:     plugin_pb.JobState_JOB_STATE_RUNNING,
		Stage:     "decision_summary",
		Message:   "VACUUM: No tasks created for 3 volumes",
		Activities: []*plugin_pb.ActivityEvent{
			{
				Source:  plugin_pb.ActivitySource_ACTIVITY_SOURCE_DETECTOR,
				Stage:   "decision_summary",
				Message: "VACUUM: No tasks created for 3 volumes",
			},
		},
	})

	activities := pluginSvc.ListActivities("vacuum", 0)
	if len(activities) == 0 {
		t.Fatalf("expected activity entries")
	}

	foundDetectionProgress := false
	foundDetectorEvent := false
	for _, activity := range activities {
		if activity.RequestID != "detect-req-1" {
			continue
		}
		if activity.Source == "worker_detection" {
			foundDetectionProgress = true
			if activity.WorkerID != "worker-detector" {
				t.Fatalf("worker_detection worker mismatch: got=%q want=%q", activity.WorkerID, "worker-detector")
			}
		}
		if activity.Source == "activity_source_detector" {
			foundDetectorEvent = true
			if activity.WorkerID != "worker-detector" {
				t.Fatalf("detector event worker mismatch: got=%q want=%q", activity.WorkerID, "worker-detector")
			}
		}
	}

	if !foundDetectionProgress {
		t.Fatalf("expected worker_detection activity")
	}
	if !foundDetectorEvent {
		t.Fatalf("expected detector activity event")
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

func TestTrackExecutionStartStoresJobPayloadDetails(t *testing.T) {
	t.Parallel()

	pluginSvc, err := New(Options{DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pluginSvc.Shutdown()

	pluginSvc.trackExecutionStart("req-payload", "worker-c", &plugin_pb.JobSpec{
		JobId:   "job-payload",
		JobType: "vacuum",
		Summary: "payload summary",
		Detail:  "payload detail",
		Parameters: map[string]*plugin_pb.ConfigValue{
			"volume_id": {
				Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 9},
			},
		},
		Labels: map[string]string{
			"source": "detector",
		},
	}, 2)

	job, found := pluginSvc.GetTrackedJob("job-payload")
	if !found || job == nil {
		t.Fatalf("expected tracked job")
	}
	if job.Detail != "" {
		t.Fatalf("expected in-memory tracked job detail to be stripped, got=%q", job.Detail)
	}
	if job.Attempt != 2 {
		t.Fatalf("unexpected attempt: %d", job.Attempt)
	}
	if len(job.Labels) != 0 {
		t.Fatalf("expected in-memory labels to be stripped, got=%+v", job.Labels)
	}
	if len(job.Parameters) != 0 {
		t.Fatalf("expected in-memory parameters to be stripped, got=%+v", job.Parameters)
	}

	detail, found, err := pluginSvc.BuildJobDetail("job-payload", 100, 0)
	if err != nil {
		t.Fatalf("BuildJobDetail: %v", err)
	}
	if !found || detail == nil || detail.Job == nil {
		t.Fatalf("expected disk-backed job detail")
	}
	if detail.Job.Detail != "payload detail" {
		t.Fatalf("unexpected disk-backed detail: %q", detail.Job.Detail)
	}
	if got := detail.Job.Labels["source"]; got != "detector" {
		t.Fatalf("unexpected disk-backed label source: %q", got)
	}
	if got, ok := detail.Job.Parameters["volume_id"].(map[string]interface{}); !ok || got["int64_value"] != "9" {
		t.Fatalf("unexpected disk-backed parameters payload: %#v", detail.Job.Parameters["volume_id"])
	}
}

func TestTrackExecutionStartStoresErasureCodingExecutionPlan(t *testing.T) {
	t.Parallel()

	pluginSvc, err := New(Options{DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pluginSvc.Shutdown()

	taskParams := &worker_pb.TaskParams{
		TaskId:     "task-ec-1",
		VolumeId:   29,
		Collection: "photos",
		Sources: []*worker_pb.TaskSource{
			{
				Node:       "source-a:8080",
				DataCenter: "dc1",
				Rack:       "rack1",
				VolumeId:   29,
			},
		},
		Targets: []*worker_pb.TaskTarget{
			{
				Node:       "target-a:8080",
				DataCenter: "dc1",
				Rack:       "rack2",
				VolumeId:   29,
				ShardIds:   []uint32{0, 10},
			},
			{
				Node:       "target-b:8080",
				DataCenter: "dc2",
				Rack:       "rack3",
				VolumeId:   29,
				ShardIds:   []uint32{1, 11},
			},
		},
		TaskParams: &worker_pb.TaskParams_ErasureCodingParams{
			ErasureCodingParams: &worker_pb.ErasureCodingTaskParams{
				DataShards:   10,
				ParityShards: 4,
			},
		},
	}
	payload, err := proto.Marshal(taskParams)
	if err != nil {
		t.Fatalf("Marshal task params: %v", err)
	}

	pluginSvc.trackExecutionStart("req-ec-plan", "worker-ec", &plugin_pb.JobSpec{
		JobId:   "job-ec-plan",
		JobType: "erasure_coding",
		Parameters: map[string]*plugin_pb.ConfigValue{
			"task_params_pb": {
				Kind: &plugin_pb.ConfigValue_BytesValue{BytesValue: payload},
			},
		},
	}, 1)

	detail, found, err := pluginSvc.BuildJobDetail("job-ec-plan", 100, 0)
	if err != nil {
		t.Fatalf("BuildJobDetail: %v", err)
	}
	if !found || detail == nil || detail.Job == nil {
		t.Fatalf("expected disk-backed detail")
	}

	rawPlan, ok := detail.Job.Parameters["execution_plan"]
	if !ok {
		t.Fatalf("expected execution_plan in parameters, got=%+v", detail.Job.Parameters)
	}
	plan, ok := rawPlan.(map[string]interface{})
	if !ok {
		t.Fatalf("unexpected execution_plan type: %T", rawPlan)
	}
	if plan["job_type"] != "erasure_coding" {
		t.Fatalf("unexpected execution plan job type: %+v", plan["job_type"])
	}
	if plan["volume_id"] != float64(29) {
		t.Fatalf("unexpected execution plan volume id: %+v", plan["volume_id"])
	}
	targets, ok := plan["targets"].([]interface{})
	if !ok || len(targets) != 2 {
		t.Fatalf("unexpected targets in execution plan: %+v", plan["targets"])
	}
	assignments, ok := plan["shard_assignments"].([]interface{})
	if !ok || len(assignments) != 4 {
		t.Fatalf("unexpected shard assignments in execution plan: %+v", plan["shard_assignments"])
	}
	firstAssignment, ok := assignments[0].(map[string]interface{})
	if !ok {
		t.Fatalf("unexpected first assignment payload: %+v", assignments[0])
	}
	if firstAssignment["shard_id"] != float64(0) || firstAssignment["kind"] != "data" {
		t.Fatalf("unexpected first assignment: %+v", firstAssignment)
	}
}

func TestBuildJobDetailIncludesActivitiesAndRunRecord(t *testing.T) {
	t.Parallel()

	pluginSvc, err := New(Options{DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pluginSvc.Shutdown()

	pluginSvc.trackExecutionStart("req-detail", "worker-z", &plugin_pb.JobSpec{
		JobId:   "job-detail",
		JobType: "vacuum",
		Summary: "detail summary",
	}, 1)
	pluginSvc.handleJobProgressUpdate("worker-z", &plugin_pb.JobProgressUpdate{
		RequestId: "req-detail",
		JobId:     "job-detail",
		JobType:   "vacuum",
		State:     plugin_pb.JobState_JOB_STATE_RUNNING,
		Stage:     "scan",
		Message:   "scanning volume",
	})
	pluginSvc.handleJobCompleted(&plugin_pb.JobCompleted{
		RequestId: "req-detail",
		JobId:     "job-detail",
		JobType:   "vacuum",
		Success:   true,
		Result: &plugin_pb.JobResult{
			Summary: "done",
			OutputValues: map[string]*plugin_pb.ConfigValue{
				"affected": {
					Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 1},
				},
			},
		},
		CompletedAt: timestamppb.Now(),
	})

	detail, found, err := pluginSvc.BuildJobDetail("job-detail", 100, 5)
	if err != nil {
		t.Fatalf("BuildJobDetail error: %v", err)
	}
	if !found || detail == nil {
		t.Fatalf("expected job detail")
	}
	if detail.Job == nil || detail.Job.JobID != "job-detail" {
		t.Fatalf("unexpected job detail payload: %+v", detail.Job)
	}
	if detail.RunRecord == nil || detail.RunRecord.JobID != "job-detail" {
		t.Fatalf("expected run record for job-detail, got=%+v", detail.RunRecord)
	}
	if len(detail.Activities) == 0 {
		t.Fatalf("expected activity timeline entries")
	}
	if detail.Job.ResultOutputValues == nil {
		t.Fatalf("expected result output values")
	}
}

func TestBuildJobDetailLoadsFromDiskWhenMemoryCleared(t *testing.T) {
	t.Parallel()

	pluginSvc, err := New(Options{DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pluginSvc.Shutdown()

	pluginSvc.trackExecutionStart("req-disk", "worker-d", &plugin_pb.JobSpec{
		JobId:   "job-disk",
		JobType: "vacuum",
		Summary: "disk summary",
		Detail:  "disk detail payload",
	}, 1)

	pluginSvc.jobsMu.Lock()
	pluginSvc.jobs = map[string]*TrackedJob{}
	pluginSvc.jobsMu.Unlock()
	pluginSvc.activitiesMu.Lock()
	pluginSvc.activities = nil
	pluginSvc.activitiesMu.Unlock()

	detail, found, err := pluginSvc.BuildJobDetail("job-disk", 100, 0)
	if err != nil {
		t.Fatalf("BuildJobDetail: %v", err)
	}
	if !found || detail == nil || detail.Job == nil {
		t.Fatalf("expected detail from disk")
	}
	if detail.Job.Detail != "disk detail payload" {
		t.Fatalf("unexpected disk detail payload: %q", detail.Job.Detail)
	}
}
