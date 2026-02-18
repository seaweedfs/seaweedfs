package plugin

import (
	"reflect"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

func TestConfigStoreDescriptorRoundTrip(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	store, err := NewConfigStore(tempDir)
	if err != nil {
		t.Fatalf("NewConfigStore: %v", err)
	}

	descriptor := &plugin_pb.JobTypeDescriptor{
		JobType:           "vacuum",
		DisplayName:       "Vacuum",
		Description:       "Vacuum volumes",
		DescriptorVersion: 1,
	}

	if err := store.SaveDescriptor("vacuum", descriptor); err != nil {
		t.Fatalf("SaveDescriptor: %v", err)
	}

	got, err := store.LoadDescriptor("vacuum")
	if err != nil {
		t.Fatalf("LoadDescriptor: %v", err)
	}
	if got == nil {
		t.Fatalf("LoadDescriptor: nil descriptor")
	}
	if got.DisplayName != descriptor.DisplayName {
		t.Fatalf("unexpected display name: got %q want %q", got.DisplayName, descriptor.DisplayName)
	}

}

func TestConfigStoreRunHistoryRetention(t *testing.T) {
	t.Parallel()

	store, err := NewConfigStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewConfigStore: %v", err)
	}

	base := time.Now().UTC().Add(-24 * time.Hour)
	for i := 0; i < 15; i++ {
		err := store.AppendRunRecord("balance", &JobRunRecord{
			RunID:       "s" + time.Duration(i).String(),
			JobID:       "job-success",
			JobType:     "balance",
			WorkerID:    "worker-a",
			Outcome:     RunOutcomeSuccess,
			CompletedAt: base.Add(time.Duration(i) * time.Minute),
		})
		if err != nil {
			t.Fatalf("AppendRunRecord success[%d]: %v", i, err)
		}
	}

	for i := 0; i < 12; i++ {
		err := store.AppendRunRecord("balance", &JobRunRecord{
			RunID:       "e" + time.Duration(i).String(),
			JobID:       "job-error",
			JobType:     "balance",
			WorkerID:    "worker-b",
			Outcome:     RunOutcomeError,
			CompletedAt: base.Add(time.Duration(i) * time.Minute),
		})
		if err != nil {
			t.Fatalf("AppendRunRecord error[%d]: %v", i, err)
		}
	}

	history, err := store.LoadRunHistory("balance")
	if err != nil {
		t.Fatalf("LoadRunHistory: %v", err)
	}
	if len(history.SuccessfulRuns) != MaxSuccessfulRunHistory {
		t.Fatalf("successful retention mismatch: got %d want %d", len(history.SuccessfulRuns), MaxSuccessfulRunHistory)
	}
	if len(history.ErrorRuns) != MaxErrorRunHistory {
		t.Fatalf("error retention mismatch: got %d want %d", len(history.ErrorRuns), MaxErrorRunHistory)
	}

	for i := 1; i < len(history.SuccessfulRuns); i++ {
		if history.SuccessfulRuns[i-1].CompletedAt.Before(history.SuccessfulRuns[i].CompletedAt) {
			t.Fatalf("successful run order not descending at %d", i)
		}
	}
	for i := 1; i < len(history.ErrorRuns); i++ {
		if history.ErrorRuns[i-1].CompletedAt.Before(history.ErrorRuns[i].CompletedAt) {
			t.Fatalf("error run order not descending at %d", i)
		}
	}
}

func TestConfigStoreListJobTypes(t *testing.T) {
	t.Parallel()

	store, err := NewConfigStore("")
	if err != nil {
		t.Fatalf("NewConfigStore: %v", err)
	}

	if err := store.SaveDescriptor("vacuum", &plugin_pb.JobTypeDescriptor{JobType: "vacuum"}); err != nil {
		t.Fatalf("SaveDescriptor: %v", err)
	}
	if err := store.SaveJobTypeConfig(&plugin_pb.PersistedJobTypeConfig{
		JobType:      "balance",
		AdminRuntime: &plugin_pb.AdminRuntimeConfig{Enabled: true},
	}); err != nil {
		t.Fatalf("SaveJobTypeConfig: %v", err)
	}
	if err := store.AppendRunRecord("ec", &JobRunRecord{Outcome: RunOutcomeSuccess, CompletedAt: time.Now().UTC()}); err != nil {
		t.Fatalf("AppendRunRecord: %v", err)
	}

	got, err := store.ListJobTypes()
	if err != nil {
		t.Fatalf("ListJobTypes: %v", err)
	}
	want := []string{"balance", "ec", "vacuum"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected job types: got=%v want=%v", got, want)
	}
}

func TestConfigStoreMonitorStateRoundTrip(t *testing.T) {
	t.Parallel()

	store, err := NewConfigStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewConfigStore: %v", err)
	}

	tracked := []TrackedJob{
		{
			JobID:     "job-1",
			JobType:   "vacuum",
			State:     "running",
			Progress:  55,
			WorkerID:  "worker-a",
			CreatedAt: time.Now().UTC().Add(-2 * time.Minute),
			UpdatedAt: time.Now().UTC().Add(-1 * time.Minute),
		},
	}
	activities := []JobActivity{
		{
			JobID:      "job-1",
			JobType:    "vacuum",
			Source:     "worker_progress",
			Message:    "processing",
			Stage:      "running",
			OccurredAt: time.Now().UTC(),
			Details: map[string]interface{}{
				"step": "scan",
			},
		},
	}

	if err := store.SaveTrackedJobs(tracked); err != nil {
		t.Fatalf("SaveTrackedJobs: %v", err)
	}
	if err := store.SaveActivities(activities); err != nil {
		t.Fatalf("SaveActivities: %v", err)
	}

	gotTracked, err := store.LoadTrackedJobs()
	if err != nil {
		t.Fatalf("LoadTrackedJobs: %v", err)
	}
	if len(gotTracked) != 1 || gotTracked[0].JobID != tracked[0].JobID {
		t.Fatalf("unexpected tracked jobs: %+v", gotTracked)
	}

	gotActivities, err := store.LoadActivities()
	if err != nil {
		t.Fatalf("LoadActivities: %v", err)
	}
	if len(gotActivities) != 1 || gotActivities[0].Message != activities[0].Message {
		t.Fatalf("unexpected activities: %+v", gotActivities)
	}
	if gotActivities[0].Details["step"] != "scan" {
		t.Fatalf("unexpected activity details: %+v", gotActivities[0].Details)
	}
}

func TestConfigStoreJobDetailRoundTrip(t *testing.T) {
	t.Parallel()

	store, err := NewConfigStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewConfigStore: %v", err)
	}

	input := TrackedJob{
		JobID:     "job/detail:1",
		JobType:   "dummy_stress",
		Summary:   "detail summary",
		Detail:    "detail payload",
		CreatedAt: time.Now().UTC().Add(-2 * time.Minute),
		UpdatedAt: time.Now().UTC(),
		Parameters: map[string]interface{}{
			"volume_id": map[string]interface{}{"int64_value": "3"},
		},
		Labels: map[string]string{
			"source": "detector",
		},
		ResultOutputValues: map[string]interface{}{
			"moved": map[string]interface{}{"bool_value": true},
		},
	}

	if err := store.SaveJobDetail(input); err != nil {
		t.Fatalf("SaveJobDetail: %v", err)
	}

	got, err := store.LoadJobDetail(input.JobID)
	if err != nil {
		t.Fatalf("LoadJobDetail: %v", err)
	}
	if got == nil {
		t.Fatalf("LoadJobDetail returned nil")
	}
	if got.Detail != input.Detail {
		t.Fatalf("unexpected detail: got=%q want=%q", got.Detail, input.Detail)
	}
	if got.Labels["source"] != "detector" {
		t.Fatalf("unexpected labels: %+v", got.Labels)
	}
	if got.ResultOutputValues == nil {
		t.Fatalf("expected result output values")
	}
}
