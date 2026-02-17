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
