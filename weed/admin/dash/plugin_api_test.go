package dash

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/admin/plugin"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

func TestExpirePluginJobAPI(t *testing.T) {
	makeRequest := func(adminServer *AdminServer, jobID string, body io.Reader) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/api/plugin/jobs/"+jobID+"/expire", body)
		req = mux.SetURLVars(req, map[string]string{"jobId": jobID})
		recorder := httptest.NewRecorder()
		adminServer.ExpirePluginJobAPI(recorder, req)
		return recorder
	}

	t.Run("empty job id", func(t *testing.T) {
		recorder := makeRequest(&AdminServer{}, "", nil)
		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("expected 400, got %d", recorder.Code)
		}
	})

	t.Run("invalid json", func(t *testing.T) {
		recorder := makeRequest(&AdminServer{}, "job-id", strings.NewReader("{"))
		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("expected 400, got %d", recorder.Code)
		}
	})

	t.Run("job not found", func(t *testing.T) {
		adminServer := &AdminServer{
			expireJobHandler: func(jobID, reason string) (*plugin.TrackedJob, bool, error) {
				return nil, false, plugin.ErrJobNotFound
			},
		}
		recorder := makeRequest(adminServer, "missing", strings.NewReader(`{"reason":"nope"}`))
		if recorder.Code != http.StatusNotFound {
			t.Fatalf("expected 404, got %d", recorder.Code)
		}
		var payload map[string]any
		if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
			t.Fatalf("failed to unmarshal body: %v", err)
		}
		if payload["error"] == nil {
			t.Fatalf("expected error payload, got %v", payload)
		}
	})

	t.Run("successful expire", func(t *testing.T) {
		expected := &plugin.TrackedJob{JobID: "foo", State: "assigned"}
		adminServer := &AdminServer{
			expireJobHandler: func(jobID, reason string) (*plugin.TrackedJob, bool, error) {
				if jobID != "foo" {
					return nil, false, errors.New("unexpected")
				}
				return expected, true, nil
			},
		}
		recorder := makeRequest(adminServer, "foo", strings.NewReader(`{"reason":"cleanup"}`))
		if recorder.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", recorder.Code)
		}
		var payload map[string]any
		if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
			t.Fatalf("failed to decode payload: %v", err)
		}
		if payload["job_id"] != "foo" {
			t.Fatalf("expected job_id foo, got %v", payload["job_id"])
		}
		if expired, ok := payload["expired"].(bool); !ok || !expired {
			t.Fatalf("expected expired=true, got %v", payload["expired"])
		}
		jobData, ok := payload["job"].(map[string]any)
		if !ok || jobData["job_id"] != "foo" {
			t.Fatalf("expected job info with job_id, got %v", payload["job"])
		}
	})

	t.Run("non-active job", func(t *testing.T) {
		adminServer := &AdminServer{
			expireJobHandler: func(jobID, reason string) (*plugin.TrackedJob, bool, error) {
				return nil, false, nil
			},
		}
		recorder := makeRequest(adminServer, "bar", strings.NewReader(`{"reason":"ignore"}`))
		if recorder.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", recorder.Code)
		}
		var payload map[string]any
		if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
			t.Fatalf("failed to decode payload: %v", err)
		}
		if payload["job_id"] != "bar" {
			t.Fatalf("expected job_id bar, got %v", payload["job_id"])
		}
		if expired, ok := payload["expired"].(bool); !ok || expired {
			t.Fatalf("expected expired=false, got %v", payload["expired"])
		}
		if payload["message"] != "job is not active" {
			t.Fatalf("expected message job is not active, got %v", payload["message"])
		}
		if _, exists := payload["job"]; exists {
			t.Fatalf("expected no job payload for non-active job, got %v", payload["job"])
		}
	})
}

func TestApplyDescriptorDefaultsToPersistedConfigBackfillsAdminDefaults(t *testing.T) {
	t.Parallel()

	config := &plugin_pb.PersistedJobTypeConfig{
		JobType:            "admin_script",
		AdminConfigValues:  map[string]*plugin_pb.ConfigValue{},
		WorkerConfigValues: map[string]*plugin_pb.ConfigValue{},
		AdminRuntime:       &plugin_pb.AdminRuntimeConfig{},
	}
	descriptor := &plugin_pb.JobTypeDescriptor{
		JobType: "admin_script",
		AdminConfigForm: &plugin_pb.ConfigForm{
			DefaultValues: map[string]*plugin_pb.ConfigValue{
				"script": {
					Kind: &plugin_pb.ConfigValue_StringValue{StringValue: "volume.balance -apply"},
				},
				"run_interval_minutes": {
					Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 17},
				},
			},
		},
		AdminRuntimeDefaults: &plugin_pb.AdminRuntimeDefaults{
			DetectionIntervalSeconds: 60,
			DetectionTimeoutSeconds:  300,
		},
	}

	applyDescriptorDefaultsToPersistedConfig(config, descriptor)

	script := config.AdminConfigValues["script"]
	if script == nil {
		t.Fatalf("expected script default to be backfilled")
	}
	scriptKind, ok := script.Kind.(*plugin_pb.ConfigValue_StringValue)
	if !ok || scriptKind.StringValue == "" {
		t.Fatalf("expected non-empty script default, got=%+v", script)
	}
	if config.AdminRuntime.DetectionIntervalSeconds != 60 {
		t.Fatalf("expected runtime detection interval default to be backfilled")
	}
}

func TestApplyDescriptorDefaultsToPersistedConfigReplacesBlankAdminScript(t *testing.T) {
	t.Parallel()

	config := &plugin_pb.PersistedJobTypeConfig{
		JobType: "admin_script",
		AdminConfigValues: map[string]*plugin_pb.ConfigValue{
			"script": {
				Kind: &plugin_pb.ConfigValue_StringValue{StringValue: "   "},
			},
		},
		AdminRuntime: &plugin_pb.AdminRuntimeConfig{},
	}
	descriptor := &plugin_pb.JobTypeDescriptor{
		JobType: "admin_script",
		AdminConfigForm: &plugin_pb.ConfigForm{
			DefaultValues: map[string]*plugin_pb.ConfigValue{
				"script": {
					Kind: &plugin_pb.ConfigValue_StringValue{StringValue: "volume.fix.replication -apply"},
				},
			},
		},
	}

	applyDescriptorDefaultsToPersistedConfig(config, descriptor)

	script := config.AdminConfigValues["script"]
	if script == nil {
		t.Fatalf("expected script config value")
	}
	scriptKind, ok := script.Kind.(*plugin_pb.ConfigValue_StringValue)
	if !ok {
		t.Fatalf("expected string script config value, got=%T", script.Kind)
	}
	if scriptKind.StringValue != "volume.fix.replication -apply" {
		t.Fatalf("expected blank script to be replaced by default, got=%q", scriptKind.StringValue)
	}
}

func TestFilterTrackedJobsByLane(t *testing.T) {
	t.Parallel()

	jobs := []plugin.TrackedJob{
		{JobID: "vacuum-1", JobType: "vacuum"},
		{JobID: "iceberg-1", JobType: "iceberg_maintenance"},
		{JobID: "lifecycle-1", JobType: "s3_lifecycle"},
	}

	filtered := filterTrackedJobsByLane(jobs, "iceberg")
	if len(filtered) != 1 {
		t.Fatalf("expected 1 iceberg job, got %d", len(filtered))
	}
	if filtered[0].JobID != "iceberg-1" {
		t.Fatalf("expected iceberg job to be retained, got %+v", filtered[0])
	}
}

func TestFilterActivitiesByLane(t *testing.T) {
	t.Parallel()

	activities := []plugin.JobActivity{
		{JobID: "vacuum-1", JobType: "vacuum"},
		{JobID: "iceberg-1", JobType: "iceberg_maintenance"},
		{JobID: "lifecycle-1", JobType: "s3_lifecycle"},
	}

	filtered := filterActivitiesByLane(activities, "lifecycle")
	if len(filtered) != 1 {
		t.Fatalf("expected 1 lifecycle activity, got %d", len(filtered))
	}
	if filtered[0].JobID != "lifecycle-1" {
		t.Fatalf("expected lifecycle activity to be retained, got %+v", filtered[0])
	}
}
