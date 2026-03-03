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

func TestBuildJobSpecFromProposalDoesNotReuseProposalID(t *testing.T) {
	t.Parallel()

	proposal := &plugin_pb.JobProposal{
		ProposalId: "vacuum-2",
		DedupeKey:  "vacuum:2",
		JobType:    "vacuum",
	}

	jobA := buildJobSpecFromProposal("vacuum", proposal, 0)
	jobB := buildJobSpecFromProposal("vacuum", proposal, 1)

	if jobA.JobId == proposal.ProposalId {
		t.Fatalf("job id must not reuse proposal id: %s", jobA.JobId)
	}
	if jobB.JobId == proposal.ProposalId {
		t.Fatalf("job id must not reuse proposal id: %s", jobB.JobId)
	}
	if jobA.JobId == jobB.JobId {
		t.Fatalf("job ids must be unique across jobs: %s", jobA.JobId)
	}
	if jobA.DedupeKey != proposal.DedupeKey {
		t.Fatalf("dedupe key must be preserved: got=%s want=%s", jobA.DedupeKey, proposal.DedupeKey)
	}
}
