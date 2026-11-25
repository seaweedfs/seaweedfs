package dash

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/admin/plugin"
)

func TestGetPluginJobDetailAPIStripsRichFieldsForReadOnly(t *testing.T) {
	dataDir := t.TempDir()
	store, err := plugin.NewConfigStore(dataDir)
	if err != nil {
		t.Fatalf("NewConfigStore: %v", err)
	}
	const marker = "secret-marker"
	if err := store.SaveJobDetail(plugin.TrackedJob{
		JobID:              "job-1",
		JobType:            "admin_script",
		State:              "succeeded",
		Summary:            "Run admin script",
		Detail:             "detail " + marker,
		Parameters:         map[string]interface{}{"script": marker},
		Labels:             map[string]string{"script_name": marker},
		ResultOutputValues: map[string]interface{}{"output": marker},
	}); err != nil {
		t.Fatalf("SaveJobDetail: %v", err)
	}
	if err := store.SaveActivities([]plugin.JobActivity{{
		JobID:   "job-1",
		JobType: "admin_script",
		Source:  "activity_source_executor",
		Stage:   "running",
		Message: "s3.configure -secret_key " + marker,
	}}); err != nil {
		t.Fatalf("SaveActivities: %v", err)
	}
	p, err := plugin.New(plugin.Options{DataDir: dataDir})
	if err != nil {
		t.Fatalf("plugin.New: %v", err)
	}
	t.Cleanup(p.Shutdown)
	s := &AdminServer{plugin: p}

	get := func(role string) (int, string) {
		req := httptest.NewRequest(http.MethodGet, "/api/plugin/jobs/job-1/detail", nil)
		req = mux.SetURLVars(req, map[string]string{"jobId": "job-1"})
		req = req.WithContext(WithAuthContext(req.Context(), "user", role, ""))
		w := httptest.NewRecorder()
		s.GetPluginJobDetailAPI(w, req)
		return w.Code, w.Body.String()
	}

	code, body := get("admin")
	if code != http.StatusOK || !strings.Contains(body, marker) {
		t.Fatalf("admin: code=%d body=%s", code, body)
	}

	code, body = get(RoleReadOnly)
	if code != http.StatusOK {
		t.Fatalf("read-only: code=%d body=%s", code, body)
	}
	if strings.Contains(body, marker) {
		t.Fatalf("read-only detail leaked rich fields: %s", body)
	}
	var detail plugin.JobDetail
	if err := json.Unmarshal([]byte(body), &detail); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if detail.Job == nil || detail.Job.State != "succeeded" || detail.Job.Summary != "Run admin script" {
		t.Fatalf("read-only detail lost compact fields: %+v", detail.Job)
	}
	if len(detail.Activities) != 1 || detail.Activities[0].Message != "s3.configure" {
		t.Fatalf("read-only detail activities: %+v", detail.Activities)
	}
}
