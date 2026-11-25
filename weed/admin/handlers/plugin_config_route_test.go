package handlers

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/admin/dash"
)

func TestPluginJobTypeConfigRequiresAdmin(t *testing.T) {
	for _, tc := range []struct {
		name, role string
		enforce    bool
		wantDenied bool
	}{
		{"read-only user", dash.RoleReadOnly, true, true},
		{"admin", "admin", true, false},
		{"no authentication", "", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			router := mux.NewRouter()
			h := &AdminHandlers{adminServer: &dash.AdminServer{}}
			h.registerAPIRoutes(router.PathPrefix("/api").Subrouter(), tc.enforce)
			req := httptest.NewRequest(http.MethodGet, "/api/plugin/job-types/admin_script/config", nil)
			req = req.WithContext(dash.WithAuthContext(req.Context(), "", tc.role, ""))
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
			if denied := w.Code == http.StatusForbidden; denied != tc.wantDenied {
				t.Fatalf("code=%d body=%s, want denied=%v", w.Code, w.Body.String(), tc.wantDenied)
			}
		})
	}
}
