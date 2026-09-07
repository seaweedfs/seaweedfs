package handlers

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/admin/dash"
	"github.com/stretchr/testify/require"
)

func TestSetVolumeReadOnlyInvalidRequests(t *testing.T) {
	for _, tc := range []struct {
		name, id, server, body string
	}{
		{"missing volume", "", "node-a", `{"read_only":true}`},
		{"negative volume", "-1", "node-a", `{"read_only":true}`},
		{"overflow volume", "4294967296", "node-a", `{"read_only":true}`},
		{"invalid volume", "abc", "node-a", `{"read_only":true}`},
		{"missing server", "7", "", `{"read_only":true}`},
		{"missing mode", "7", "node-a", `{}`},
		{"null mode", "7", "node-a", `{"read_only":null}`},
		{"string mode", "7", "node-a", `{"read_only":"false"}`},
		{"malformed JSON", "7", "node-a", `{"read_only":`},
		{"trailing JSON", "7", "node-a", `{"read_only":true}{}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tc.body))
			req = mux.SetURLVars(req, map[string]string{"id": tc.id, "server": tc.server})
			w := httptest.NewRecorder()
			// Invalid input must be rejected before accessing the cluster.
			(&ClusterHandlers{}).SetVolumeReadOnly(w, req)
			require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
		})
	}
}

func TestSetVolumeReadOnlyRoutePermissions(t *testing.T) {
	for _, tc := range []struct {
		name, role string
		enforce    bool
		wantStatus int
	}{
		{"read-only user", dash.RoleReadOnly, true, http.StatusForbidden},
		{"admin", "admin", true, http.StatusBadRequest},
		{"no authentication", "", false, http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			router := mux.NewRouter()
			newRouteTestAdminHandlers().registerAPIRoutes(router.PathPrefix("/api").Subrouter(), tc.enforce)
			// Deliberately omit the mode: allowed callers reach validation, while
			// read-only callers must be rejected by the route's write guard.
			req := httptest.NewRequest(http.MethodPost, "/api/volumes/7/node-a/read-only", strings.NewReader(`{}`))
			req = req.WithContext(dash.WithAuthContext(req.Context(), "", tc.role, ""))
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
			require.Equal(t, tc.wantStatus, w.Code, w.Body.String())
		})
	}
}
