package handlers

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
	"github.com/gorilla/sessions"
	"github.com/seaweedfs/seaweedfs/weed/admin/dash"
)

func TestSetupRoutes_RegistersPluginSchedulerStatesAPI_NoAuth(t *testing.T) {
	router := mux.NewRouter()

	newRouteTestAdminHandlers().SetupRoutes(router, false, "", "", "", "", true)

	if !hasRoute(router, http.MethodGet, "/api/plugin/scheduler-states") {
		t.Fatalf("expected GET /api/plugin/scheduler-states to be registered in no-auth mode")
	}
	if !hasRoute(router, http.MethodGet, "/api/plugin/jobs/example/detail") {
		t.Fatalf("expected GET /api/plugin/jobs/:jobId/detail to be registered in no-auth mode")
	}
	if !hasRoute(router, http.MethodPost, "/api/plugin/jobs/example/expire") {
		t.Fatalf("expected POST /api/plugin/jobs/:jobId/expire to be registered in no-auth mode")
	}
}

func TestSetupRoutes_RegistersPluginSchedulerStatesAPI_WithAuth(t *testing.T) {
	router := mux.NewRouter()

	newRouteTestAdminHandlers().SetupRoutes(router, true, "admin", "password", "", "", true)

	if !hasRoute(router, http.MethodGet, "/api/plugin/scheduler-states") {
		t.Fatalf("expected GET /api/plugin/scheduler-states to be registered in auth mode")
	}
	if !hasRoute(router, http.MethodGet, "/api/plugin/jobs/example/detail") {
		t.Fatalf("expected GET /api/plugin/jobs/:jobId/detail to be registered in auth mode")
	}
	if !hasRoute(router, http.MethodPost, "/api/plugin/jobs/example/expire") {
		t.Fatalf("expected POST /api/plugin/jobs/:jobId/expire to be registered in auth mode")
	}
}

func TestSetupRoutes_RegistersPluginPages_NoAuth(t *testing.T) {
	router := mux.NewRouter()

	newRouteTestAdminHandlers().SetupRoutes(router, false, "", "", "", "", true)

	assertHasRoute(t, router, http.MethodGet, "/plugin")
	assertHasRoute(t, router, http.MethodGet, "/plugin/configuration")
	assertHasRoute(t, router, http.MethodGet, "/plugin/queue")
	assertHasRoute(t, router, http.MethodGet, "/plugin/detection")
	assertHasRoute(t, router, http.MethodGet, "/plugin/execution")
	assertHasRoute(t, router, http.MethodGet, "/plugin/monitoring")
	assertHasRoute(t, router, http.MethodGet, "/plugin/lanes/default")
	assertHasRoute(t, router, http.MethodGet, "/plugin/lanes/default/configuration")
	assertHasRoute(t, router, http.MethodGet, "/plugin/lanes/default/queue")
	assertHasRoute(t, router, http.MethodGet, "/plugin/lanes/default/detection")
	assertHasRoute(t, router, http.MethodGet, "/plugin/lanes/default/execution")
	assertHasRoute(t, router, http.MethodGet, "/plugin/lanes/default/monitoring")
	assertHasRoute(t, router, http.MethodGet, "/plugin/lanes/default/workers")
	assertHasRoute(t, router, http.MethodGet, "/plugin/lanes/lifecycle")
	assertHasRoute(t, router, http.MethodGet, "/plugin/lanes/lifecycle/configuration")
	assertHasRoute(t, router, http.MethodGet, "/plugin/lanes/lifecycle/workers")
}

func TestSetupRoutes_RegistersPluginPages_WithAuth(t *testing.T) {
	router := mux.NewRouter()

	newRouteTestAdminHandlers().SetupRoutes(router, true, "admin", "password", "", "", true)

	assertHasRoute(t, router, http.MethodGet, "/plugin")
	assertHasRoute(t, router, http.MethodGet, "/plugin/configuration")
	assertHasRoute(t, router, http.MethodGet, "/plugin/queue")
	assertHasRoute(t, router, http.MethodGet, "/plugin/detection")
	assertHasRoute(t, router, http.MethodGet, "/plugin/execution")
	assertHasRoute(t, router, http.MethodGet, "/plugin/monitoring")
	assertHasRoute(t, router, http.MethodGet, "/plugin/lanes/iceberg")
	assertHasRoute(t, router, http.MethodGet, "/plugin/lanes/iceberg/configuration")
	assertHasRoute(t, router, http.MethodGet, "/plugin/lanes/iceberg/queue")
	assertHasRoute(t, router, http.MethodGet, "/plugin/lanes/iceberg/detection")
	assertHasRoute(t, router, http.MethodGet, "/plugin/lanes/iceberg/execution")
	assertHasRoute(t, router, http.MethodGet, "/plugin/lanes/iceberg/monitoring")
	assertHasRoute(t, router, http.MethodGet, "/plugin/lanes/iceberg/workers")
}

func newRouteTestAdminHandlers() *AdminHandlers {
	adminServer := &dash.AdminServer{}
	store := sessions.NewCookieStore([]byte("test-session-key"))
	return &AdminHandlers{
		adminServer:            adminServer,
		sessionStore:           store,
		authHandlers:           &AuthHandlers{adminServer: adminServer, sessionStore: store},
		clusterHandlers:        &ClusterHandlers{adminServer: adminServer},
		fileBrowserHandlers:    &FileBrowserHandlers{adminServer: adminServer},
		userHandlers:           &UserHandlers{adminServer: adminServer},
		policyHandlers:         &PolicyHandlers{adminServer: adminServer},
		pluginHandlers:         &PluginHandlers{adminServer: adminServer},
		mqHandlers:             &MessageQueueHandlers{adminServer: adminServer},
		serviceAccountHandlers: &ServiceAccountHandlers{adminServer: adminServer},
	}
}

func hasRoute(router *mux.Router, method string, path string) bool {
	req := httptest.NewRequest(method, path, nil)
	var match mux.RouteMatch
	return router.Match(req, &match)
}

func assertHasRoute(t *testing.T, router *mux.Router, method string, path string) {
	t.Helper()
	if !hasRoute(router, method, path) {
		t.Fatalf("expected %s %s to be registered", method, path)
	}
}
