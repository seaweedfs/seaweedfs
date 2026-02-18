package handlers

import (
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/seaweedfs/seaweedfs/weed/admin/dash"
)

func TestSetupRoutes_RegistersPluginSchedulerStatesAPI_NoAuth(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()

	newRouteTestAdminHandlers().SetupRoutes(router, false, "", "", "", "", true)

	if !hasRoute(router, "GET", "/api/plugin/scheduler-states") {
		t.Fatalf("expected GET /api/plugin/scheduler-states to be registered in no-auth mode")
	}
}

func TestSetupRoutes_RegistersPluginSchedulerStatesAPI_WithAuth(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()

	newRouteTestAdminHandlers().SetupRoutes(router, true, "admin", "password", "", "", true)

	if !hasRoute(router, "GET", "/api/plugin/scheduler-states") {
		t.Fatalf("expected GET /api/plugin/scheduler-states to be registered in auth mode")
	}
}

func TestSetupRoutes_RegistersPluginPages_NoAuth(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()

	newRouteTestAdminHandlers().SetupRoutes(router, false, "", "", "", "", true)

	assertHasRoute(t, router, "GET", "/plugin")
	assertHasRoute(t, router, "GET", "/plugin/configuration")
	assertHasRoute(t, router, "GET", "/plugin/monitoring")
}

func TestSetupRoutes_RegistersPluginPages_WithAuth(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()

	newRouteTestAdminHandlers().SetupRoutes(router, true, "admin", "password", "", "", true)

	assertHasRoute(t, router, "GET", "/plugin")
	assertHasRoute(t, router, "GET", "/plugin/configuration")
	assertHasRoute(t, router, "GET", "/plugin/monitoring")
}

func newRouteTestAdminHandlers() *AdminHandlers {
	adminServer := &dash.AdminServer{}
	return &AdminHandlers{
		adminServer:            adminServer,
		authHandlers:           &AuthHandlers{adminServer: adminServer},
		clusterHandlers:        &ClusterHandlers{adminServer: adminServer},
		fileBrowserHandlers:    &FileBrowserHandlers{adminServer: adminServer},
		userHandlers:           &UserHandlers{adminServer: adminServer},
		policyHandlers:         &PolicyHandlers{adminServer: adminServer},
		maintenanceHandlers:    &MaintenanceHandlers{adminServer: adminServer},
		pluginHandlers:         &PluginHandlers{adminServer: adminServer},
		mqHandlers:             &MessageQueueHandlers{adminServer: adminServer},
		serviceAccountHandlers: &ServiceAccountHandlers{adminServer: adminServer},
	}
}

func hasRoute(router *gin.Engine, method string, path string) bool {
	for _, route := range router.Routes() {
		if route.Method == method && route.Path == path {
			return true
		}
	}
	return false
}

func assertHasRoute(t *testing.T, router *gin.Engine, method string, path string) {
	t.Helper()
	if !hasRoute(router, method, path) {
		t.Fatalf("expected %s %s to be registered", method, path)
	}
}
