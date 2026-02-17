package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/seaweedfs/seaweedfs/weed/admin/dash"
	"github.com/seaweedfs/seaweedfs/weed/admin/view/app"
	"github.com/seaweedfs/seaweedfs/weed/admin/view/layout"
)

// PluginHandlers handles plugin runtime UI pages.
type PluginHandlers struct {
	adminServer *dash.AdminServer
}

// NewPluginHandlers creates a new instance of PluginHandlers.
func NewPluginHandlers(adminServer *dash.AdminServer) *PluginHandlers {
	return &PluginHandlers{
		adminServer: adminServer,
	}
}

// ShowPluginRuntime displays plugin runtime monitoring and configuration page.
func (h *PluginHandlers) ShowPluginRuntime(c *gin.Context) {
	enabled := h.adminServer != nil && h.adminServer.IsPluginRuntimeEnabled()

	c.Header("Content-Type", "text/html")
	component := app.PluginRuntime(enabled)
	layoutComponent := layout.Layout(c, component)
	if err := layoutComponent.Render(c.Request.Context(), c.Writer); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to render template: " + err.Error()})
		return
	}
}
