package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/seaweedfs/seaweedfs/weed/admin/dash"
	"github.com/seaweedfs/seaweedfs/weed/admin/view/app"
	"github.com/seaweedfs/seaweedfs/weed/admin/view/layout"
)

// PluginHandlers handles plugin UI pages.
type PluginHandlers struct {
	adminServer *dash.AdminServer
}

// NewPluginHandlers creates a new instance of PluginHandlers.
func NewPluginHandlers(adminServer *dash.AdminServer) *PluginHandlers {
	return &PluginHandlers{
		adminServer: adminServer,
	}
}

// ShowPlugin displays plugin monitoring and configuration page.
func (h *PluginHandlers) ShowPlugin(c *gin.Context) {
	c.Header("Content-Type", "text/html")
	component := app.Plugin(true)
	layoutComponent := layout.Layout(c, component)
	if err := layoutComponent.Render(c.Request.Context(), c.Writer); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to render template: " + err.Error()})
		return
	}
}
