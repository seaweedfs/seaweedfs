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

// ShowPlugin displays plugin overview page.
func (h *PluginHandlers) ShowPlugin(c *gin.Context) {
	h.renderPluginPage(c, "overview")
}

// ShowPluginConfiguration displays plugin configuration page.
func (h *PluginHandlers) ShowPluginConfiguration(c *gin.Context) {
	h.renderPluginPage(c, "configuration")
}

// ShowPluginDetection displays plugin detection jobs page.
func (h *PluginHandlers) ShowPluginDetection(c *gin.Context) {
	h.renderPluginPage(c, "detection")
}

// ShowPluginQueue displays plugin job queue page.
func (h *PluginHandlers) ShowPluginQueue(c *gin.Context) {
	h.renderPluginPage(c, "queue")
}

// ShowPluginExecution displays plugin execution jobs page.
func (h *PluginHandlers) ShowPluginExecution(c *gin.Context) {
	h.renderPluginPage(c, "execution")
}

// ShowPluginMonitoring displays plugin monitoring page.
func (h *PluginHandlers) ShowPluginMonitoring(c *gin.Context) {
	// Backward-compatible alias for the old monitoring URL.
	h.renderPluginPage(c, "detection")
}

func (h *PluginHandlers) renderPluginPage(c *gin.Context, page string) {
	c.Header("Content-Type", "text/html")
	component := app.Plugin(page)
	layoutComponent := layout.Layout(c, component)
	if err := layoutComponent.Render(c.Request.Context(), c.Writer); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to render template: " + err.Error()})
		return
	}
}
