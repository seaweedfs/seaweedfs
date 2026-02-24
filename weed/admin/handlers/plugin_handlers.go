package handlers

import (
	"bytes"
	"net/http"

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
func (h *PluginHandlers) ShowPlugin(w http.ResponseWriter, r *http.Request) {
	h.renderPluginPage(w, r, "overview")
}

// ShowPluginConfiguration displays plugin configuration page.
func (h *PluginHandlers) ShowPluginConfiguration(w http.ResponseWriter, r *http.Request) {
	h.renderPluginPage(w, r, "configuration")
}

// ShowPluginDetection displays plugin detection jobs page.
func (h *PluginHandlers) ShowPluginDetection(w http.ResponseWriter, r *http.Request) {
	h.renderPluginPage(w, r, "detection")
}

// ShowPluginQueue displays plugin job queue page.
func (h *PluginHandlers) ShowPluginQueue(w http.ResponseWriter, r *http.Request) {
	h.renderPluginPage(w, r, "queue")
}

// ShowPluginExecution displays plugin execution jobs page.
func (h *PluginHandlers) ShowPluginExecution(w http.ResponseWriter, r *http.Request) {
	h.renderPluginPage(w, r, "execution")
}

// ShowPluginMonitoring displays plugin monitoring page.
func (h *PluginHandlers) ShowPluginMonitoring(w http.ResponseWriter, r *http.Request) {
	// Backward-compatible alias for the old monitoring URL.
	h.renderPluginPage(w, r, "detection")
}

func (h *PluginHandlers) renderPluginPage(w http.ResponseWriter, r *http.Request, page string) {
	component := app.Plugin(page)
	viewCtx := layout.NewViewContext(r, dash.UsernameFromContext(r.Context()), dash.CSRFTokenFromContext(r.Context()))
	layoutComponent := layout.Layout(viewCtx, component)

	var buf bytes.Buffer
	if err := layoutComponent.Render(r.Context(), &buf); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Failed to render template: "+err.Error())
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(buf.Bytes())
}
