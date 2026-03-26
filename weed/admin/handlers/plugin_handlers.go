package handlers

import (
	"bytes"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/admin/dash"
	adminplugin "github.com/seaweedfs/seaweedfs/weed/admin/plugin"
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

// ShowPluginLane displays a lane overview page using the shared plugin UI.
func (h *PluginHandlers) ShowPluginLane(w http.ResponseWriter, r *http.Request) {
	h.renderPluginPageWithLane(w, r, "overview")
}

// ShowPluginLaneConfiguration displays a lane-specific configuration page.
func (h *PluginHandlers) ShowPluginLaneConfiguration(w http.ResponseWriter, r *http.Request) {
	h.renderPluginPageWithLane(w, r, "configuration")
}

// ShowPluginLaneQueue displays a lane-specific queue page.
func (h *PluginHandlers) ShowPluginLaneQueue(w http.ResponseWriter, r *http.Request) {
	h.renderPluginPageWithLane(w, r, "queue")
}

// ShowPluginLaneDetection displays a lane-specific detection page.
func (h *PluginHandlers) ShowPluginLaneDetection(w http.ResponseWriter, r *http.Request) {
	h.renderPluginPageWithLane(w, r, "detection")
}

// ShowPluginLaneExecution displays a lane-specific execution page.
func (h *PluginHandlers) ShowPluginLaneExecution(w http.ResponseWriter, r *http.Request) {
	h.renderPluginPageWithLane(w, r, "execution")
}

// ShowPluginLaneMonitoring displays a lane-specific monitoring page.
func (h *PluginHandlers) ShowPluginLaneMonitoring(w http.ResponseWriter, r *http.Request) {
	// Backward-compatible alias for the old monitoring URL.
	h.renderPluginPageWithLane(w, r, "detection")
}

// ShowPluginLaneWorkers displays workers filtered to a specific scheduler lane.
func (h *PluginHandlers) ShowPluginLaneWorkers(w http.ResponseWriter, r *http.Request) {
	// Backward-compatible alias for the old lane overview URL.
	h.renderPluginPageWithLane(w, r, "overview")
}

func (h *PluginHandlers) renderPluginPageWithLane(w http.ResponseWriter, r *http.Request, page string) {
	initialJob := r.URL.Query().Get("job")
	lane := mux.Vars(r)["lane"]
	component := app.Plugin(page, initialJob, lane)
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

func (h *PluginHandlers) renderPluginPage(w http.ResponseWriter, r *http.Request, page string) {
	initialJob := r.URL.Query().Get("job")
	lane := r.URL.Query().Get("lane")
	if lane == "" && initialJob != "" {
		// Derive lane from job type so that e.g. ?job=iceberg_maintenance
		// scopes the page to the iceberg lane automatically.
		lane = string(adminplugin.JobTypeLane(initialJob))
	}
	if lane == "" {
		lane = "default"
	}
	component := app.Plugin(page, initialJob, lane)
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
