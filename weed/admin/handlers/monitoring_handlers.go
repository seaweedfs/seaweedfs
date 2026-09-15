package handlers

import (
	"net/http"

	"github.com/a-h/templ"
	"github.com/seaweedfs/seaweedfs/weed/admin/dash"
	"github.com/seaweedfs/seaweedfs/weed/admin/view/app"
	"github.com/seaweedfs/seaweedfs/weed/admin/view/layout"
)

// MonitoringHandlers serves the metrics monitoring pages.
type MonitoringHandlers struct {
	adminServer *dash.AdminServer
}

func NewMonitoringHandlers(adminServer *dash.AdminServer) *MonitoringHandlers {
	return &MonitoringHandlers{adminServer: adminServer}
}

func (h *MonitoringHandlers) ShowOverview(w http.ResponseWriter, r *http.Request) {
	h.render(w, r, app.Monitoring)
}

func (h *MonitoringHandlers) ShowVolumeServers(w http.ResponseWriter, r *http.Request) {
	h.render(w, r, app.MonitoringVolumeServers)
}

func (h *MonitoringHandlers) ShowFilers(w http.ResponseWriter, r *http.Request) {
	h.render(w, r, app.MonitoringFilers)
}

func (h *MonitoringHandlers) ShowS3(w http.ResponseWriter, r *http.Request) {
	h.render(w, r, app.MonitoringS3)
}

func (h *MonitoringHandlers) ShowMasters(w http.ResponseWriter, r *http.Request) {
	h.render(w, r, app.MonitoringMasters)
}

func (h *MonitoringHandlers) ShowWorkers(w http.ResponseWriter, r *http.Request) {
	h.render(w, r, app.MonitoringWorkers)
}

// render builds the monitoring data once and wraps the given page in the layout.
func (h *MonitoringHandlers) render(w http.ResponseWriter, r *http.Request, page func(dash.MonitoringData) templ.Component) {
	data := h.adminServer.GetMonitoringData()

	username := usernameOrDefault(r)
	w.Header().Set("Content-Type", "text/html")
	viewCtx := layout.NewViewContext(r, username, dash.CSRFTokenFromContext(r.Context()))
	if err := layout.Layout(viewCtx, page(*data)).Render(r.Context(), w); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Failed to render template: "+err.Error())
	}
}
