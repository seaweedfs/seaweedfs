package iceberg

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// handleReportMetrics accepts the scan and commit reports engines send after
// planning or committing. The catalog keeps no metrics store, but the endpoint
// has to exist: clients that get a 404 here log an error per query, and some
// treat repeated failures as a broken catalog.
func (s *Server) handleReportMetrics(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	namespace := parseNamespace(vars["namespace"])
	tableName := vars["table"]

	if len(namespace) == 0 || tableName == "" {
		writeError(w, http.StatusBadRequest, "BadRequestException", "Namespace and table name are required")
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, maxMetricsReportBytes))
	if err != nil {
		writeError(w, http.StatusBadRequest, "BadRequestException", "Invalid request body")
		return
	}

	var report struct {
		ReportType string `json:"report-type"`
	}
	if len(body) > 0 {
		if err := json.Unmarshal(body, &report); err != nil {
			writeError(w, http.StatusBadRequest, "BadRequestException", "Invalid request body: "+err.Error())
			return
		}
	}

	glog.V(3).Infof("Iceberg: metrics report %q for %s.%s", report.ReportType, flattenNamespacePath(namespace), tableName)
	w.WriteHeader(http.StatusNoContent)
}

// Reports carry per-file scan metrics and can grow with the table; read a
// bounded amount since the content is discarded anyway.
const maxMetricsReportBytes = 1 << 20
