package iceberg

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// Reports carry per-file scan metrics and grow with the table. The content is
// discarded either way, so anything past this is accepted without being read
// into memory rather than rejected.
const maxMetricsReportBytes = 1 << 20

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

	// One byte past the limit distinguishes a report that fits from one that
	// was cut short, which would otherwise fail to parse and look malformed.
	body, err := io.ReadAll(io.LimitReader(r.Body, maxMetricsReportBytes+1))
	if err != nil {
		writeError(w, http.StatusBadRequest, "BadRequestException", "Invalid request body")
		return
	}
	if len(body) > maxMetricsReportBytes {
		glog.V(3).Infof("Iceberg: discarding oversized metrics report for %s.%s", flattenNamespacePath(namespace), tableName)
		w.WriteHeader(http.StatusNoContent)
		return
	}

	var report struct {
		ReportType string `json:"report-type"`
	}
	if err := json.Unmarshal(body, &report); err != nil {
		writeError(w, http.StatusBadRequest, "BadRequestException", "Invalid request body: "+err.Error())
		return
	}
	if report.ReportType == "" {
		writeError(w, http.StatusBadRequest, "BadRequestException", "report-type is required")
		return
	}

	glog.V(3).Infof("Iceberg: metrics report %q for %s.%s", report.ReportType, flattenNamespacePath(namespace), tableName)
	w.WriteHeader(http.StatusNoContent)
}
