package iceberg

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/mux"
)

func postMetrics(t *testing.T, body string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, "/v1/namespaces/ns/tables/t/metrics", strings.NewReader(body))
	r = mux.SetURLVars(r, map[string]string{"namespace": "ns", "table": "t"})
	w := httptest.NewRecorder()
	(&Server{}).handleReportMetrics(w, r)
	return w
}

func TestHandleReportMetricsAcceptsScanReport(t *testing.T) {
	w := postMetrics(t, `{"report-type":"scan-report","table-name":"t","snapshot-id":1}`)
	if w.Code != http.StatusNoContent {
		t.Errorf("status = %d, want %d: %s", w.Code, http.StatusNoContent, w.Body.String())
	}
}

func TestHandleReportMetricsRejectsGarbage(t *testing.T) {
	w := postMetrics(t, `not json`)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestHandleReportMetricsRequiresTable(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/v1/namespaces/ns/tables//metrics", strings.NewReader(`{}`))
	r = mux.SetURLVars(r, map[string]string{"namespace": "ns"})
	w := httptest.NewRecorder()
	(&Server{}).handleReportMetrics(w, r)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
}
