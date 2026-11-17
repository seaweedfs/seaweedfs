package s3api

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestStatusHandlerFailsWhileDraining(t *testing.T) {
	s3a := &S3ApiServer{}
	probe := func() int {
		recorder := httptest.NewRecorder()
		s3a.StatusHandler(recorder, httptest.NewRequest(http.MethodGet, "/healthz", nil))
		return recorder.Code
	}
	if code := probe(); code != http.StatusOK {
		t.Fatalf("probe returned %d before draining", code)
	}
	s3a.StartDraining()
	if code := probe(); code != http.StatusServiceUnavailable {
		t.Fatalf("probe returned %d while draining, want 503", code)
	}
}
