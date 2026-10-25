package s3tables

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// captureLogs swaps the two log sinks for recorders and restores them when the test ends.
func captureLogs(t *testing.T) (refusals, serverErrors *[]string) {
	t.Helper()
	var r, s []string
	oldR, oldS := logClientRefusal, logServerError
	logClientRefusal = func(f string, a ...interface{}) { r = append(r, fmt.Sprintf(f, a...)) }
	logServerError = func(f string, a ...interface{}) { s = append(s, fmt.Sprintf(f, a...)) }
	t.Cleanup(func() { logClientRefusal, logServerError = oldR, oldS })
	return &r, &s
}

// A refusal the API answers with a 4xx is not a server error.
func TestRequestRefusedWith4xxIsNotLoggedAsError(t *testing.T) {
	refusals, serverErrors := captureLogs(t)
	h := NewS3TablesHandler()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("not json"))
	req.Header.Set("X-Amz-Target", "S3Tables.ListNamespaces")
	rec := httptest.NewRecorder()
	h.HandleRequest(rec, req, nil)
	if rec.Code < 400 || rec.Code >= 500 {
		t.Fatalf("the probe request must be refused with a 4xx for this test to mean anything, got %d", rec.Code) // positive control
	}
	if len(*serverErrors) != 0 {
		t.Fatalf("a %d refusal was logged at ERROR: %v", rec.Code, *serverErrors)
	}
	if len(*refusals) != 1 || !strings.Contains((*refusals)[0], "ListNamespaces refused") {
		t.Fatalf("the refusal must still be logged once (at V(1)), got %v", *refusals)
	}
}

func TestLogRequestErrorByStatus(t *testing.T) {
	for _, c := range []struct {
		status    int
		wantError bool
	}{{http.StatusForbidden, false}, {http.StatusNotFound, false}, {http.StatusConflict, false},
		{http.StatusInternalServerError, true}, {http.StatusServiceUnavailable, true}, {0, true}} {
		refusals, serverErrors := captureLogs(t)
		logRequestError("Op", c.status, errors.New("boom"))
		if gotError := len(*serverErrors) == 1 && len(*refusals) == 0; gotError != c.wantError {
			t.Errorf("status %d: ERROR=%v, want %v (refusals %v, errors %v)", c.status, gotError, c.wantError, *refusals, *serverErrors)
		}
	}
}

func TestStatusRecorderKeepsTheFirstStatus(t *testing.T) {
	rec := httptest.NewRecorder()
	sw := &statusRecorder{ResponseWriter: rec}
	_, _ = sw.Write([]byte("x"))
	sw.WriteHeader(http.StatusInternalServerError)
	if sw.status != http.StatusOK {
		t.Fatalf("an implicit 200 by Write must win, got %d", sw.status)
	}
	sw2 := &statusRecorder{ResponseWriter: httptest.NewRecorder()}
	sw2.WriteHeader(http.StatusForbidden)
	if sw2.status != http.StatusForbidden {
		t.Fatalf("got %d", sw2.status)
	}
}

func TestStatusRecorderUnwrapsForResponseController(t *testing.T) {
	sw := &statusRecorder{ResponseWriter: httptest.NewRecorder()}
	if err := http.NewResponseController(sw).Flush(); err != nil {
		t.Fatalf("ResponseController through the recorder: %v", err)
	}
	if sw.Unwrap() == nil {
		t.Fatal("Unwrap must return the wrapped writer")
	}
}

// A malformed CreateNamespace body is answered with a 400; neither the
// dispatcher nor the handler itself may log it at ERROR.
func TestMalformedCreateNamespaceIsNotLoggedAsError(t *testing.T) {
	refusals, serverErrors := captureLogs(t)

	before := glog.Stats.Error.Lines()
	glog.Errorf("S3Tables test: positive control for the ERROR line counter")
	if glog.Stats.Error.Lines() != before+1 {
		t.Fatal("the ERROR line counter did not see a direct glog.Errorf") // positive control
	}

	h := NewS3TablesHandler()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("not json"))
	req.Header.Set("X-Amz-Target", "S3Tables.CreateNamespace")
	rec := httptest.NewRecorder()
	before = glog.Stats.Error.Lines()
	h.HandleRequest(rec, req, nil)
	errorLines := glog.Stats.Error.Lines() - before
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("a malformed body must be refused with 400, got %d", rec.Code)
	}
	if errorLines != 0 || len(*serverErrors) != 0 {
		t.Fatalf("a 400 refusal was logged at ERROR: %d direct line(s), dispatcher %v", errorLines, *serverErrors)
	}
	if len(*refusals) != 1 || !strings.Contains((*refusals)[0], "CreateNamespace refused") {
		t.Fatalf("the refusal must still be logged once (at V(1)), got %v", *refusals)
	}
}

// The recorder must not claim to flush a writer that cannot.
func TestStatusRecorderFlushFollowsTheWrappedWriter(t *testing.T) {
	type plainWriter struct{ http.ResponseWriter }
	sw := &statusRecorder{ResponseWriter: plainWriter{httptest.NewRecorder()}}
	if err := http.NewResponseController(sw).Flush(); !errors.Is(err, http.ErrNotSupported) {
		t.Fatalf("Flush through a recorder over a non-flushing writer: %v, want ErrNotSupported", err)
	}
	rec := httptest.NewRecorder()
	if err := http.NewResponseController(&statusRecorder{ResponseWriter: rec}).Flush(); err != nil || !rec.Flushed {
		t.Fatalf("Flush through a recorder over a flushing writer: err=%v flushed=%v", err, rec.Flushed) // positive control
	}
}
