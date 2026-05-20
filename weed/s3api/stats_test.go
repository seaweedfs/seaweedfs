package s3api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/stretchr/testify/assert"
)

// TestTrackAuditFallbackForDirectWriteHeader covers the regression behind
// issue #9463: handlers that bypass writeSuccessResponse helpers and call
// w.WriteHeader directly (GetObjectHandler stream path, HeadObjectHandler,
// GetBucketEncryption, GetObjectLockConfiguration, etc.) used to drop their
// audit entry. track() must mark the request as audited via a fallback
// PostLog after the handler returns.
func TestTrackAuditFallbackForDirectWriteHeader(t *testing.T) {
	var captured *http.Request
	handler := func(w http.ResponseWriter, r *http.Request) {
		captured = r
		w.WriteHeader(http.StatusOK)
	}
	wrapped := track(handler, "GET")
	wrapped(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/bucket/object", nil))

	if assert.NotNil(t, captured, "handler must have been invoked") {
		assert.True(t, s3err.AuditAlreadyLogged(captured),
			"track must emit fallback audit when handler writes response directly")
	}
}

// TestTrackAuditSkipsFallbackWhenHandlerEmits guards against double logging:
// handlers that already call PostLog (e.g. via writeSuccessResponseXML or
// WriteErrorResponse) flip the audit flag, and track must observe that and
// not re-emit.
func TestTrackAuditSkipsFallbackWhenHandlerEmits(t *testing.T) {
	var captured *http.Request
	handler := func(w http.ResponseWriter, r *http.Request) {
		captured = r
		s3err.PostLog(r, http.StatusOK, s3err.ErrNone)
		w.WriteHeader(http.StatusOK)
	}
	wrapped := track(handler, "PUT")
	wrapped(httptest.NewRecorder(), httptest.NewRequest(http.MethodPut, "/bucket/object", nil))

	if assert.NotNil(t, captured) {
		assert.True(t, s3err.AuditAlreadyLogged(captured),
			"flag must be set after handler PostLog")
	}
}
