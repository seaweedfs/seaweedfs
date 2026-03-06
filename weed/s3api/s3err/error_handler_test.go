package s3err

import (
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/util/request_id"
	"github.com/stretchr/testify/assert"
)

func TestWriteErrorResponseReusesRequestID(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/bucket/object", nil)
	req = mux.SetURLVars(req, map[string]string{
		"bucket": "bucket",
		"object": "object",
	})
	req = req.WithContext(request_id.Set(req.Context(), "req-123"))

	rr := httptest.NewRecorder()
	WriteErrorResponse(rr, req, ErrNoSuchKey)

	assert.Equal(t, "req-123", rr.Header().Get(request_id.AmzRequestIDHeader))
	assert.Equal(t, "req-123", extractRequestIDFromBody(rr.Body.String()))
}

func extractRequestIDFromBody(body string) string {
	re := regexp.MustCompile(`<RequestId>([^<]+)</RequestId>`)
	matches := re.FindStringSubmatch(body)
	if len(matches) < 2 {
		return ""
	}
	return matches[1]
}
