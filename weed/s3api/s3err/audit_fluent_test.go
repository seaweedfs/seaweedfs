package s3err

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/util/request_id"
	"github.com/stretchr/testify/assert"
)

func TestGetAccessLogUsesAmzRequestID(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/bucket/object", nil)
	req.Header.Set(request_id.AmzRequestIDHeader, "req-123")

	log := GetAccessLog(req, http.StatusOK, ErrNone)

	assert.Equal(t, "req-123", log.RequestID)
}
