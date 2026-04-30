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
	req = req.WithContext(request_id.Set(req.Context(), "req-123"))

	log := GetAccessLog(req, http.StatusOK, ErrNone)

	assert.Equal(t, "req-123", log.RequestID)
}

func TestGetAccessLogRemoteIP(t *testing.T) {
	tests := []struct {
		name           string
		remoteAddr     string
		xRealIP        string
		xForwardedFor  string
		expectedRemote string
	}{
		{
			name:           "falls back to RemoteAddr when no headers set",
			remoteAddr:     "10.89.0.1:35832",
			expectedRemote: "10.89.0.1:35832",
		},
		{
			name:           "uses X-Real-IP when X-Forwarded-For is absent",
			remoteAddr:     "10.89.0.1:35832",
			xRealIP:        "203.0.113.7",
			expectedRemote: "203.0.113.7",
		},
		{
			name:           "prefers X-Forwarded-For over X-Real-IP",
			remoteAddr:     "10.89.0.1:35832",
			xRealIP:        "203.0.113.7",
			xForwardedFor:  "198.51.100.42",
			expectedRemote: "198.51.100.42",
		},
		{
			name:           "uses first hop in X-Forwarded-For chain",
			remoteAddr:     "10.89.0.1:35832",
			xForwardedFor:  "198.51.100.42, 10.0.0.5, 10.89.0.1",
			expectedRemote: "198.51.100.42",
		},
		{
			name:           "skips empty leading entries in X-Forwarded-For",
			remoteAddr:     "10.89.0.1:35832",
			xForwardedFor:  ", 198.51.100.42",
			expectedRemote: "198.51.100.42",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/bucket/object", nil)
			req.RemoteAddr = tc.remoteAddr
			if tc.xRealIP != "" {
				req.Header.Set("X-Real-IP", tc.xRealIP)
			}
			if tc.xForwardedFor != "" {
				req.Header.Set("X-Forwarded-For", tc.xForwardedFor)
			}

			log := GetAccessLog(req, http.StatusOK, ErrNone)

			assert.Equal(t, tc.expectedRemote, log.RemoteIP)
		})
	}
}
