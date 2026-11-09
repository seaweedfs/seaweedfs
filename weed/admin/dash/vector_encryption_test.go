package dash

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestVectorEncryptionChecksRemoteGateway(t *testing.T) {
	for _, test := range []struct {
		body string
		want bool
	}{{`{"sseS3":true}`, true}, {`{"sseS3":false}`, false}, {``, false}} {
		gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/status" || !r.URL.Query().Has("features") {
				t.Errorf("wrong probe: %s", r.URL)
			}
			_, _ = w.Write([]byte(test.body))
		}))
		s := &AdminServer{s3PublicEndpoint: gateway.URL}
		if got := s.vectorEncryptionConfigured(t.Context()); got != test.want {
			t.Errorf("body %q: %v", test.body, got)
		}
		gateway.Close()
	}
}
