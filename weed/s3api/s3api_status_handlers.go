package s3api

import (
	"encoding/json"
	"net/http"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

func (s3a *S3ApiServer) StatusHandler(w http.ResponseWriter, r *http.Request) {
	if _, features := r.URL.Query()["features"]; features {
		body, _ := json.Marshal(map[string]bool{"sseS3": GetSSES3KeyManager().IsConfigured()})
		s3err.WriteResponse(w, r, http.StatusOK, body, "application/json")
		return
	}
	if s3a.draining.Load() {
		s3err.WriteResponse(w, r, http.StatusServiceUnavailable, []byte{}, "")
		return
	}
	// write out the response code and content type header
	s3err.WriteResponse(w, r, http.StatusOK, []byte{}, "")
}

// StartDraining makes the health probes fail so load balancers stop routing
// new requests here, while requests that still arrive are served as usual.
func (s3a *S3ApiServer) StartDraining() {
	s3a.draining.Store(true)
}
