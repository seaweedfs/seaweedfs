package s3api

import (
	"net/http"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

func (s3a *S3ApiServer) StatusHandler(w http.ResponseWriter, r *http.Request) {
	// write out the response code and content type header
	s3err.WriteResponse(w, r, http.StatusOK, []byte{}, "")
}
