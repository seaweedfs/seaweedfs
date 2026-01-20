package s3api

import (
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"net/http"
)

func (s3a *S3ApiServer) StatusHandler(w http.ResponseWriter, r *http.Request) {
	// write out the response code and content type header
	s3err.WriteResponse(w, r, http.StatusOK, []byte{}, "")
}
