package s3api

import "net/http"

func (s3a *S3ApiServer) StatusHandler(w http.ResponseWriter, r *http.Request) {
	// write out the response code and content type header
	writeSuccessResponseEmpty(w)
}
