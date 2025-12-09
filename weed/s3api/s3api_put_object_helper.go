package s3api

import (
	"io"
	"net/http"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// getRequestDataReader returns the appropriate reader for the request body.
// When IAM is disabled, it still processes chunked transfer encoding for
// authTypeStreamingUnsigned to strip checksum headers and extract the actual data.
// This fixes issues where chunked data with checksums would be stored incorrectly
// when IAM is not enabled.
func getRequestDataReader(s3a *S3ApiServer, r *http.Request) (io.ReadCloser, s3err.ErrorCode) {
	var s3ErrCode s3err.ErrorCode
	dataReader := r.Body
	rAuthType := getRequestAuthType(r)
	if s3a.iam.isEnabled() {
		if rAuthType == authTypeStreamingSigned || rAuthType == authTypeStreamingUnsigned {
			dataReader, s3ErrCode = s3a.iam.newChunkedReader(r)
		}
	} else {
		switch rAuthType {
		case authTypeStreamingSigned:
			s3ErrCode = s3err.ErrAuthNotSetup
		case authTypeStreamingUnsigned:
			// Even when IAM is disabled, we still need to handle chunked transfer encoding
			// to strip checksum headers and process the data correctly
			dataReader, s3ErrCode = s3a.iam.newChunkedReader(r)
		}
	}

	return dataReader, s3ErrCode
}
