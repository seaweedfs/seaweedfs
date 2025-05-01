package s3api

import (
	"io"
	"net/http"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

func getRequestDataReader(s3a *S3ApiServer, r *http.Request) (io.ReadCloser, s3err.ErrorCode) {
	var s3ErrCode s3err.ErrorCode
	dataReader := r.Body
	rAuthType := getRequestAuthType(r)
	if s3a.iam.isEnabled() {
		switch rAuthType {
		case authTypeStreamingSigned, authTypeStreamingUnsigned:
			dataReader, s3ErrCode = s3a.iam.newChunkedReader(r)
		case authTypeSignedV2, authTypePresignedV2:
			_, s3ErrCode = s3a.iam.isReqAuthenticatedV2(r)
		case authTypePresigned, authTypeSigned:
			_, s3ErrCode = s3a.iam.reqSignatureV4Verify(r)
		}
	} else {
		if authTypeStreamingSigned == rAuthType {
			s3ErrCode = s3err.ErrAuthNotSetup
		}
	}

	return dataReader, s3ErrCode
}
