package s3api

import (
	"net/http"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

// IsSameObjectCopy determines if this is a same-object copy operation
func IsSameObjectCopy(r *http.Request, srcBucket, srcObject, dstBucket, dstObject string) bool {
	return srcBucket == dstBucket && srcObject == dstObject
}

// NeedsKeyRotation determines if the copy operation requires key rotation
func NeedsKeyRotation(entry *filer_pb.Entry, r *http.Request) bool {
	// Check for SSE-C key rotation
	if IsSSECEncrypted(entry.Extended) && IsSSECRequest(r) {
		return true // Assume different keys for safety
	}

	// Check for SSE-KMS key rotation
	if IsSSEKMSEncrypted(entry.Extended) && IsSSEKMSRequest(r) {
		srcKeyID, _ := GetSourceSSEKMSInfo(entry.Extended)
		dstKeyID := r.Header.Get(s3_constants.AmzServerSideEncryptionAwsKmsKeyId)
		return srcKeyID != dstKeyID
	}

	return false
}
