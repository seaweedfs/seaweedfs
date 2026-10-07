package s3api

import (
	"net/http"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/stretchr/testify/assert"
)

// TestValidateRequestEncryption verifies that a PutObject or
// CreateMultipartUpload request asking for an encryption method that cannot be
// honored is rejected instead of being stored unencrypted
func TestValidateRequestEncryption(t *testing.T) {
	ssec := map[string]string{
		s3_constants.AmzServerSideEncryptionCustomerAlgorithm: "AES256",
		s3_constants.AmzServerSideEncryptionCustomerKey:       "a2tra2tra2tra2tra2tra2tra2tra2tra2tra2tra2s=",
		s3_constants.AmzServerSideEncryptionCustomerKeyMD5:    "f6OQvGsmFBq4WOqaVcuO5w==",
	}
	testCases := []struct {
		name    string
		headers map[string]string
		sse     string
		want    s3err.ErrorCode
	}{
		{name: "no encryption", want: s3err.ErrNone},
		{name: "SSE-S3", sse: s3_constants.SSEAlgorithmAES256, want: s3err.ErrNone},
		{name: "SSE-KMS", sse: s3_constants.SSEAlgorithmKMS, want: s3err.ErrNone},
		{name: "SSE-C", headers: ssec, want: s3err.ErrNone},
		{name: "unknown algorithm", sse: "aes:kms", want: s3err.ErrInvalidEncryptionMethod},
		{name: "misspelled AES256", sse: "AES-256", want: s3err.ErrInvalidEncryptionMethod},
		{name: "SSE-C and SSE-S3", headers: ssec, sse: s3_constants.SSEAlgorithmAES256, want: s3err.ErrIncompatibleEncryptionMethod},
		{name: "SSE-C and SSE-KMS", headers: ssec, sse: s3_constants.SSEAlgorithmKMS, want: s3err.ErrIncompatibleEncryptionMethod},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			h := http.Header{}
			for k, v := range tc.headers {
				h.Set(k, v)
			}
			if tc.sse != "" {
				h.Set(s3_constants.AmzServerSideEncryption, tc.sse)
			}
			assert.Equal(t, tc.want, ValidateRequestEncryption(h))
		})
	}
}
