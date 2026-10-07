package s3api

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
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
		name      string
		headers   map[string]string
		sse       string
		sseValues []string
		dup       string
		want      s3err.ErrorCode
	}{
		{name: "no encryption", want: s3err.ErrNone},
		{name: "SSE-S3", sse: s3_constants.SSEAlgorithmAES256, want: s3err.ErrNone},
		{name: "SSE-KMS", sse: s3_constants.SSEAlgorithmKMS, want: s3err.ErrNone},
		{name: "SSE-C", headers: ssec, want: s3err.ErrNone},
		{name: "unknown algorithm", sse: "aes:kms", want: s3err.ErrInvalidEncryptionMethod},
		{name: "misspelled AES256", sse: "AES-256", want: s3err.ErrInvalidEncryptionMethod},
		{name: "SSE-C and SSE-S3", headers: ssec, sse: s3_constants.SSEAlgorithmAES256, want: s3err.ErrIncompatibleEncryptionMethod},
		{name: "SSE-C and SSE-KMS", headers: ssec, sse: s3_constants.SSEAlgorithmKMS, want: s3err.ErrIncompatibleEncryptionMethod},
		{name: "repeated algorithm header", sseValues: []string{"AES256", "aws:kms"}, want: s3err.ErrIncompatibleEncryptionMethod},
		{name: "repeated identical algorithm", sseValues: []string{"AES256", "AES256"}, want: s3err.ErrIncompatibleEncryptionMethod},
		{name: "empty value before KMS", sseValues: []string{"", "aws:kms"}, want: s3err.ErrIncompatibleEncryptionMethod},
		{name: "SSE-S3 hidden behind empty value", sseValues: []string{"", "AES256"}, headers: ssec, want: s3err.ErrIncompatibleEncryptionMethod},
		{name: "repeated SSE-C key header", headers: ssec, dup: s3_constants.AmzServerSideEncryptionCustomerKey, want: s3err.ErrIncompatibleEncryptionMethod},
		{name: "repeated KMS key id", sse: s3_constants.SSEAlgorithmKMS, dup: s3_constants.AmzServerSideEncryptionAwsKmsKeyId, want: s3err.ErrIncompatibleEncryptionMethod},
		{name: "SSE-S3 with KMS key id", sse: s3_constants.SSEAlgorithmAES256, headers: map[string]string{s3_constants.AmzServerSideEncryptionAwsKmsKeyId: "key-id"}, want: s3err.ErrIncompatibleEncryptionMethod},
		{name: "KMS key id without method", headers: map[string]string{s3_constants.AmzServerSideEncryptionAwsKmsKeyId: "key-id"}, want: s3err.ErrIncompatibleEncryptionMethod},
		{name: "KMS key id with aws:kms", sse: s3_constants.SSEAlgorithmKMS, headers: map[string]string{s3_constants.AmzServerSideEncryptionAwsKmsKeyId: "key-id"}, want: s3err.ErrNone},
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
			for _, v := range tc.sseValues {
				h.Add(s3_constants.AmzServerSideEncryption, v)
			}
			if tc.dup != "" {
				h.Add(tc.dup, "first")
				h.Add(tc.dup, "second")
			}
			assert.Equal(t, tc.want, ValidateRequestEncryption(h))
		})
	}
}

// TestDirectoryMarkerSSEC verifies marker content stored under SSE-C encrypts
// on write and decrypts on read through the stored entry metadata
func TestDirectoryMarkerSSEC(t *testing.T) {
	ssecHeaders := func(r *http.Request) {
		r.Header.Set(s3_constants.AmzServerSideEncryptionCustomerAlgorithm, "AES256")
		r.Header.Set(s3_constants.AmzServerSideEncryptionCustomerKey, "a2tra2tra2tra2tra2tra2tra2tra2tra2tra2tra2s=")
		r.Header.Set(s3_constants.AmzServerSideEncryptionCustomerKeyMD5, "mT2HRsMGJ5IX5C+0rreZ8Q==")
	}
	plaintext := []byte("directory marker content")
	s3a := &S3ApiServer{}

	putReq := httptest.NewRequest(http.MethodPut, "/bucket/dir/", nil)
	ssecHeaders(putReq)
	sseResult, errCode := s3a.handleAllSSEEncryption(putReq, bytes.NewReader(plaintext), 0)
	assert.Equal(t, s3err.ErrNone, errCode)
	encrypted, err := io.ReadAll(sseResult.DataReader)
	assert.NoError(t, err)
	assert.NotEqual(t, plaintext, encrypted)

	entry := &filer_pb.Entry{Extended: map[string][]byte{}, Content: encrypted}
	storeSSEMetadata(entry, sseResult)

	sseType := s3a.detectPrimarySSEType(entry)
	assert.Equal(t, s3_constants.SSETypeC, sseType)

	getReq := httptest.NewRequest(http.MethodGet, "/bucket/dir/", nil)
	ssecHeaders(getReq)
	decrypted, errCode := s3a.decryptDirectoryContent(getReq, entry, sseType)
	assert.Equal(t, s3err.ErrNone, errCode)
	assert.Equal(t, plaintext, decrypted)

	bareReq := httptest.NewRequest(http.MethodGet, "/bucket/dir/", nil)
	_, errCode = s3a.decryptDirectoryContent(bareReq, entry, sseType)
	assert.Equal(t, s3err.ErrSSECustomerKeyMissing, errCode)
}
