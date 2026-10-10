package s3api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"

	"github.com/seaweedfs/seaweedfs/weed/pb/s3_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

// Multi-chunk copy paths read SSE settings from request headers only, so the
// destination bucket default must be synthesized into headers to reach them.
func TestApplyCopyBucketDefaultEncryptionHeaders(t *testing.T) {
	tests := []struct {
		name       string
		cfg        *s3_pb.EncryptionConfiguration
		wantSse    string
		wantKmsKey string
		wantBktKey string
	}{
		{name: "nil config", cfg: nil},
		{
			name: "kms with key and bucket key",
			cfg: &s3_pb.EncryptionConfiguration{
				SseAlgorithm:     "aws:kms",
				KmsKeyId:         "arn:aws:kms:us-east-1:123:key/abc",
				BucketKeyEnabled: true,
			},
			wantSse:    "aws:kms",
			wantKmsKey: "arn:aws:kms:us-east-1:123:key/abc",
			wantBktKey: "true",
		},
		{
			name: "kms without key resolves the aws/s3 default",
			cfg: &s3_pb.EncryptionConfiguration{
				SseAlgorithm: "aws:kms",
			},
			wantSse:    "aws:kms",
			wantKmsKey: "alias/aws/s3",
		},
		{
			name:    "aes256",
			cfg:     &s3_pb.EncryptionConfiguration{SseAlgorithm: "AES256"},
			wantSse: "AES256",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, _ := http.NewRequest("PUT", "/dst", nil)
			applyCopyBucketDefaultEncryptionHeaders(r, tt.cfg)
			if got := r.Header.Get(s3_constants.AmzServerSideEncryption); got != tt.wantSse {
				t.Fatalf("SSE header = %q, want %q", got, tt.wantSse)
			}
			if got := r.Header.Get(s3_constants.AmzServerSideEncryptionAwsKmsKeyId); got != tt.wantKmsKey {
				t.Fatalf("KMS key header = %q, want %q", got, tt.wantKmsKey)
			}
			if got := r.Header.Get(s3_constants.AmzServerSideEncryptionBucketKeyEnabled); got != tt.wantBktKey {
				t.Fatalf("bucket-key header = %q, want %q", got, tt.wantBktKey)
			}
		})
	}
}

// The client's encryption headers must validate before bucket defaults are
// synthesized: a KMS option without aws:kms, or a repeated header, would
// otherwise be laundered into an accepted request by the default headers.
func TestCopyObjectHandlerRejectsMalformedEncryptionBeforeDefaults(t *testing.T) {
	for _, tt := range []struct {
		name    string
		headers map[string][]string
	}{
		{
			name: "kms key id without algorithm",
			headers: map[string][]string{
				s3_constants.AmzServerSideEncryptionAwsKmsKeyId: {"arn:aws:kms:us-east-1:123:key/abc"},
			},
		},
		{
			name: "repeated algorithm header",
			headers: map[string][]string{
				s3_constants.AmzServerSideEncryption: {"AES256", "aws:kms"},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s3a := newHeadBucketTestServer(t, &fakeLookupFiler{})
			r, _ := http.NewRequest(http.MethodPut, "/dst-b/o", nil)
			r = mux.SetURLVars(r, map[string]string{"bucket": "dst-b", "object": "o"})
			r.Header.Set("X-Amz-Copy-Source", "/src-b/k")
			for name, values := range tt.headers {
				for _, v := range values {
					r.Header.Add(name, v)
				}
			}
			w := httptest.NewRecorder()
			s3a.CopyObjectHandler(w, r)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400; body: %s", w.Code, w.Body.String())
			}
		})
	}
}
