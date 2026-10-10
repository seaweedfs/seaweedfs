package s3api

import (
	"net/http"
	"testing"

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
			name: "kms without key",
			cfg: &s3_pb.EncryptionConfiguration{
				SseAlgorithm: "aws:kms",
			},
			wantSse: "aws:kms",
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
