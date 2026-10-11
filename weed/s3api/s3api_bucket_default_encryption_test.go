package s3api

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/s3_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// Issue 11647: Bucket default encryption (SSE-S3) must be applied when
// -s3.encryptVolumeData (s3a.cipher) is enabled, matching explicit SSE headers.
func TestPutObjectAppliesBucketDefaultEncryptionWithVolumeCipher(t *testing.T) {
	// Configure test key manager with a super key for SSE-S3 encryption.
	km := GetSSES3KeyManager()
	oldSuperKey := km.superKey
	km.superKey = make([]byte, 32)
	for i := range km.superKey {
		km.superKey[i] = byte(i + 1)
	}
	t.Cleanup(func() {
		km.superKey = oldSuperKey
	})

	testCases := []struct {
		name         string
		enableCipher bool
	}{
		{
			name:         "volume encryption enabled (s3a.cipher=true)",
			enableCipher: true,
		},
		{
			name:         "volume encryption disabled (s3a.cipher=false)",
			enableCipher: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			volume := startFakeVolumeServer(t)
			filerImpl := &ambiguousPutFiler{
				volume:  volume,
				entries: map[string]*filer_pb.Entry{},
				apply:   true,
			}
			s3a := newPutTestServer(t, startFakeFiler(t, filerImpl))
			s3a.cipher = tc.enableCipher

			// Configure bucket "b" with default AES256 (SSE-S3) encryption.
			s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
			s3a.bucketConfigCache.Set("b", &BucketConfig{
				Name: "b",
				Encryption: &s3_pb.EncryptionConfiguration{
					SseAlgorithm: "AES256",
				},
			})

			// PUT request without explicit SSE headers.
			r := httptest.NewRequest(http.MethodPut, "/b/plain.txt", nil)
			filePath := "/buckets/b/plain.txt"
			etag, code, sseMeta := s3a.putToFiler(r, filePath, strings.NewReader("hello seaweedfs"), "b", "plain.txt", 1, 0, nil, false, "")
			if code != s3err.ErrNone {
				t.Fatalf("putToFiler returned error code %v, want %v", code, s3err.ErrNone)
			}
			if etag == "" {
				t.Fatal("expected non-empty etag")
			}

			// Verify returned SSE response metadata.
			if sseMeta.SSEType != s3_constants.SSETypeS3 {
				t.Fatalf("expected SSE response metadata type %s, got %s", s3_constants.SSETypeS3, sseMeta.SSEType)
			}

			// Verify entry saved on the filer.
			entry, ok := filerImpl.entries[filePath]
			if !ok {
				t.Fatalf("entry not found on filer at %s", filePath)
			}

			sseHeaderVal, hasSSE := entry.Extended[s3_constants.AmzServerSideEncryption]
			if !hasSSE || !bytes.Equal(sseHeaderVal, []byte("AES256")) {
				t.Fatalf("expected entry to have %s=AES256, got hasSSE=%v val=%s", s3_constants.AmzServerSideEncryption, hasSSE, string(sseHeaderVal))
			}

			if len(entry.Extended[s3_constants.SeaweedFSSSES3Key]) == 0 {
				t.Fatal("expected entry to have stored SSE-S3 key metadata")
			}

			if len(entry.Chunks) == 0 {
				t.Fatal("expected entry to have at least one chunk")
			}

			for i, chunk := range entry.Chunks {
				if chunk.SseType != filer_pb.SSEType_SSE_S3 {
					t.Errorf("chunk %d: expected SseType SSE_S3, got %v", i, chunk.SseType)
				}
				if len(chunk.SseMetadata) == 0 {
					t.Errorf("chunk %d: expected non-empty SseMetadata", i)
				}
				if tc.enableCipher && len(chunk.CipherKey) == 0 {
					t.Errorf("chunk %d: expected volume CipherKey when s3a.cipher is true", i)
				}
				if !tc.enableCipher && len(chunk.CipherKey) != 0 {
					t.Errorf("chunk %d: expected empty volume CipherKey when s3a.cipher is false", i)
				}
			}
		})
	}
}
