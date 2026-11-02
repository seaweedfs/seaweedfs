package s3api

import (
	"bytes"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/s3_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
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

// A part staged under .uploads must not pick up a bucket default encryption
// change: the upload's encryption is fixed at CreateMultipartUpload, so a part
// written after PutBucketEncryption would otherwise mix plaintext and
// ciphertext parts and the completed object would fail to decrypt.
func TestPutObjectPartSkipsBucketDefaultEncryption(t *testing.T) {
	volume := startFakeVolumeServer(t)
	filerImpl := &ambiguousPutFiler{
		volume:  volume,
		entries: map[string]*filer_pb.Entry{},
		apply:   true,
	}
	s3a := newPutTestServer(t, startFakeFiler(t, filerImpl))
	s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
	s3a.bucketConfigCache.Set("b", &BucketConfig{
		Name: "b",
		Encryption: &s3_pb.EncryptionConfiguration{
			SseAlgorithm: "AES256",
		},
	})

	filePath := "/buckets/b/.uploads/up1/0002_deadbeef.part"
	r := withMultipartPart(httptest.NewRequest(http.MethodPut, "/b/o?partNumber=2&uploadId=up1", nil))
	etag, code, sseMeta := s3a.putToFiler(r, filePath, strings.NewReader("part data"), "b", "", 2, 0, nil, false, "")
	if code != s3err.ErrNone {
		t.Fatalf("putToFiler = %v, want %v", code, s3err.ErrNone)
	}
	if etag == "" {
		t.Fatal("expected non-empty etag")
	}
	if sseMeta.SSEType != "" {
		t.Fatalf("part unexpectedly encrypted, SSEType=%s", sseMeta.SSEType)
	}

	entry, ok := filerImpl.entries[filePath]
	if !ok {
		t.Fatalf("entry not found on filer at %s", filePath)
	}
	if _, has := entry.Extended[s3_constants.AmzServerSideEncryption]; has {
		t.Fatal("part entry carries SSE attributes from bucket default encryption")
	}
	for i, chunk := range entry.Chunks {
		if chunk.SseType != filer_pb.SSEType_NONE {
			t.Errorf("chunk %d: expected SseType NONE, got %v", i, chunk.SseType)
		}
	}
}

// An ordinary PutObject whose key happens to look like a staged part path
// (.uploads/...part) is still a top-level object write and must get the
// bucket's default encryption; only calls explicitly marked as part writes
// skip it.
func TestPutObjectUnderUploadsFolderKeepsBucketDefaultEncryption(t *testing.T) {
	km := GetSSES3KeyManager()
	oldSuperKey := km.superKey
	km.superKey = make([]byte, 32)
	for i := range km.superKey {
		km.superKey[i] = byte(i + 1)
	}
	t.Cleanup(func() {
		km.superKey = oldSuperKey
	})

	volume := startFakeVolumeServer(t)
	filerImpl := &ambiguousPutFiler{
		volume:  volume,
		entries: map[string]*filer_pb.Entry{},
		apply:   true,
	}
	s3a := newPutTestServer(t, startFakeFiler(t, filerImpl))
	s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
	s3a.bucketConfigCache.Set("b", &BucketConfig{
		Name: "b",
		Encryption: &s3_pb.EncryptionConfiguration{
			SseAlgorithm: "AES256",
		},
	})

	filePath := "/buckets/b/.uploads/notes.part"
	r := httptest.NewRequest(http.MethodPut, "/b/.uploads/notes.part", nil)
	etag, code, sseMeta := s3a.putToFiler(r, filePath, strings.NewReader("ordinary object body"), "b", ".uploads/notes.part", 1, 0, nil, false, "")
	if code != s3err.ErrNone {
		t.Fatalf("putToFiler = %v, want %v", code, s3err.ErrNone)
	}
	if etag == "" {
		t.Fatal("expected non-empty etag")
	}
	if sseMeta.SSEType != s3_constants.SSETypeS3 {
		t.Fatalf("expected SSE response metadata type %s, got %s", s3_constants.SSETypeS3, sseMeta.SSEType)
	}

	entry, ok := filerImpl.entries[filePath]
	if !ok {
		t.Fatalf("entry not found on filer at %s", filePath)
	}
	if !bytes.Equal(entry.Extended[s3_constants.AmzServerSideEncryption], []byte("AES256")) {
		t.Fatal("expected object entry to keep bucket default AES256 encryption")
	}
	for i, chunk := range entry.Chunks {
		if chunk.SseType != filer_pb.SSEType_SSE_S3 {
			t.Errorf("chunk %d: expected SseType SSE_S3, got %v", i, chunk.SseType)
		}
	}
}

// A vector bucket is a LANCE table bucket; the AES256 its record declares is
// the bucket's default encryption. An ICEBERG bucket's declaration is not
// applied, because its catalog reads client manifests straight from the filer.
func TestPutObjectAppliesTableBucketDeclaredEncryption(t *testing.T) {
	km := GetSSES3KeyManager()
	oldSuperKey := km.superKey
	km.superKey = make([]byte, 32)
	for i := range km.superKey {
		km.superKey[i] = byte(i + 1)
	}
	t.Cleanup(func() {
		km.superKey = oldSuperKey
	})

	volume := startFakeVolumeServer(t)
	filerImpl := &ambiguousPutFiler{
		volume:  volume,
		entries: map[string]*filer_pb.Entry{},
		apply:   true,
	}
	s3a := newPutTestServer(t, startFakeFiler(t, filerImpl))
	s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
	for bucket, format := range map[string]string{"lance": s3tables.FormatLance, "ice": s3tables.FormatIceberg} {
		s3a.bucketConfigCache.Set(bucket, s3a.newBucketConfigFromEntry(bucket, &filer_pb.Entry{
			Name: bucket,
			Extended: map[string][]byte{
				s3tables.ExtendedKeyTableBucket: []byte("true"),
				s3tables.ExtendedKeyMetadata: []byte(`{"name":"` + bucket + `","format":"` + format +
					`","encryptionConfiguration":{"sseAlgorithm":"AES256"}}`),
			},
		}))
	}

	cfg, err := s3a.GetBucketEncryptionConfig("lance")
	if err != nil || cfg.SseAlgorithm != EncryptionTypeAES256 {
		t.Fatalf("GetBucketEncryptionConfig(lance) = %v, %v; want AES256", cfg, err)
	}
	if metadata, err := s3a.GetBucketMetadata("lance"); err != nil || metadata.Encryption != nil {
		t.Fatalf("declared encryption leaked into the stored bucket metadata: %v, %v", metadata, err)
	}
	if _, err := s3a.GetBucketEncryptionConfig("ice"); !errors.Is(err, ErrNoEncryptionConfig) {
		t.Fatalf("GetBucketEncryptionConfig(ice) error = %v, want ErrNoEncryptionConfig", err)
	}
	if code := s3a.removeEncryptionConfiguration("lance"); code != s3err.ErrMethodNotAllowed {
		t.Fatalf("DeleteBucketEncryption(lance) = %v, want MethodNotAllowed for a declared encryption", code)
	}
	if code := s3a.removeEncryptionConfiguration("ice"); code != s3err.ErrNoSuchBucketEncryptionConfiguration {
		t.Fatalf("DeleteBucketEncryption(ice) = %v, want NoSuchBucketEncryptionConfiguration", code)
	}

	for bucket, want := range map[string]filer_pb.SSEType{"lance": filer_pb.SSEType_SSE_S3, "ice": filer_pb.SSEType_NONE} {
		filePath := "/buckets/" + bucket + "/vector/idx/data/0.lance"
		r := httptest.NewRequest(http.MethodPut, "/"+bucket+"/vector/idx/data/0.lance", nil)
		_, code, _ := s3a.putToFiler(r, filePath, strings.NewReader("lance data file"), bucket, "vector/idx/data/0.lance", 1, 0, nil, false, "")
		if code != s3err.ErrNone {
			t.Fatalf("putToFiler(%s) = %v, want %v", bucket, code, s3err.ErrNone)
		}
		entry := filerImpl.entries[filePath]
		if entry == nil || len(entry.Chunks) == 0 {
			t.Fatalf("no entry with chunks at %s", filePath)
		}
		for i, chunk := range entry.Chunks {
			if chunk.SseType != want {
				t.Errorf("%s chunk %d: SseType %v, want %v", bucket, i, chunk.SseType, want)
			}
		}
	}
}
