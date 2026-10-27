package s3api

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/s3_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// Bucket-level configuration writes against a bucket that does not exist must
// answer NoSuchBucket, not InternalError.
func TestBucketEncryptionOpsOnMissingBucket(t *testing.T) {
	filerImpl := &ambiguousPutFiler{entries: map[string]*filer_pb.Entry{}}
	s3a := newPutTestServer(t, startFakeFiler(t, filerImpl))

	if code := s3a.updateEncryptionConfiguration("missing-bucket", &s3_pb.EncryptionConfiguration{SseAlgorithm: EncryptionTypeAES256}); code != s3err.ErrNoSuchBucket {
		t.Fatalf("updateEncryptionConfiguration = %v, want %v", code, s3err.ErrNoSuchBucket)
	}
	if code := s3a.removeEncryptionConfiguration("missing-bucket"); code != s3err.ErrNoSuchBucket {
		t.Fatalf("removeEncryptionConfiguration = %v, want %v", code, s3err.ErrNoSuchBucket)
	}
	if _, code := s3a.getEncryptionConfiguration("missing-bucket"); code != s3err.ErrNoSuchBucket {
		t.Fatalf("getEncryptionConfiguration = %v, want %v", code, s3err.ErrNoSuchBucket)
	}

	// Internal readers must keep treating a missing bucket as "no encryption".
	if _, err := s3a.GetBucketEncryptionConfig("missing-bucket"); !errors.Is(err, ErrNoEncryptionConfig) {
		t.Fatalf("GetBucketEncryptionConfig = %v, want %v", err, ErrNoEncryptionConfig)
	}
}

func TestPutBucketPolicyOnMissingBucket(t *testing.T) {
	filerImpl := &ambiguousPutFiler{entries: map[string]*filer_pb.Entry{}}
	s3a := newPutTestServer(t, startFakeFiler(t, filerImpl))

	policy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::missing-bucket/*"}]}`
	r := mux.SetURLVars(httptest.NewRequest(http.MethodPut, "/missing-bucket", strings.NewReader(policy)), map[string]string{"bucket": "missing-bucket"})
	rec := httptest.NewRecorder()
	s3a.PutBucketPolicyHandler(rec, r)
	if rec.Code != http.StatusNotFound || !strings.Contains(rec.Body.String(), "NoSuchBucket") {
		t.Fatalf("status=%d body=%s, want 404 NoSuchBucket", rec.Code, rec.Body.String())
	}
}
