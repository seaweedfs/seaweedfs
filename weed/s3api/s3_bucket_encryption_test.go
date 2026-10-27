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
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

// A bucket deleted between the policy write's own lookup and its UpdateEntry
// surfaces as a filer NotFound; the answer stays NoSuchBucket, not
// InternalError.
func TestBucketPolicyOpsOnVanishedUpdateEntry(t *testing.T) {
	const bucket = "vanished-update-bucket"
	policy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::vanished-update-bucket/*"}]}`
	newFiler := func() *ambiguousPutFiler {
		return &ambiguousPutFiler{
			entries: map[string]*filer_pb.Entry{
				"/buckets/" + bucket: {
					Name:        bucket,
					IsDirectory: true,
					Extended:    map[string][]byte{BUCKET_POLICY_METADATA_KEY: []byte(policy)},
				},
			},
			updateErr:     status.Error(codes.NotFound, "not found /buckets/"+bucket),
			updateFailKey: "/buckets/" + bucket,
		}
	}

	t.Run("put", func(t *testing.T) {
		s3a := newPutTestServer(t, startFakeFiler(t, newFiler()))
		r := mux.SetURLVars(httptest.NewRequest(http.MethodPut, "/"+bucket, strings.NewReader(policy)), map[string]string{"bucket": bucket})
		rec := httptest.NewRecorder()
		s3a.PutBucketPolicyHandler(rec, r)
		if rec.Code != http.StatusNotFound || !strings.Contains(rec.Body.String(), "NoSuchBucket") {
			t.Fatalf("status=%d body=%s, want 404 NoSuchBucket", rec.Code, rec.Body.String())
		}
	})

	t.Run("delete", func(t *testing.T) {
		s3a := newPutTestServer(t, startFakeFiler(t, newFiler()))
		r := mux.SetURLVars(httptest.NewRequest(http.MethodDelete, "/"+bucket, nil), map[string]string{"bucket": bucket})
		rec := httptest.NewRecorder()
		s3a.DeleteBucketPolicyHandler(rec, r)
		if rec.Code != http.StatusNotFound || !strings.Contains(rec.Body.String(), "NoSuchBucket") {
			t.Fatalf("status=%d body=%s, want 404 NoSuchBucket", rec.Code, rec.Body.String())
		}
	})
}

// The bucket patch transaction carries an existence condition: a PATCH on an
// absent entry is otherwise a server-side no-op, so without it a bucket
// deleted between the metadata read and the patch would acknowledge a config
// write that never landed. The failed condition reports NoSuchBucket.
func TestBucketConfigWritesOnVanishedPatch(t *testing.T) {
	const bucket = "vanished-patch-bucket"
	bucketKey := "/buckets/" + bucket
	newFiler := func(sawCondition *bool) *ambiguousPutFiler {
		f := &ambiguousPutFiler{
			entries: map[string]*filer_pb.Entry{
				bucketKey: {Name: bucket, IsDirectory: true},
			},
		}
		f.txnCheck = func(req *filer_pb.ObjectTransactionRequest) {
			for _, c := range req.GetCondition().GetClauses() {
				if c.GetKind() == filer_pb.WriteCondition_IF_EXISTS {
					*sawCondition = true
				}
			}
			delete(f.entries, bucketKey)
		}
		return f
	}

	t.Run("put bucket encryption", func(t *testing.T) {
		var sawCondition bool
		s3a := newPutTestServer(t, startFakeFiler(t, newFiler(&sawCondition)))
		body := `<ServerSideEncryptionConfiguration>` +
			`<Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>AES256</SSEAlgorithm>` +
			`</ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>`
		r := mux.SetURLVars(httptest.NewRequest(http.MethodPut, "/"+bucket, strings.NewReader(body)), map[string]string{"bucket": bucket})
		rec := httptest.NewRecorder()
		s3a.PutBucketEncryptionHandler(rec, r)
		if rec.Code != http.StatusNotFound || !strings.Contains(rec.Body.String(), "NoSuchBucket") {
			t.Fatalf("status=%d body=%s, want 404 NoSuchBucket", rec.Code, rec.Body.String())
		}
		if !sawCondition {
			t.Fatal("bucket patch missing the existence condition")
		}
	})

	t.Run("update bucket config", func(t *testing.T) {
		var sawCondition bool
		s3a := newPutTestServer(t, startFakeFiler(t, newFiler(&sawCondition)))
		if code := s3a.setBucketVersioningStatus(bucket, s3_constants.VersioningEnabled); code != s3err.ErrNoSuchBucket {
			t.Fatalf("setBucketVersioningStatus = %v, want %v", code, s3err.ErrNoSuchBucket)
		}
		if !sawCondition {
			t.Fatal("bucket patch missing the existence condition")
		}
	})

	// Rewriting the stored status produces an empty patch; it must still carry
	// the existence condition rather than skip the transaction.
	t.Run("unchanged config value", func(t *testing.T) {
		var sawEmptyPatch bool
		f := &ambiguousPutFiler{
			entries: map[string]*filer_pb.Entry{
				bucketKey: {
					Name:        bucket,
					IsDirectory: true,
					Extended:    map[string][]byte{s3_constants.ExtVersioningKey: []byte(s3_constants.VersioningEnabled)},
				},
			},
		}
		f.txnCheck = func(req *filer_pb.ObjectTransactionRequest) {
			for _, c := range req.GetCondition().GetClauses() {
				if c.GetKind() == filer_pb.WriteCondition_IF_EXISTS && len(req.Mutations) == 0 {
					sawEmptyPatch = true
				}
			}
			delete(f.entries, bucketKey)
		}
		s3a := newPutTestServer(t, startFakeFiler(t, f))
		if code := s3a.setBucketVersioningStatus(bucket, s3_constants.VersioningEnabled); code != s3err.ErrNoSuchBucket {
			t.Fatalf("setBucketVersioningStatus = %v, want %v", code, s3err.ErrNoSuchBucket)
		}
		if !sawEmptyPatch {
			t.Fatal("empty patch skipped the existence-condition transaction")
		}
	})
}
