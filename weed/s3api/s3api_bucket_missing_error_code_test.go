package s3api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Bucket encryption and policy writes used to answer 500 InternalError for a
// bucket that does not exist, because the failed filer lookup only surfaced
// from inside the configuration update, which maps every error to an internal
// one. S3 answers 404 NoSuchBucket.

func testPolicyBody(bucket string) string {
	return `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::` + bucket + `/*"}]}`
}

const testEncryptionBody = `<ServerSideEncryptionConfiguration>` +
	`<Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>AES256</SSEAlgorithm>` +
	`</ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>`

func writeSubresource(t *testing.T, filer *fakeLookupFiler, method, bucket, body string, handler func(*S3ApiServer) http.HandlerFunc) (int, string) {
	t.Helper()
	s3a := newHeadBucketTestServer(t, filer)
	req := httptest.NewRequest(method, "/"+bucket, strings.NewReader(body))
	req = mux.SetURLVars(req, map[string]string{"bucket": bucket})
	rr := httptest.NewRecorder()
	handler(s3a)(rr, req)
	return rr.Code, rr.Body.String()
}

// A missing entry is a definite answer, so every subresource write must report
// NoSuchBucket rather than InternalError.
func TestBucketSubresourceWritesOnMissingBucket(t *testing.T) {
	const bucket = "gone-bucket"

	t.Run("put bucket encryption", func(t *testing.T) {
		code, body := writeSubresource(t, &fakeLookupFiler{}, http.MethodPut, bucket, testEncryptionBody,
			func(s3a *S3ApiServer) http.HandlerFunc { return s3a.PutBucketEncryptionHandler })
		if code != http.StatusNotFound || !strings.Contains(body, "NoSuchBucket") {
			t.Fatalf("missing bucket encryption put: %d %s, want 404 NoSuchBucket", code, body)
		}
	})

	t.Run("delete bucket encryption", func(t *testing.T) {
		code, body := writeSubresource(t, &fakeLookupFiler{}, http.MethodDelete, bucket, "",
			func(s3a *S3ApiServer) http.HandlerFunc { return s3a.DeleteBucketEncryptionHandler })
		if code != http.StatusNotFound || !strings.Contains(body, "NoSuchBucket") {
			t.Fatalf("missing bucket encryption delete: %d %s, want 404 NoSuchBucket", code, body)
		}
	})

	t.Run("put bucket policy", func(t *testing.T) {
		code, body := writeSubresource(t, &fakeLookupFiler{}, http.MethodPut, bucket, testPolicyBody(bucket),
			func(s3a *S3ApiServer) http.HandlerFunc { return s3a.PutBucketPolicyHandler })
		if code != http.StatusNotFound || !strings.Contains(body, "NoSuchBucket") {
			t.Fatalf("missing bucket policy put: %d %s, want 404 NoSuchBucket", code, body)
		}
	})

	t.Run("delete bucket policy", func(t *testing.T) {
		code, body := writeSubresource(t, &fakeLookupFiler{}, http.MethodDelete, bucket, "",
			func(s3a *S3ApiServer) http.HandlerFunc { return s3a.DeleteBucketPolicyHandler })
		if code != http.StatusNotFound || !strings.Contains(body, "NoSuchBucket") {
			t.Fatalf("missing bucket policy delete: %d %s, want 404 NoSuchBucket", code, body)
		}
	})

	t.Run("get bucket encryption", func(t *testing.T) {
		code, body := writeSubresource(t, &fakeLookupFiler{}, http.MethodGet, bucket, "",
			func(s3a *S3ApiServer) http.HandlerFunc { return s3a.GetBucketEncryptionHandler })
		if code != http.StatusNotFound || !strings.Contains(body, "NoSuchBucket") {
			t.Fatalf("missing bucket encryption get: %d %s, want 404 NoSuchBucket", code, body)
		}
	})
}

// On a live bucket with no encryption configured the answer stays
// ServerSideEncryptionConfigurationNotFoundError: the miss classification must
// only rewrite the missing-bucket case.
func TestGetBucketEncryptionOnLiveBucketWithoutConfig(t *testing.T) {
	const bucket = "unencrypted-bucket"
	code, body := writeSubresource(t, &fakeLookupFiler{
		entry: &filer_pb.Entry{Name: bucket, IsDirectory: true},
	}, http.MethodGet, bucket, "",
		func(s3a *S3ApiServer) http.HandlerFunc { return s3a.GetBucketEncryptionHandler })
	if code != http.StatusNotFound || !strings.Contains(body, "ServerSideEncryptionConfigurationNotFoundError") {
		t.Fatalf("live bucket encryption get: %d %s, want 404 ServerSideEncryptionConfigurationNotFoundError", code, body)
	}
}

// A transient filer failure that arrives mid-write (after the entry's own
// lookup passed) must still be InternalError, not a definite 404.
func TestBucketSubresourceWritesOnFilerFailureMidRequest(t *testing.T) {
	const bucket = "flaky-bucket"
	entry := &filer_pb.Entry{Name: bucket, IsDirectory: true,
		Extended: map[string][]byte{BUCKET_POLICY_METADATA_KEY: []byte(testPolicyBody(bucket))}}
	filerError := status.Error(codes.Internal, "filer store unavailable")

	t.Run("put bucket policy", func(t *testing.T) {
		code, body := writeSubresource(t, &fakeLookupFiler{entry: entry, updateErr: filerError},
			http.MethodPut, bucket, testPolicyBody(bucket),
			func(s3a *S3ApiServer) http.HandlerFunc { return s3a.PutBucketPolicyHandler })
		if code != http.StatusInternalServerError || !strings.Contains(body, "InternalError") {
			t.Fatalf("mid-write filer failure policy put: %d %s, want 500 InternalError", code, body)
		}
	})

	t.Run("delete bucket policy", func(t *testing.T) {
		code, body := writeSubresource(t, &fakeLookupFiler{entry: entry, updateErr: filerError},
			http.MethodDelete, bucket, "",
			func(s3a *S3ApiServer) http.HandlerFunc { return s3a.DeleteBucketPolicyHandler })
		if code != http.StatusInternalServerError || !strings.Contains(body, "InternalError") {
			t.Fatalf("mid-write filer failure policy delete: %d %s, want 500 InternalError", code, body)
		}
	})

	t.Run("put bucket encryption", func(t *testing.T) {
		code, body := writeSubresource(t, &fakeLookupFiler{entry: entry, txnErr: filerError},
			http.MethodPut, bucket, testEncryptionBody,
			func(s3a *S3ApiServer) http.HandlerFunc { return s3a.PutBucketEncryptionHandler })
		if code != http.StatusInternalServerError || !strings.Contains(body, "InternalError") {
			t.Fatalf("mid-write filer failure encryption put: %d %s, want 500 InternalError", code, body)
		}
	})
}

// A lookup that failed for another reason must not be reported as a missing
// bucket: clients stop retrying a transient filer failure once they see a 404.
func TestBucketSubresourceWritesOnFilerFailure(t *testing.T) {
	const bucket = "unreachable-bucket"
	filerError := status.Error(codes.Internal, "filer store unavailable")

	t.Run("put bucket encryption", func(t *testing.T) {
		code, body := writeSubresource(t, &fakeLookupFiler{lookupErr: filerError}, http.MethodPut, bucket, testEncryptionBody,
			func(s3a *S3ApiServer) http.HandlerFunc { return s3a.PutBucketEncryptionHandler })
		if code != http.StatusInternalServerError || !strings.Contains(body, "InternalError") {
			t.Fatalf("filer failure encryption put: %d %s, want 500 InternalError", code, body)
		}
	})

	t.Run("delete bucket encryption", func(t *testing.T) {
		code, body := writeSubresource(t, &fakeLookupFiler{lookupErr: filerError}, http.MethodDelete, bucket, "",
			func(s3a *S3ApiServer) http.HandlerFunc { return s3a.DeleteBucketEncryptionHandler })
		if code != http.StatusInternalServerError || !strings.Contains(body, "InternalError") {
			t.Fatalf("filer failure encryption delete: %d %s, want 500 InternalError", code, body)
		}
	})

	t.Run("put bucket policy", func(t *testing.T) {
		code, body := writeSubresource(t, &fakeLookupFiler{lookupErr: filerError}, http.MethodPut, bucket, testPolicyBody(bucket),
			func(s3a *S3ApiServer) http.HandlerFunc { return s3a.PutBucketPolicyHandler })
		if code != http.StatusInternalServerError || !strings.Contains(body, "InternalError") {
			t.Fatalf("filer failure policy put: %d %s, want 500 InternalError", code, body)
		}
	})

	t.Run("delete bucket policy", func(t *testing.T) {
		code, body := writeSubresource(t, &fakeLookupFiler{lookupErr: filerError}, http.MethodDelete, bucket, "",
			func(s3a *S3ApiServer) http.HandlerFunc { return s3a.DeleteBucketPolicyHandler })
		if code != http.StatusInternalServerError || !strings.Contains(body, "InternalError") {
			t.Fatalf("filer failure policy delete: %d %s, want 500 InternalError", code, body)
		}
	})
}

// A bucket deleted between the handler's existence check and the policy
// write's own lookup still reports NoSuchBucket: the write path classifies its
// own miss.
func TestDeleteBucketPolicyOnBucketVanishingMidRequest(t *testing.T) {
	const bucket = "vanishing-bucket"
	entry := &filer_pb.Entry{Name: bucket, IsDirectory: true,
		Extended: map[string][]byte{BUCKET_POLICY_METADATA_KEY: []byte(testPolicyBody(bucket))}}
	// The delete handler looks the bucket up twice (existence check, policy
	// read) before deleteBucketPolicy's own lookup fails.
	code, body := writeSubresource(t,
		&fakeLookupFiler{entry: entry, failAfter: 2, failErr: filer_pb.ErrNotFound},
		http.MethodDelete, bucket, "",
		func(s3a *S3ApiServer) http.HandlerFunc { return s3a.DeleteBucketPolicyHandler })
	if code != http.StatusNotFound || !strings.Contains(body, "NoSuchBucket") {
		t.Fatalf("vanishing bucket policy delete: %d %s, want 404 NoSuchBucket", code, body)
	}
}

// A live bucket must get past the write: the fake filer applies the
// UpdateEntry, and the handler's IAM step tolerates a nil iam.
func TestPutBucketPolicyOnExistingBucket(t *testing.T) {
	const bucket = "live-bucket"
	code, body := writeSubresource(t, &fakeLookupFiler{
		entry: &filer_pb.Entry{Name: bucket, IsDirectory: true},
	}, http.MethodPut, bucket, testPolicyBody(bucket),
		func(s3a *S3ApiServer) http.HandlerFunc { return s3a.PutBucketPolicyHandler })

	if code != http.StatusNoContent {
		t.Fatalf("existing bucket policy put failed: %d %s, want 204", code, body)
	}
}
