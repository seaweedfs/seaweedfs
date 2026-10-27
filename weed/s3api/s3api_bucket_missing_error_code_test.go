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

// Issue 12131: PUT/DELETE bucket encryption and PUT bucket policy answered 500
// InternalError for a bucket that does not exist, because the failed filer
// lookup only surfaced from inside the configuration update, which maps every
// error to an internal one. S3 answers 404 NoSuchBucket.

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
}

// A bucket deleted between the existence check and the write itself still
// reports NoSuchBucket: the write path classifies its own miss.
func TestBucketSubresourceWritesOnBucketVanishingMidRequest(t *testing.T) {
	const bucket = "vanishing-bucket"
	entry := &filer_pb.Entry{Name: bucket, IsDirectory: true}

	t.Run("put bucket encryption", func(t *testing.T) {
		code, body := writeSubresource(t, &fakeLookupFiler{entry: entry, failAfter: 1, failErr: filer_pb.ErrNotFound},
			http.MethodPut, bucket, testEncryptionBody,
			func(s3a *S3ApiServer) http.HandlerFunc { return s3a.PutBucketEncryptionHandler })
		if code != http.StatusNotFound || !strings.Contains(body, "NoSuchBucket") {
			t.Fatalf("vanishing bucket encryption put: %d %s, want 404 NoSuchBucket", code, body)
		}
	})

	t.Run("delete bucket encryption", func(t *testing.T) {
		code, body := writeSubresource(t, &fakeLookupFiler{entry: entry, failAfter: 1, failErr: filer_pb.ErrNotFound},
			http.MethodDelete, bucket, "",
			func(s3a *S3ApiServer) http.HandlerFunc { return s3a.DeleteBucketEncryptionHandler })
		if code != http.StatusNotFound || !strings.Contains(body, "NoSuchBucket") {
			t.Fatalf("vanishing bucket encryption delete: %d %s, want 404 NoSuchBucket", code, body)
		}
	})

	t.Run("put bucket policy", func(t *testing.T) {
		code, body := writeSubresource(t, &fakeLookupFiler{entry: entry, failAfter: 1, failErr: filer_pb.ErrNotFound},
			http.MethodPut, bucket, testPolicyBody(bucket),
			func(s3a *S3ApiServer) http.HandlerFunc { return s3a.PutBucketPolicyHandler })
		if code != http.StatusNotFound || !strings.Contains(body, "NoSuchBucket") {
			t.Fatalf("vanishing bucket policy put: %d %s, want 404 NoSuchBucket", code, body)
		}
	})

	t.Run("delete bucket policy", func(t *testing.T) {
		code, body := writeSubresource(t, &fakeLookupFiler{entry: entry, failAfter: 1, failErr: filer_pb.ErrNotFound},
			http.MethodDelete, bucket, "",
			func(s3a *S3ApiServer) http.HandlerFunc { return s3a.DeleteBucketPolicyHandler })
		if code != http.StatusNotFound || !strings.Contains(body, "NoSuchBucket") {
			t.Fatalf("vanishing bucket policy delete: %d %s, want 404 NoSuchBucket", code, body)
		}
	})
}

// A transient filer failure that arrives mid-write (after the existence check
// passed) must still be InternalError, not a definite 404.
func TestBucketSubresourceWritesOnFilerFailureMidRequest(t *testing.T) {
	const bucket = "flaky-bucket"
	entry := &filer_pb.Entry{Name: bucket, IsDirectory: true}
	filerError := status.Error(codes.Internal, "filer store unavailable")

	t.Run("put bucket encryption", func(t *testing.T) {
		code, body := writeSubresource(t, &fakeLookupFiler{entry: entry, failAfter: 1, failErr: filerError},
			http.MethodPut, bucket, testEncryptionBody,
			func(s3a *S3ApiServer) http.HandlerFunc { return s3a.PutBucketEncryptionHandler })
		if code != http.StatusInternalServerError || !strings.Contains(body, "InternalError") {
			t.Fatalf("mid-write filer failure encryption put: %d %s, want 500 InternalError", code, body)
		}
	})

	t.Run("put bucket policy", func(t *testing.T) {
		code, body := writeSubresource(t, &fakeLookupFiler{entry: entry, failAfter: 1, failErr: filerError},
			http.MethodPut, bucket, testPolicyBody(bucket),
			func(s3a *S3ApiServer) http.HandlerFunc { return s3a.PutBucketPolicyHandler })
		if code != http.StatusInternalServerError || !strings.Contains(body, "InternalError") {
			t.Fatalf("mid-write filer failure policy put: %d %s, want 500 InternalError", code, body)
		}
	})

	t.Run("delete bucket policy", func(t *testing.T) {
		code, body := writeSubresource(t, &fakeLookupFiler{entry: entry, failAfter: 1, failErr: filerError},
			http.MethodDelete, bucket, "",
			func(s3a *S3ApiServer) http.HandlerFunc { return s3a.DeleteBucketPolicyHandler })
		if code != http.StatusInternalServerError || !strings.Contains(body, "InternalError") {
			t.Fatalf("mid-write filer failure policy delete: %d %s, want 500 InternalError", code, body)
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
}

// The check must not reject a live bucket: with an entry present the write gets
// past it. The fake filer has no UpdateEntry, so only the existence decision is
// under test here.
func TestPutBucketPolicyOnExistingBucketPassesExistenceCheck(t *testing.T) {
	const bucket = "live-bucket"
	code, body := writeSubresource(t, &fakeLookupFiler{
		entry: &filer_pb.Entry{Name: bucket, IsDirectory: true},
	}, http.MethodPut, bucket, testPolicyBody(bucket),
		func(s3a *S3ApiServer) http.HandlerFunc { return s3a.PutBucketPolicyHandler })

	if code == http.StatusNotFound && strings.Contains(body, "NoSuchBucket") {
		t.Fatalf("existing bucket was rejected as missing: %d %s", code, body)
	}
}
