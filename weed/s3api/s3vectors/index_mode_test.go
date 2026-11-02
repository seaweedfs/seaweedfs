package s3vectors

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables/s3tablestest"
)

func withClassicCutoff(t *testing.T, cutoff time.Time) {
	t.Helper()
	saved := classicIndexModeCutoff
	classicIndexModeCutoff = cutoff
	t.Cleanup(func() { classicIndexModeCutoff = saved })
}

func getBucketDefaultIndexMode(t *testing.T, s *Server, bucket string) string {
	t.Helper()
	w := postJSON(t, s.handleGetVectorBucket, &GetVectorBucketRequest{VectorBucketName: bucket})
	if w.Code != http.StatusOK {
		t.Fatalf("GetVectorBucket: %d %s", w.Code, w.Body.String())
	}
	var resp GetVectorBucketResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode GetVectorBucket: %v", err)
	}
	return resp.VectorBucket.DefaultIndexMode
}

func TestPutVectorBucketDefaultIndexMode(t *testing.T) {
	fs := s3tablestest.Start(t)
	s := NewServer(s3tables.NewManagerClient(fs.Client), allowAllAuth{}, newFakeVectors(0))
	if w := postJSON(t, s.handleCreateVectorBucket, &CreateVectorBucketRequest{VectorBucketName: "vec-bucket"}); w.Code != http.StatusOK {
		t.Fatalf("create bucket: %d %s", w.Code, w.Body.String())
	}
	if got := getBucketDefaultIndexMode(t, s, "vec-bucket"); got != indexModeEnhanced {
		t.Fatalf("new bucket defaultIndexMode = %q, want ENHANCED", got)
	}

	put := func(req *PutVectorBucketDefaultIndexModeRequest) (int, string) {
		w := postJSON(t, s.handlePutVectorBucketDefaultIndexMode, req)
		return w.Code, w.Body.String()
	}
	if code, body := put(&PutVectorBucketDefaultIndexModeRequest{VectorBucketName: "vec-bucket", DefaultIndexMode: "FAST"}); code != http.StatusBadRequest || !strings.Contains(body, "ValidationException") {
		t.Fatalf("unknown mode: %d %s, want 400 ValidationException", code, body)
	}
	if code, body := put(&PutVectorBucketDefaultIndexModeRequest{VectorBucketName: "vec-bucket"}); code != http.StatusBadRequest {
		t.Fatalf("missing mode: %d %s, want 400", code, body)
	}
	if code, body := put(&PutVectorBucketDefaultIndexModeRequest{VectorBucketName: "vec-bucket", DefaultIndexMode: indexModeClassic}); code != http.StatusBadRequest || !strings.Contains(body, "September 30, 2026") {
		t.Fatalf("CLASSIC on a new bucket: %d %s, want 400 naming the cutoff", code, body)
	}
	if code, body := put(&PutVectorBucketDefaultIndexModeRequest{VectorBucketName: "missing", DefaultIndexMode: indexModeEnhanced}); code != http.StatusNotFound {
		t.Fatalf("missing bucket: %d %s, want 404", code, body)
	}

	withClassicCutoff(t, time.Now().Add(time.Hour))
	if code, body := put(&PutVectorBucketDefaultIndexModeRequest{VectorBucketArn: s.vectorBucketArn("vec-bucket"), DefaultIndexMode: indexModeClassic}); code != http.StatusOK {
		t.Fatalf("CLASSIC on an older bucket: %d %s", code, body)
	}
	if got := getBucketDefaultIndexMode(t, s, "vec-bucket"); got != indexModeClassic {
		t.Fatalf("defaultIndexMode after put = %q, want CLASSIC", got)
	}
	if code, body := put(&PutVectorBucketDefaultIndexModeRequest{VectorBucketName: "vec-bucket", DefaultIndexMode: indexModeEnhanced}); code != http.StatusOK {
		t.Fatalf("back to ENHANCED: %d %s", code, body)
	}
	if got := getBucketDefaultIndexMode(t, s, "vec-bucket"); got != indexModeEnhanced {
		t.Fatalf("defaultIndexMode after reset = %q, want ENHANCED", got)
	}
}

func TestPutVectorBucketDefaultIndexModeNeedsItsOwnGrant(t *testing.T) {
	fs := s3tablestest.Start(t)
	s := NewServer(s3tables.NewManagerClient(fs.Client), allowAllAuth{}, newFakeVectors(0))
	if w := postJSONAs(t, "", s.handleCreateVectorBucket, &CreateVectorBucketRequest{VectorBucketName: "vec-bucket"}); w.Code != http.StatusOK {
		t.Fatalf("create bucket: %d %s", w.Code, w.Body.String())
	}
	grant := func(action string) {
		t.Helper()
		policy := fmt.Sprintf(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"%s","Resource":"%s"}]}`,
			action, s.vectorBucketArn("vec-bucket"))
		if w := postJSONAs(t, "", s.handlePutVectorBucketPolicy, &PutVectorBucketPolicyRequest{VectorBucketName: "vec-bucket", Policy: policy}); w.Code != http.StatusOK {
			t.Fatalf("put policy: %d %s", w.Code, w.Body.String())
		}
	}
	put := func() int {
		return postJSONAs(t, "alice", s.handlePutVectorBucketDefaultIndexMode, &PutVectorBucketDefaultIndexModeRequest{
			VectorBucketName: "vec-bucket", DefaultIndexMode: indexModeEnhanced,
		}).Code
	}
	grant("s3vectors:GetVectorBucket")
	if code := put(); code != http.StatusForbidden {
		t.Fatalf("put under a read grant: %d, want 403", code)
	}
	grant("s3vectors:PutVectorBucketDefaultIndexMode")
	if code := put(); code != http.StatusOK {
		t.Fatalf("put under its own grant: %d, want 200", code)
	}
}
