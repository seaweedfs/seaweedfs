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

func getIndex(t *testing.T, s *Server, bucket, index string) *Index {
	t.Helper()
	w := postJSON(t, s.handleGetIndex, &GetIndexRequest{VectorBucketName: bucket, IndexName: index})
	if w.Code != http.StatusOK {
		t.Fatalf("GetIndex %s: %d %s", index, w.Code, w.Body.String())
	}
	var resp GetIndexResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode GetIndex: %v", err)
	}
	return resp.Index
}

func createIndex(t *testing.T, s *Server, bucket, index string) {
	t.Helper()
	if w := postJSON(t, s.handleCreateIndex, &CreateIndexRequest{
		VectorBucketName:      bucket,
		IndexName:             index,
		DataType:              "float32",
		Dimension:             3,
		DistanceMetric:        "cosine",
		MetadataConfiguration: &MetadataConfiguration{NonFilterableMetadataKeys: []string{"body"}},
	}); w.Code != http.StatusOK {
		t.Fatalf("create index %s: %d %s", index, w.Code, w.Body.String())
	}
}

func TestUpdateIndexMode(t *testing.T) {
	fs := s3tablestest.Start(t)
	s := NewServer(s3tables.NewManagerClient(fs.Client), allowAllAuth{}, newFakeVectors(0))
	if w := postJSON(t, s.handleCreateVectorBucket, &CreateVectorBucketRequest{VectorBucketName: "vec-bucket"}); w.Code != http.StatusOK {
		t.Fatalf("create bucket: %d %s", w.Code, w.Body.String())
	}
	createIndex(t, s, "vec-bucket", "idx")
	if got := getIndex(t, s, "vec-bucket", "idx").IndexMode; got != indexModeEnhanced {
		t.Fatalf("new index mode = %q, want ENHANCED", got)
	}

	update := func(req *UpdateIndexModeRequest) (int, string) {
		w := postJSON(t, s.handleUpdateIndexMode, req)
		return w.Code, w.Body.String()
	}
	if code, body := update(&UpdateIndexModeRequest{VectorBucketName: "vec-bucket", IndexName: "idx", IndexMode: "classic"}); code != http.StatusBadRequest {
		t.Fatalf("lowercase mode: %d %s, want 400", code, body)
	}
	if code, body := update(&UpdateIndexModeRequest{VectorBucketName: "vec-bucket", IndexName: "idx", IndexMode: indexModeClassic}); code != http.StatusBadRequest || !strings.Contains(body, "September 30, 2026") {
		t.Fatalf("CLASSIC on a new bucket: %d %s, want 400 naming the cutoff", code, body)
	}
	if code, body := update(&UpdateIndexModeRequest{VectorBucketName: "vec-bucket", IndexName: "missing", IndexMode: indexModeEnhanced}); code != http.StatusNotFound {
		t.Fatalf("missing index: %d %s, want 404", code, body)
	}

	withClassicCutoff(t, time.Now().Add(time.Hour))
	if code, body := update(&UpdateIndexModeRequest{IndexArn: s.indexArn("vec-bucket", "idx"), IndexMode: indexModeClassic}); code != http.StatusOK {
		t.Fatalf("CLASSIC on an older bucket: %d %s", code, body)
	}
	idx := getIndex(t, s, "vec-bucket", "idx")
	if idx.IndexMode != indexModeClassic || idx.Dimension != 3 || idx.DistanceMetric != "cosine" ||
		idx.MetadataConfiguration == nil || len(idx.MetadataConfiguration.NonFilterableMetadataKeys) != 1 {
		t.Fatalf("index after update = %+v, want CLASSIC with its settings kept", idx)
	}
	if code, body := update(&UpdateIndexModeRequest{VectorBucketName: "vec-bucket", IndexName: "idx", IndexMode: indexModeClassic}); code != http.StatusOK {
		t.Fatalf("repeated update: %d %s", code, body)
	}

	// New indexes inherit the bucket default; existing ones keep their mode.
	if w := postJSON(t, s.handlePutVectorBucketDefaultIndexMode, &PutVectorBucketDefaultIndexModeRequest{
		VectorBucketName: "vec-bucket", DefaultIndexMode: indexModeClassic,
	}); w.Code != http.StatusOK {
		t.Fatalf("put default: %d %s", w.Code, w.Body.String())
	}
	createIndex(t, s, "vec-bucket", "inherits")
	if got := getIndex(t, s, "vec-bucket", "inherits").IndexMode; got != indexModeClassic {
		t.Fatalf("index created under a CLASSIC default = %q, want CLASSIC", got)
	}
	if code, body := update(&UpdateIndexModeRequest{VectorBucketName: "vec-bucket", IndexName: "idx", IndexMode: indexModeEnhanced}); code != http.StatusOK {
		t.Fatalf("back to ENHANCED: %d %s", code, body)
	}
	if got := getIndex(t, s, "vec-bucket", "inherits").IndexMode; got != indexModeClassic {
		t.Fatalf("updating one index changed another to %q", got)
	}
	if got := getBucketDefaultIndexMode(t, s, "vec-bucket"); got != indexModeClassic {
		t.Fatalf("updating an index changed the bucket default to %q", got)
	}

	if w := postJSON(t, s.handleDeleteIndex, &DeleteIndexRequest{VectorBucketName: "vec-bucket", IndexName: "idx"}); w.Code != http.StatusOK {
		t.Fatalf("delete updated index: %d %s", w.Code, w.Body.String())
	}
}

func TestUpdateIndexModeNeedsItsOwnGrant(t *testing.T) {
	fs := s3tablestest.Start(t)
	s := NewServer(s3tables.NewManagerClient(fs.Client), allowAllAuth{}, newFakeVectors(0))
	if w := postJSONAs(t, "", s.handleCreateVectorBucket, &CreateVectorBucketRequest{VectorBucketName: "vec-bucket"}); w.Code != http.StatusOK {
		t.Fatalf("create bucket: %d %s", w.Code, w.Body.String())
	}
	createIndex(t, s, "vec-bucket", "idx")
	grant := func(action string) {
		t.Helper()
		policy := fmt.Sprintf(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"%s","Resource":"%s"}]}`,
			action, s.indexArn("vec-bucket", "idx"))
		if w := postJSONAs(t, "", s.handlePutVectorBucketPolicy, &PutVectorBucketPolicyRequest{VectorBucketName: "vec-bucket", Policy: policy}); w.Code != http.StatusOK {
			t.Fatalf("put policy: %d %s", w.Code, w.Body.String())
		}
	}
	update := func() int {
		return postJSONAs(t, "alice", s.handleUpdateIndexMode, &UpdateIndexModeRequest{
			VectorBucketName: "vec-bucket", IndexName: "idx", IndexMode: indexModeEnhanced,
		}).Code
	}
	grant("s3vectors:GetIndex")
	if code := update(); code != http.StatusForbidden {
		t.Fatalf("update under a read grant: %d, want 403", code)
	}
	grant("s3vectors:UpdateIndexMode")
	if code := update(); code != http.StatusOK {
		t.Fatalf("update under its own grant: %d, want 200", code)
	}
}

func TestIndexFromRecordModeDefaultsToEnhanced(t *testing.T) {
	s := &Server{}
	for _, tc := range []struct {
		record map[string]interface{}
		want   string
	}{
		{map[string]interface{}{}, indexModeEnhanced},
		{map[string]interface{}{"metadata": map[string]interface{}{"fullMetadata": map[string]interface{}{"dimension": "3"}}}, indexModeEnhanced},
		{map[string]interface{}{"metadata": map[string]interface{}{"fullMetadata": map[string]interface{}{"indexMode": "CLASSIC"}}}, indexModeClassic},
	} {
		if got := indexFromRecord(s, "b", "i", tc.record).IndexMode; got != tc.want {
			t.Fatalf("indexFromRecord(%v).IndexMode = %q, want %q", tc.record, got, tc.want)
		}
	}
}
