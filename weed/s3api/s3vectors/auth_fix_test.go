package s3vectors

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables/s3tablestest"
)

// postJSONAsIdentity runs the handler with the identity object in the context,
// the way Auth leaves it.
func postJSONAsIdentity(t *testing.T, id *s3api.Identity, handler http.HandlerFunc, payload interface{}) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	ctx := s3_constants.SetIdentityInContext(s3_constants.SetIdentityNameInContext(r.Context(), id.Name), id)
	handler(w, r.WithContext(ctx))
	return w
}

// CreateVectorBucket is authorized as an s3vectors action, and the creator
// still owns the bucket.
func TestCreateVectorBucketChecksVectorAction(t *testing.T) {
	fs := s3tablestest.Start(t)
	s := NewServer(s3tables.NewManagerClient(fs.Client), allowAllAuth{}, newFakeVectors(0))
	create := &CreateVectorBucketRequest{VectorBucketName: "vec-bucket"}

	tablesOnly := &s3api.Identity{Name: "bob", Actions: []s3api.Action{"s3tables:*"}}
	if w := postJSONAsIdentity(t, tablesOnly, s.handleCreateVectorBucket, create); w.Code != http.StatusForbidden {
		t.Fatalf("create with s3tables:* = %d %s, want 403", w.Code, w.Body.String())
	}
	vectors := &s3api.Identity{Name: "alice", Actions: []s3api.Action{"s3vectors:*"}}
	if w := postJSONAsIdentity(t, vectors, s.handleCreateVectorBucket, create); w.Code != http.StatusOK {
		t.Fatalf("create with s3vectors:* = %d %s, want 200", w.Code, w.Body.String())
	}

	record, err := s.vectorBucket(context.Background(), "vec-bucket")
	if err != nil {
		t.Fatalf("read bucket record: %v", err)
	}
	if record.OwnerAccountID != "alice" {
		t.Fatalf("owner = %q, want alice", record.OwnerAccountID)
	}
	get := &GetVectorBucketRequest{VectorBucketName: "vec-bucket"}
	if w := postJSONAsIdentity(t, &s3api.Identity{Name: "alice"}, s.handleGetVectorBucket, get); w.Code != http.StatusOK {
		t.Fatalf("owner get without actions = %d %s, want 200", w.Code, w.Body.String())
	}
	if w := postJSONAsIdentity(t, &s3api.Identity{Name: "carol"}, s.handleGetVectorBucket, get); w.Code != http.StatusForbidden {
		t.Fatalf("non-owner get without actions = %d %s, want 403", w.Code, w.Body.String())
	}
}

// DeleteIndex deletes only the record its format check read: a table that
// replaced the index in between survives, and the caller gets a conflict.
func TestDeleteIndexKeepsReplacedRecord(t *testing.T) {
	fs := s3tablestest.Start(t)
	s := NewServer(s3tables.NewManagerClient(fs.Client), allowAllAuth{}, newFakeVectors(0))
	if w := postJSON(t, s.handleCreateVectorBucket, &CreateVectorBucketRequest{VectorBucketName: "vec-bucket"}); w.Code != http.StatusOK {
		t.Fatalf("create bucket: %d %s", w.Code, w.Body.String())
	}
	if w := postJSON(t, s.handleCreateIndex, &CreateIndexRequest{
		VectorBucketName: "vec-bucket", IndexName: "idx", DataType: "float32", Dimension: 3, DistanceMetric: "euclidean",
	}); w.Code != http.StatusOK {
		t.Fatalf("create index: %d %s", w.Code, w.Body.String())
	}

	dir, name := splitEntryPath(s3tables.GetTablePath("vec-bucket", indexNamespace, "idx"))
	lookups := 0
	fs.OnLookup = func(d, n string) {
		if d != dir || n != name {
			return
		}
		// The first lookup is the handler's format check; swap the record
		// for an Iceberg table right after it.
		if lookups++; lookups != 2 {
			return
		}
		entry := fs.Get(dir, name)
		var meta map[string]interface{}
		if err := json.Unmarshal(entry.Extended[s3tables.ExtendedKeyMetadata], &meta); err != nil {
			t.Errorf("decode index metadata: %v", err)
			return
		}
		meta["format"] = s3tables.FormatIceberg
		meta["versionToken"] = "replaced"
		data, _ := json.Marshal(meta)
		ext := make(map[string][]byte, len(entry.Extended))
		for k, v := range entry.Extended {
			ext[k] = v
		}
		ext[s3tables.ExtendedKeyMetadata] = data
		fs.Put(dir, name, ext)
	}

	w := postJSON(t, s.handleDeleteIndex, &DeleteIndexRequest{VectorBucketName: "vec-bucket", IndexName: "idx"})
	fs.OnLookup = nil
	if w.Code != http.StatusConflict || !strings.Contains(w.Body.String(), "ConflictException") {
		t.Fatalf("delete over a replaced record = %d %s, want 409 ConflictException", w.Code, w.Body.String())
	}
	if fs.Get(dir, name) == nil {
		t.Fatal("DeleteIndex removed the table that replaced the index")
	}
}
