package s3vectors

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
