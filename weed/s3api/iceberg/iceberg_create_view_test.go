package iceberg

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/view"
	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
	"google.golang.org/grpc"
)

// memFiler is a minimal in-memory SeaweedFilerClient backing the view handlers
// end-to-end through the s3tables manager. Only the entry operations the create
// path touches are implemented; anything else panics so a missing dependency is
// loud rather than silently passing.
type memFiler struct {
	filer_pb.SeaweedFilerClient
	entries map[string]*filer_pb.Entry
}

func newMemFiler() *memFiler {
	return &memFiler{entries: map[string]*filer_pb.Entry{}}
}

func (m *memFiler) WithFilerClient(_ bool, fn func(client filer_pb.SeaweedFilerClient) error) error {
	return fn(m)
}

func (m *memFiler) seed(p string, entry *filer_pb.Entry) {
	m.entries[p] = entry
}

func (m *memFiler) LookupDirectoryEntry(_ context.Context, in *filer_pb.LookupDirectoryEntryRequest, _ ...grpc.CallOption) (*filer_pb.LookupDirectoryEntryResponse, error) {
	entry, ok := m.entries[path.Join(in.Directory, in.Name)]
	if !ok {
		return nil, filer_pb.ErrNotFound
	}
	return &filer_pb.LookupDirectoryEntryResponse{Entry: entry}, nil
}

func (m *memFiler) CreateEntry(_ context.Context, in *filer_pb.CreateEntryRequest, _ ...grpc.CallOption) (*filer_pb.CreateEntryResponse, error) {
	m.entries[path.Join(in.Directory, in.Entry.Name)] = in.Entry
	return &filer_pb.CreateEntryResponse{}, nil
}

func (m *memFiler) UpdateEntry(_ context.Context, in *filer_pb.UpdateEntryRequest, _ ...grpc.CallOption) (*filer_pb.UpdateEntryResponse, error) {
	m.entries[path.Join(in.Directory, in.Entry.Name)] = in.Entry
	return &filer_pb.UpdateEntryResponse{}, nil
}

func newCreateViewRequest(t *testing.T, namespace, name, sql string) *http.Request {
	t.Helper()
	schema := iceberg.NewSchemaWithIdentifiers(0, nil,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
	)
	ver, err := view.NewVersionFromSQL(1, schema.ID, sql, table.Identifier{namespace})
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(CreateViewRequest{Name: name, Schema: schema, ViewVersion: ver})
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodPost, "/v1/namespaces/"+namespace+"/views", bytes.NewReader(body))
	r = mux.SetURLVars(r, map[string]string{"namespace": namespace})
	r = r.WithContext(s3_constants.SetIdentityNameInContext(r.Context(), s3_constants.AccountAdminId))
	return r
}

// seedNamespace registers a bucket and namespace so the s3tables existence and
// auth-context lookups pass; ownership is irrelevant since the admin principal
// is always allowed.
func seedNamespace(fc *memFiler, bucket, namespace string) {
	fc.seed(s3tables.GetTableBucketPath(bucket), &filer_pb.Entry{Name: bucket, IsDirectory: true})
	meta, _ := json.Marshal(map[string]any{"namespace": []string{namespace}, "ownerAccountId": s3_constants.AccountAdminId})
	fc.seed(s3tables.GetNamespacePath(bucket, namespace), &filer_pb.Entry{
		Name:        namespace,
		IsDirectory: true,
		Extended:    map[string][]byte{s3tables.ExtendedKeyMetadata: meta},
	})
}

func TestCreateViewMissingNamespaceReturns404(t *testing.T) {
	fc := newMemFiler()
	s := NewServer(fc, nil)

	w := httptest.NewRecorder()
	s.handleCreateView(w, newCreateViewRequest(t, "ns", "v", "SELECT 1"))

	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d (body: %s)", w.Code, http.StatusNotFound, w.Body.String())
	}
	var errResp ErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &errResp); err != nil {
		t.Fatalf("unmarshal error body: %v", err)
	}
	if errResp.Error.Type != "NoSuchNamespaceException" {
		t.Fatalf("error type = %q, want NoSuchNamespaceException", errResp.Error.Type)
	}
}

func TestCreateViewTagsEntryAsView(t *testing.T) {
	const bucket = "warehouse"
	fc := newMemFiler()
	seedNamespace(fc, bucket, "ns")
	s := NewServer(fc, nil)

	w := httptest.NewRecorder()
	s.handleCreateView(w, newCreateViewRequest(t, "ns", "v", "SELECT 1"))
	if w.Code != http.StatusOK {
		t.Fatalf("create status = %d, want 200 (body: %s)", w.Code, w.Body.String())
	}

	entry, ok := fc.entries[s3tables.GetTablePath(bucket, "ns", "v")]
	if !ok {
		t.Fatalf("view entry not created")
	}
	if got := string(entry.Extended[s3tables.ExtendedKeyEntryType]); got != s3tables.EntryTypeView {
		t.Fatalf("entryType = %q, want %q", got, s3tables.EntryTypeView)
	}
	if _, ok := entry.Extended[s3tables.ExtendedKeyMetadata]; !ok {
		t.Fatalf("view entry missing metadata attribute")
	}
}

func TestCreateViewDuplicateDoesNotClobberMetadata(t *testing.T) {
	const bucket = "warehouse"
	fc := newMemFiler()
	seedNamespace(fc, bucket, "ns")
	s := NewServer(fc, nil)

	w := httptest.NewRecorder()
	s.handleCreateView(w, newCreateViewRequest(t, "ns", "v", "SELECT 1"))
	if w.Code != http.StatusOK {
		t.Fatalf("first create status = %d, want 200 (body: %s)", w.Code, w.Body.String())
	}

	metadataKey := path.Join(s3tables.GetTablePath(bucket, "ns", "v"), "metadata", "v1.metadata.json")
	first, ok := fc.entries[metadataKey]
	if !ok {
		t.Fatalf("metadata file not written at %s", metadataKey)
	}
	original := append([]byte(nil), first.Content...)

	// Re-create the same view with different SQL: the existence pre-check must
	// short-circuit so the persisted v1.metadata.json stays byte-for-byte intact.
	w = httptest.NewRecorder()
	s.handleCreateView(w, newCreateViewRequest(t, "ns", "v", "SELECT 2"))
	if w.Code != http.StatusOK {
		t.Fatalf("duplicate create status = %d, want 200 (body: %s)", w.Code, w.Body.String())
	}
	if got := fc.entries[metadataKey].Content; !bytes.Equal(got, original) {
		t.Fatalf("duplicate create clobbered stored metadata:\n got %s\nwant %s", got, original)
	}
}
