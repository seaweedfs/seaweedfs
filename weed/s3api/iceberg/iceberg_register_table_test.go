package iceberg

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path"
	"strings"
	"testing"

	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

// TestRegisterTableRequestWireShape pins the field names to the iceberg-go REST
// client payload: {"name", "metadata-location"}.
func TestRegisterTableRequestWireShape(t *testing.T) {
	body := `{"name":"orders","metadata-location":"s3://bucket/ns/orders/metadata/v1.metadata.json"}`
	var req RegisterTableRequest
	if err := json.Unmarshal([]byte(body), &req); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if req.Name != "orders" {
		t.Errorf("Name = %q, want orders", req.Name)
	}
	if req.MetadataLocation != "s3://bucket/ns/orders/metadata/v1.metadata.json" {
		t.Errorf("MetadataLocation = %q", req.MetadataLocation)
	}
}

// TestRegisterTableRejectsOutOfBoundsLocation confirms a metadata-location that
// points outside the request's table bucket, or contains a traversal segment, is
// rejected with 400 before any metadata read.
func TestRegisterTableRejectsOutOfBoundsLocation(t *testing.T) {
	cases := []struct {
		name             string
		metadataLocation string
	}{
		{"cross bucket", "s3://other/ns/orders/metadata/v1.metadata.json"},
		{"traversal", "s3://bucket/ns/../../etc/metadata/v1.metadata.json"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			body := `{"name":"orders","metadata-location":"` + c.metadataLocation + `"}`
			r := httptest.NewRequest(http.MethodPost, "/v1/bucket/namespaces/ns/register", strings.NewReader(body))
			r = mux.SetURLVars(r, map[string]string{"prefix": "bucket", "namespace": "ns"})
			rec := httptest.NewRecorder()

			(&Server{}).handleRegisterTable(rec, r)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
			}
			var resp ErrorResponse
			if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
				t.Fatalf("decode response: %v", err)
			}
			if resp.Error.Type != "BadRequestException" {
				t.Fatalf("error type = %q, want BadRequestException", resp.Error.Type)
			}
		})
	}
}

const registerTestBucket = "warehouse"

// seedMetadataFile writes content as the file name in the metadata directory
// of finance/<table> and returns its location.
func seedMetadataFile(fc *memFiler, tableName, name string, content []byte) string {
	tablePath := path.Join("finance", tableName)
	fc.seed(path.Join(metadataDirPath(registerTestBucket, tablePath), name), &filer_pb.Entry{Name: name, Content: content})
	return "s3://" + registerTestBucket + "/" + tablePath + "/metadata/" + name
}

// seedRegistrableMetadata writes the metadata of a table with two snapshots,
// main on the second, the way the file a table is registered from holds them.
func seedRegistrableMetadata(t *testing.T, fc *memFiler, tableName string) (string, table.Metadata) {
	t.Helper()
	metadata := metadataWithSnapshots(t, []int64{1, 2}, 0)
	content, err := json.Marshal(metadata)
	if err != nil {
		t.Fatalf("marshal metadata: %v", err)
	}
	return seedMetadataFile(fc, tableName, "00001-7c1e2f4a.metadata.json", content), metadata
}

func registerTableRequest(name, metadataLocation string) *http.Request {
	body := `{"name":"` + name + `","metadata-location":"` + metadataLocation + `"}`
	r := httptest.NewRequest(http.MethodPost, "/v1/"+registerTestBucket+"/namespaces/finance/register", strings.NewReader(body))
	r = mux.SetURLVars(r, map[string]string{"prefix": registerTestBucket, "namespace": "finance"})
	return r.WithContext(s3_constants.SetIdentityNameInContext(r.Context(), "alice"))
}

func registeredTableRequest(method, name, body string) *http.Request {
	r := httptest.NewRequest(method, "/v1/"+registerTestBucket+"/namespaces/finance/tables/"+name, strings.NewReader(body))
	r = mux.SetURLVars(r, map[string]string{"prefix": registerTestBucket, "namespace": "finance", "table": name})
	return r.WithContext(s3_constants.SetIdentityNameInContext(r.Context(), "alice"))
}

// commitWithUUIDAssertion is the commit Spark sends for an INSERT: it asserts
// the table UUID it loaded before changing anything.
func commitWithUUIDAssertion(tableUUID uuid.UUID) string {
	return `{"requirements":[{"type":"assert-table-uuid","uuid":"` + tableUUID.String() + `"}],` +
		`"updates":[{"action":"set-properties","updates":{"committed":"true"}}]}`
}

// servedMetadata decodes the table metadata from a load, register, or commit
// response.
func servedMetadata(t *testing.T, what string, w *httptest.ResponseRecorder) table.Metadata {
	t.Helper()
	if w.Code != http.StatusOK {
		t.Fatalf("%s: status = %d, want %d (body: %s)", what, w.Code, http.StatusOK, w.Body.String())
	}
	var result struct {
		Metadata json.RawMessage `json:"metadata"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatalf("%s: decode response: %v", what, err)
	}
	metadata, err := table.ParseMetadataBytes(result.Metadata)
	if err != nil {
		t.Fatalf("%s: parse response metadata: %v", what, err)
	}
	return metadata
}

// checkServesRegistered fails unless got is the registered table, not a
// placeholder: same UUID, same snapshots, same current snapshot.
func checkServesRegistered(t *testing.T, what string, got, want table.Metadata) {
	t.Helper()
	if got.TableUUID() != want.TableUUID() {
		t.Errorf("%s: table UUID = %s, want %s", what, got.TableUUID(), want.TableUUID())
	}
	if len(got.Snapshots()) != len(want.Snapshots()) {
		t.Errorf("%s: %d snapshots, want %d", what, len(got.Snapshots()), len(want.Snapshots()))
	}
	if current := got.CurrentSnapshot(); current == nil || current.SnapshotID != want.CurrentSnapshot().SnapshotID {
		t.Errorf("%s: current snapshot = %+v, want %d", what, current, want.CurrentSnapshot().SnapshotID)
	}
}

// storedMetadata returns the metadata the catalog record of finance.<name>
// holds.
func storedMetadata(t *testing.T, s *Server, fc *memFiler, name string) *s3tables.TableMetadata {
	t.Helper()
	var getResp s3tables.GetTableResponse
	err := fc.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return s.tablesManager.Execute(context.Background(), s3tables.NewManagerClient(client), "GetTable", &s3tables.GetTableRequest{
			TableBucketARN: buildTableBucketARN(registerTestBucket),
			Namespace:      []string{"finance"},
			Name:           name,
		}, &getResp, "alice")
	})
	if err != nil {
		t.Fatalf("GetTable %s: %v", name, err)
	}
	return getResp.Metadata
}

// loadAndCommit loads finance.<name> twice, then commits to it with the UUID
// assertion Spark sends, checking each response serves the registered table.
func loadAndCommit(t *testing.T, s *Server, name string, registered table.Metadata) {
	t.Helper()
	for _, what := range []string{"first load", "second load"} {
		w := httptest.NewRecorder()
		s.handleLoadTable(w, registeredTableRequest(http.MethodGet, name, ""))
		checkServesRegistered(t, what, servedMetadata(t, what, w), registered)
	}

	w := httptest.NewRecorder()
	s.handleUpdateTable(w, registeredTableRequest(http.MethodPost, name, commitWithUUIDAssertion(registered.TableUUID())))
	committed := servedMetadata(t, "commit", w)
	checkServesRegistered(t, "commit", committed, registered)
	if got := committed.Properties()["committed"]; got != "true" {
		t.Errorf("commit: property committed = %q, want true", got)
	}

	w = httptest.NewRecorder()
	s.handleLoadTable(w, registeredTableRequest(http.MethodGet, name, ""))
	loaded := servedMetadata(t, "load after commit", w)
	checkServesRegistered(t, "load after commit", loaded, registered)
	if got := loaded.Properties()["committed"]; got != "true" {
		t.Errorf("load after commit: property committed = %q, want true", got)
	}
}

// A registered table serves the metadata it was registered from on every
// load, and a commit that asserts the table UUID lands on it. Loads used to
// serve an empty placeholder with a nil UUID, which hid the table's data and
// failed every commit on the UUID assertion.
func TestRegisterTableServesRegisteredMetadata(t *testing.T) {
	fc := newMemFiler()
	seedNamespace(fc, registerTestBucket, "finance", "alice")
	s := NewServer(fc, nil)
	location, registered := seedRegistrableMetadata(t, fc, "events")

	w := httptest.NewRecorder()
	s.handleRegisterTable(w, registerTableRequest("events", location))
	checkServesRegistered(t, "register", servedMetadata(t, "register", w), registered)

	// The record keeps what was registered, as a created table's does, rather
	// than only pointing at the file.
	stored := storedMetadata(t, s, fc, "events")
	if stored == nil || len(stored.FullMetadata) == 0 {
		t.Fatal("the catalog record does not keep the registered metadata")
	}
	if stored.Iceberg == nil || stored.Iceberg.TableUUID != registered.TableUUID().String() {
		t.Errorf("catalog record UUID = %+v, want %s", stored.Iceberg, registered.TableUUID())
	}

	loadAndCommit(t, s, "events", registered)
}

// A table registered before the record kept its metadata has only the
// metadata location in its record. Loading it reads the file there, and the
// first commit starts from that file rather than from a placeholder.
func TestLoadTableReadsMetadataFileOfOlderRegistration(t *testing.T) {
	fc := newMemFiler()
	seedNamespace(fc, registerTestBucket, "finance", "alice")
	s := NewServer(fc, nil)
	location, registered := seedRegistrableMetadata(t, fc, "events")

	// The record an older RegisterTable left: the location and nothing else.
	err := fc.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return s.tablesManager.Execute(context.Background(), s3tables.NewManagerClient(client), "RegisterTable", &s3tables.RegisterTableRequest{
			TableBucketARN:   buildTableBucketARN(registerTestBucket),
			Namespace:        []string{"finance"},
			Name:             "events",
			MetadataLocation: location,
		}, nil, "alice")
	})
	if err != nil {
		t.Fatalf("seed older registration: %v", err)
	}

	loadAndCommit(t, s, "events", registered)

	// The first commit stores the metadata in the record, under the UUID of the
	// file it read rather than a fresh one.
	stored := storedMetadata(t, s, fc, "events")
	if stored == nil || stored.Iceberg == nil {
		t.Fatal("the first commit stored no metadata in the record")
	}
	if stored.Iceberg.TableUUID != registered.TableUUID().String() {
		t.Errorf("record table UUID after first commit = %s, want the file's %s", stored.Iceberg.TableUUID, registered.TableUUID())
	}
}

// seedOversizedMetadata writes a metadata file larger than the manager's
// request limit, so it can only be registered by location and UUID.
func seedOversizedMetadata(t *testing.T, fc *memFiler, tableName string) (string, table.Metadata) {
	t.Helper()
	builder, err := table.MetadataBuilderFromBase(metadataWithSnapshots(t, []int64{1, 2}, 0), "")
	if err != nil {
		t.Fatalf("MetadataBuilderFromBase() error = %v", err)
	}
	if err := builder.SetProperties(map[string]string{"padding": strings.Repeat("x", s3tables.MaxRequestBodySize)}); err != nil {
		t.Fatalf("SetProperties() error = %v", err)
	}
	registered, err := builder.Build()
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	content, err := json.Marshal(registered)
	if err != nil {
		t.Fatalf("marshal metadata: %v", err)
	}
	return seedMetadataFile(fc, tableName, "00001-7c1e2f4a.metadata.json", content), registered
}

// A metadata file too large to travel in the manager request is registered by
// its location and UUID alone, and loads read it from there. Sending it whole
// failed every such registration on the manager's request size limit.
func TestRegisterTableKeepsOversizedMetadataByLocation(t *testing.T) {
	fc := newMemFiler()
	seedNamespace(fc, registerTestBucket, "finance", "alice")
	s := NewServer(fc, nil)

	location, registered := seedOversizedMetadata(t, fc, "events")

	w := httptest.NewRecorder()
	s.handleRegisterTable(w, registerTableRequest("events", location))
	checkServesRegistered(t, "register", servedMetadata(t, "register", w), registered)

	stored := storedMetadata(t, s, fc, "events")
	if stored == nil || stored.Iceberg == nil || stored.Iceberg.TableUUID != registered.TableUUID().String() {
		t.Errorf("catalog record metadata = %+v, want the file's UUID %s", stored, registered.TableUUID())
	}
	if stored != nil && len(stored.FullMetadata) != 0 {
		t.Errorf("catalog record holds %d bytes of metadata, over the manager's request limit", len(stored.FullMetadata))
	}

	w = httptest.NewRecorder()
	s.handleLoadTable(w, registeredTableRequest(http.MethodGet, "events", ""))
	checkServesRegistered(t, "load", servedMetadata(t, "load", w), registered)
}

// A commit to a location-only record cannot ship the new metadata back either:
// the pointer flips by location alone, and the record must not keep a stored
// copy that would shadow the freshly staged file.
func TestCommitTableOnOversizedRegistration(t *testing.T) {
	fc := newMemFiler()
	seedNamespace(fc, registerTestBucket, "finance", "alice")
	s := NewServer(fc, nil)

	location, registered := seedOversizedMetadata(t, fc, "events")
	w := httptest.NewRecorder()
	s.handleRegisterTable(w, registerTableRequest("events", location))
	checkServesRegistered(t, "register", servedMetadata(t, "register", w), registered)

	w = httptest.NewRecorder()
	s.handleUpdateTable(w, registeredTableRequest(http.MethodPost, "events", commitWithUUIDAssertion(registered.TableUUID())))
	committed := servedMetadata(t, "commit", w)
	checkServesRegistered(t, "commit", committed, registered)
	if got := committed.Properties()["committed"]; got != "true" {
		t.Errorf("commit: property committed = %q, want true", got)
	}

	if stored := storedMetadata(t, s, fc, "events"); stored != nil && len(stored.FullMetadata) != 0 {
		t.Errorf("catalog record holds %d bytes of metadata, over the manager's request limit", len(stored.FullMetadata))
	}

	w = httptest.NewRecorder()
	s.handleLoadTable(w, registeredTableRequest(http.MethodGet, "events", ""))
	if got := servedMetadata(t, "load", w).Properties()["committed"]; got != "true" {
		t.Errorf("load: property committed = %q, want true", got)
	}
}

// A file that is not Iceberg metadata is the client's error, and no catalog
// record is left behind for it.
func TestRegisterTableRejectsInvalidMetadata(t *testing.T) {
	for name, content := range map[string][]byte{
		"truncated":  []byte(`{"format-version":2,"table-uuid":`),
		"empty file": nil,
	} {
		t.Run(name, func(t *testing.T) {
			fc := newMemFiler()
			seedNamespace(fc, registerTestBucket, "finance", "alice")
			s := NewServer(fc, nil)
			location := seedMetadataFile(fc, "events", "00001-7c1e2f4a.metadata.json", content)

			w := httptest.NewRecorder()
			s.handleRegisterTable(w, registerTableRequest("events", location))
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d (body: %s)", w.Code, http.StatusBadRequest, w.Body.String())
			}
			if !strings.Contains(w.Body.String(), "BadRequestException") {
				t.Errorf("body = %s, want a BadRequestException", w.Body.String())
			}

			w = httptest.NewRecorder()
			s.handleLoadTable(w, registeredTableRequest(http.MethodGet, "events", ""))
			if w.Code != http.StatusNotFound {
				t.Errorf("load after rejected register = %d, want %d", w.Code, http.StatusNotFound)
			}
		})
	}
}
