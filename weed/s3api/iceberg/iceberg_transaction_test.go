package iceberg

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path"
	"strings"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
	"google.golang.org/grpc"
)

func TestDecodeCommitTransactionRequest(t *testing.T) {
	body := `{"table-changes":[
		{"identifier":{"namespace":["ns"],"name":"t1"},"requirements":[{"type":"assert-table-uuid","uuid":"00000000-0000-0000-0000-000000000001"}],"updates":[{"action":"set-properties","updates":{"k":"v"}}]},
		{"identifier":{"namespace":["ns","sub"],"name":"t2"},"requirements":[],"updates":[]}
	]}`

	var req CommitTransactionRequest
	if err := json.Unmarshal([]byte(body), &req); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if len(req.TableChanges) != 2 {
		t.Fatalf("table-changes = %d, want 2", len(req.TableChanges))
	}
	if req.TableChanges[0].Identifier.Name != "t1" {
		t.Fatalf("change[0] name = %q, want t1", req.TableChanges[0].Identifier.Name)
	}
	if got := []string(req.TableChanges[1].Identifier.Namespace); len(got) != 2 || got[1] != "sub" {
		t.Fatalf("change[1] namespace = %v, want [ns sub]", got)
	}
}

func TestParseTableChangeSeparatesStatistics(t *testing.T) {
	change := tableChangeRequest{
		Requirements: json.RawMessage(`[{"type":"assert-table-uuid","uuid":"00000000-0000-0000-0000-000000000001"}]`),
		Updates: []json.RawMessage{
			json.RawMessage(`{"action":"set-properties","updates":{"k":"v"}}`),
			json.RawMessage(`{"action":"set-statistics","snapshot-id":10,"statistics-path":"s3://bucket/table/metadata/stats.puffin","file-size-in-bytes":100,"file-footer-size-in-bytes":20,"blob-metadata":[]}`),
		},
	}

	requirements, updates, statistics, reqErr := parseTableChange(change)
	if reqErr != nil {
		t.Fatalf("parseTableChange() error = %+v", reqErr)
	}
	if len(requirements) != 1 {
		t.Fatalf("requirements = %d, want 1", len(requirements))
	}
	if len(updates) != 1 {
		t.Fatalf("updates = %d, want 1", len(updates))
	}
	if len(statistics) != 1 || statistics[0].set == nil || statistics[0].set.SnapshotID != 10 {
		t.Fatalf("unexpected statistics updates: %#v", statistics)
	}
}

func TestParseTableChangeRejectsInvalidRequirements(t *testing.T) {
	change := tableChangeRequest{Requirements: json.RawMessage(`{not json}`)}
	_, _, _, reqErr := parseTableChange(change)
	if reqErr == nil {
		t.Fatalf("parseTableChange() expected error for invalid requirements")
	}
	if reqErr.status != 400 {
		t.Fatalf("status = %d, want 400", reqErr.status)
	}
}

func TestCloneTableMetadataIsIndependent(t *testing.T) {
	orig := &s3tables.TableMetadata{
		Iceberg:      &s3tables.IcebergMetadata{TableUUID: "uuid-1"},
		FullMetadata: json.RawMessage(`{"v":1}`),
	}
	clone := cloneTableMetadata(orig)
	if clone == orig || clone.Iceberg == orig.Iceberg {
		t.Fatalf("cloneTableMetadata returned aliased pointers")
	}

	orig.Iceberg.TableUUID = "mutated"
	orig.FullMetadata[0] = 'X'
	if clone.Iceberg.TableUUID != "uuid-1" {
		t.Fatalf("clone Iceberg.TableUUID = %q, want uuid-1", clone.Iceberg.TableUUID)
	}
	if string(clone.FullMetadata) != `{"v":1}` {
		t.Fatalf("clone FullMetadata = %q, mutated with source", clone.FullMetadata)
	}

	if cloneTableMetadata(nil) != nil {
		t.Fatalf("cloneTableMetadata(nil) = non-nil")
	}
}

func TestBuildTableRestoreRequestCarriesPriorState(t *testing.T) {
	pc := &preparedTableCommit{
		namespace:           []string{"ns", "sub"},
		tableName:           "t1",
		prevMetadataLoc:     "s3://bucket/ns/t1/metadata/v3.metadata.json",
		prevMetadataVersion: 3,
		prevMetadata: &s3tables.TableMetadata{
			Iceberg:      &s3tables.IcebergMetadata{TableUUID: uuid.NewString()},
			FullMetadata: json.RawMessage(`{"format-version":2,"last-updated-ms":3}`),
		},
	}

	req := buildTableRestoreRequest("arn:bucket", pc)

	if req.TableBucketARN != "arn:bucket" || req.Name != "t1" {
		t.Fatalf("restore target = %s/%s, want arn:bucket/t1", req.TableBucketARN, req.Name)
	}
	if req.MetadataLocation != pc.prevMetadataLoc {
		t.Fatalf("restore MetadataLocation = %q, want %q", req.MetadataLocation, pc.prevMetadataLoc)
	}
	if req.MetadataVersion != pc.prevMetadataVersion {
		t.Fatalf("restore MetadataVersion = %d, want %d", req.MetadataVersion, pc.prevMetadataVersion)
	}
	if req.Metadata == nil || !bytes.Equal(req.Metadata.FullMetadata, pc.prevMetadata.FullMetadata) {
		t.Fatalf("restore FullMetadata = %q, want %q", req.Metadata.FullMetadata, pc.prevMetadata.FullMetadata)
	}
	if req.Metadata.Iceberg == nil || req.Metadata.Iceberg.TableUUID != pc.prevMetadata.Iceberg.TableUUID {
		t.Fatalf("restore TableUUID mismatch")
	}
}

// TestRollbackRevertsEveryFlippedField mirrors handleUpdateTable's partial-field
// update semantics to prove that after a flip + rollback the table is back to its
// pre-transaction state. With the old location-only rollback the table kept the
// new full metadata and version; the restore request must revert all of them.
func TestRollbackRevertsEveryFlippedField(t *testing.T) {
	// prior on-disk state of an already-committed table.
	prior := tableEntry{
		metadataLocation: "s3://bucket/ns/t1/metadata/v3.metadata.json",
		metadataVersion:  3,
		metadata: &s3tables.TableMetadata{
			Iceberg:      &s3tables.IcebergMetadata{TableUUID: "uuid-1"},
			FullMetadata: json.RawMessage(`{"format-version":2,"v":3}`),
		},
	}

	pc := &preparedTableCommit{
		namespace:           []string{"ns"},
		tableName:           "t1",
		tableUUID:           uuid.MustParse("00000000-0000-0000-0000-000000000001"),
		metadataBytes:       json.RawMessage(`{"format-version":2,"v":4}`),
		metadataVersion:     4,
		newMetadataLoc:      "s3://bucket/ns/t1/metadata/v4.metadata.json",
		prevMetadataLoc:     prior.metadataLocation,
		prevMetadataVersion: prior.metadataVersion,
		prevMetadata:        cloneTableMetadata(prior.metadata),
	}

	// flip: apply the new commit, then roll it back via the restore request.
	state := prior.clone()
	state.applyUpdate(&s3tables.UpdateTableRequest{
		Metadata:         &s3tables.TableMetadata{Iceberg: &s3tables.IcebergMetadata{TableUUID: pc.tableUUID.String()}, FullMetadata: pc.metadataBytes},
		MetadataVersion:  pc.metadataVersion,
		MetadataLocation: pc.newMetadataLoc,
	})
	state.applyUpdate(buildTableRestoreRequest("arn:bucket", pc))

	if state.metadataLocation != prior.metadataLocation {
		t.Fatalf("after rollback location = %q, want %q", state.metadataLocation, prior.metadataLocation)
	}
	if state.metadataVersion != prior.metadataVersion {
		t.Fatalf("after rollback version = %d, want %d", state.metadataVersion, prior.metadataVersion)
	}
	if !bytes.Equal(state.metadata.FullMetadata, prior.metadata.FullMetadata) {
		t.Fatalf("after rollback FullMetadata = %q, want %q", state.metadata.FullMetadata, prior.metadata.FullMetadata)
	}
	if state.metadata.Iceberg.TableUUID != prior.metadata.Iceberg.TableUUID {
		t.Fatalf("after rollback TableUUID = %q, want %q", state.metadata.Iceberg.TableUUID, prior.metadata.Iceberg.TableUUID)
	}
}

// TestCleanupSafeMetadataKeepsFailedRollback proves a flip failure at index 2
// where table 0's rollback also fails leaves table 0's new metadata in place
// (its pointer still references it), while the rolled-back table 1 and the
// never-flipped tables 2 and 3 are safe to delete.
func TestCleanupSafeMetadataKeepsFailedRollback(t *testing.T) {
	prepared := make([]preparedTableCommit, 4)
	rolledBack := []bool{false, true} // tables 0 and 1 were flipped; 0's rollback failed

	safe := cleanupSafeMetadata(prepared, 2, rolledBack)

	want := []bool{false, true, true, true}
	for i := range want {
		if safe[i] != want[i] {
			t.Fatalf("safe[%d] = %v, want %v (full %v)", i, safe[i], want[i], safe)
		}
	}
}

// TestCleanupSafeMetadataFirstFlipFails covers a flip failure at the first table:
// nothing was flipped, so every written file is safe to delete.
func TestCleanupSafeMetadataFirstFlipFails(t *testing.T) {
	prepared := make([]preparedTableCommit, 3)
	safe := cleanupSafeMetadata(prepared, 0, nil)
	for i, ok := range safe {
		if !ok {
			t.Fatalf("safe[%d] = false, want true (full %v)", i, safe)
		}
	}
}

// TestCleanupSafeMetadataAllRolledBack covers the common case where every prior
// flip rolls back cleanly, so all metadata files are safe to delete.
func TestCleanupSafeMetadataAllRolledBack(t *testing.T) {
	prepared := make([]preparedTableCommit, 3)
	safe := cleanupSafeMetadata(prepared, 2, []bool{true, true})
	for i, ok := range safe {
		if !ok {
			t.Fatalf("safe[%d] = false, want true (full %v)", i, safe)
		}
	}
}

// tableEntry models the persisted table fields and the partial-update rules
// handleUpdateTable applies, so rollback can be exercised without a live filer.
type tableEntry struct {
	metadataLocation string
	metadataVersion  int
	metadata         *s3tables.TableMetadata
}

func (e tableEntry) clone() *tableEntry {
	return &tableEntry{
		metadataLocation: e.metadataLocation,
		metadataVersion:  e.metadataVersion,
		metadata:         cloneTableMetadata(e.metadata),
	}
}

func (e *tableEntry) applyUpdate(req *s3tables.UpdateTableRequest) {
	if req.Metadata != nil {
		if e.metadata == nil {
			e.metadata = &s3tables.TableMetadata{}
		}
		if req.Metadata.Iceberg != nil {
			if e.metadata.Iceberg == nil {
				e.metadata.Iceberg = &s3tables.IcebergMetadata{}
			}
			if req.Metadata.Iceberg.TableUUID != "" {
				e.metadata.Iceberg.TableUUID = req.Metadata.Iceberg.TableUUID
			}
		}
		if len(req.Metadata.FullMetadata) > 0 {
			e.metadata.FullMetadata = req.Metadata.FullMetadata
		}
	}
	if req.MetadataLocation != "" {
		e.metadataLocation = req.MetadataLocation
	}
	if req.MetadataVersion > 0 {
		e.metadataVersion = req.MetadataVersion
	}
}

// A transaction against a registration that kept only its metadata location
// reads that file: the UUID assertion the client took from its load passes,
// and the commit lands on the registered snapshots instead of a placeholder.
func TestCommitTransactionReadsMetadataFileOfOlderRegistration(t *testing.T) {
	fc := newMemFiler()
	seedNamespace(fc, registerTestBucket, "finance", "alice")
	s := NewServer(fc, nil)
	location, registered := seedRegistrableMetadata(t, fc, "events")

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

	commitTransaction := func(requirements, updates string) *httptest.ResponseRecorder {
		body := `{"table-changes":[{"identifier":{"namespace":["finance"],"name":"events"},` +
			`"requirements":[` + requirements + `],"updates":[` + updates + `]}]}`
		r := httptest.NewRequest(http.MethodPost, "/v1/"+registerTestBucket+"/transactions/commit", strings.NewReader(body))
		r = mux.SetURLVars(r, map[string]string{"prefix": registerTestBucket})
		r = r.WithContext(s3_constants.SetIdentityNameInContext(r.Context(), "alice"))
		w := httptest.NewRecorder()
		s.handleCommitTransaction(w, r)
		return w
	}

	w := commitTransaction(
		`{"type":"assert-table-uuid","uuid":"`+registered.TableUUID().String()+`"}`,
		`{"action":"set-properties","updates":{"committed":"true"}}`)
	if w.Code != http.StatusNoContent {
		t.Fatalf("uuid-asserting transaction: status = %d, want %d (body: %s)", w.Code, http.StatusNoContent, w.Body.String())
	}

	w = commitTransaction("", `{"action":"set-properties","updates":{"again":"true"}}`)
	if w.Code != http.StatusNoContent {
		t.Fatalf("requirement-free transaction: status = %d, want %d (body: %s)", w.Code, http.StatusNoContent, w.Body.String())
	}

	w = httptest.NewRecorder()
	s.handleLoadTable(w, registeredTableRequest(http.MethodGet, "events", ""))
	loaded := servedMetadata(t, "load after transactions", w)
	checkServesRegistered(t, "load after transactions", loaded, registered)
	if got := loaded.Properties()["committed"]; got != "true" {
		t.Errorf("property committed = %q, want true", got)
	}
	if got := loaded.Properties()["again"]; got != "true" {
		t.Errorf("property again = %q, want true", got)
	}
}

// A transaction re-stamps the default name mapping the way a single-table
// commit does, so readers of field-id-less files keep resolving columns even
// when the table's record started without one.
func TestCommitTransactionRefreshesNameMapping(t *testing.T) {
	fc := newMemFiler()
	seedNamespace(fc, registerTestBucket, "finance", "alice")
	s := NewServer(fc, nil)

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "label", Type: iceberg.PrimitiveTypes.String},
	)
	metadata, err := newTableMetadata(uuid.New(), "s3://"+registerTestBucket+"/finance/events", schema, nil, nil, nil)
	if err != nil {
		t.Fatalf("newTableMetadata: %v", err)
	}
	raw, err := json.Marshal(metadata)
	if err != nil {
		t.Fatalf("marshal metadata: %v", err)
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("decode metadata: %v", err)
	}
	if props, ok := doc["properties"].(map[string]any); ok {
		delete(props, table.DefaultNameMappingKey)
	}
	if raw, err = json.Marshal(doc); err != nil {
		t.Fatalf("re-marshal metadata: %v", err)
	}
	location := seedMetadataFile(fc, "events", "00001-7c1e2f4a.metadata.json", raw)

	err = fc.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
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

	body := `{"table-changes":[{"identifier":{"namespace":["finance"],"name":"events"},` +
		`"updates":[{"action":"set-properties","updates":{"committed":"true"}}]}]}`
	r := httptest.NewRequest(http.MethodPost, "/v1/"+registerTestBucket+"/transactions/commit", strings.NewReader(body))
	r = mux.SetURLVars(r, map[string]string{"prefix": registerTestBucket})
	r = r.WithContext(s3_constants.SetIdentityNameInContext(r.Context(), "alice"))
	w := httptest.NewRecorder()
	s.handleCommitTransaction(w, r)
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d (body: %s)", w.Code, http.StatusNoContent, w.Body.String())
	}

	w = httptest.NewRecorder()
	s.handleLoadTable(w, registeredTableRequest(http.MethodGet, "events", ""))
	loaded := servedMetadata(t, "load after transaction", w)
	mappingJSON, ok := loaded.Properties()[table.DefaultNameMappingKey]
	if !ok || mappingJSON == "" {
		t.Fatalf("committed metadata lost %s: %v", table.DefaultNameMappingKey, loaded.Properties())
	}
}

// failUpdateFiler fails entry updates on one path, so a transaction's flip of
// that table fails after the tables before it in the request have flipped.
type failUpdateFiler struct {
	*memFiler
	failPath string
}

func (f *failUpdateFiler) WithFilerClient(_ bool, fn func(client filer_pb.SeaweedFilerClient) error) error {
	return fn(f)
}

func (f *failUpdateFiler) UpdateEntry(ctx context.Context, in *filer_pb.UpdateEntryRequest, opts ...grpc.CallOption) (*filer_pb.UpdateEntryResponse, error) {
	if path.Join(in.Directory, in.Entry.Name) == f.failPath {
		return nil, errors.New("injected update failure")
	}
	return f.memFiler.UpdateEntry(ctx, in, opts...)
}

// A transaction that fails after flipping a table rolls it back. UpdateTable
// cannot clear the FullMetadata a flip stored, so for a registration that kept
// only its metadata location the restore has to write back the file's
// metadata: otherwise loads go on serving the change the client was told
// failed, and a client retrying the transaction applies it twice.
func TestCommitTransactionRollbackRestoresOlderRegistration(t *testing.T) {
	fc := &failUpdateFiler{memFiler: newMemFiler()}
	seedNamespace(fc.memFiler, registerTestBucket, "finance", "alice")
	s := NewServer(fc, nil)
	location, registered := seedRegistrableMetadata(t, fc.memFiler, "events")

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
	otherLocation, _ := seedRegistrableMetadata(t, fc.memFiler, "other")
	w := httptest.NewRecorder()
	s.handleRegisterTable(w, registerTableRequest("other", otherLocation))
	if w.Code != http.StatusOK {
		t.Fatalf("register other: status = %d (body: %s)", w.Code, w.Body.String())
	}

	// events flips first; the flip of other then fails and rolls events back.
	fc.failPath = s3tables.GetTablePath(registerTestBucket, "finance", "other")
	change := func(name string) string {
		return `{"identifier":{"namespace":["finance"],"name":"` + name + `"},` +
			`"updates":[{"action":"set-properties","updates":{"committed":"true"}}]}`
	}
	body := `{"table-changes":[` + change("events") + `,` + change("other") + `]}`
	r := httptest.NewRequest(http.MethodPost, "/v1/"+registerTestBucket+"/transactions/commit", strings.NewReader(body))
	r = mux.SetURLVars(r, map[string]string{"prefix": registerTestBucket})
	r = r.WithContext(s3_constants.SetIdentityNameInContext(r.Context(), "alice"))
	w = httptest.NewRecorder()
	s.handleCommitTransaction(w, r)
	if w.Code == http.StatusNoContent {
		t.Fatal("transaction succeeded although the flip of other failed")
	}

	w = httptest.NewRecorder()
	s.handleLoadTable(w, registeredTableRequest(http.MethodGet, "events", ""))
	loaded := servedMetadata(t, "load after rolled-back transaction", w)
	checkServesRegistered(t, "load after rolled-back transaction", loaded, registered)
	if got, ok := loaded.Properties()["committed"]; ok {
		t.Errorf("load after rolled-back transaction serves its property committed = %q", got)
	}
}

// A transaction on a registration kept by location alone — one whose metadata
// is too large to ship — flips the pointer by location too, and the record
// holds no stored copy that would shadow the staged file.
func TestCommitTransactionOnOversizedRegistration(t *testing.T) {
	fc := newMemFiler()
	seedNamespace(fc, registerTestBucket, "finance", "alice")
	s := NewServer(fc, nil)
	location, registered := seedOversizedMetadata(t, fc, "events")

	err := fc.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return s.tablesManager.Execute(context.Background(), s3tables.NewManagerClient(client), "RegisterTable", &s3tables.RegisterTableRequest{
			TableBucketARN:   buildTableBucketARN(registerTestBucket),
			Namespace:        []string{"finance"},
			Name:             "events",
			MetadataLocation: location,
		}, nil, "alice")
	})
	if err != nil {
		t.Fatalf("seed location-only registration: %v", err)
	}

	body := `{"table-changes":[{"identifier":{"namespace":["finance"],"name":"events"},` +
		`"requirements":[{"type":"assert-table-uuid","uuid":"` + registered.TableUUID().String() + `"}],` +
		`"updates":[{"action":"set-properties","updates":{"committed":"true"}}]}]}`
	r := httptest.NewRequest(http.MethodPost, "/v1/"+registerTestBucket+"/transactions/commit", strings.NewReader(body))
	r = mux.SetURLVars(r, map[string]string{"prefix": registerTestBucket})
	r = r.WithContext(s3_constants.SetIdentityNameInContext(r.Context(), "alice"))
	w := httptest.NewRecorder()
	s.handleCommitTransaction(w, r)
	if w.Code != http.StatusNoContent {
		t.Fatalf("oversized transaction: status = %d, want %d (body: %s)", w.Code, http.StatusNoContent, w.Body.String())
	}

	if stored := storedMetadata(t, s, fc, "events"); stored != nil && len(stored.FullMetadata) != 0 {
		t.Errorf("catalog record holds %d bytes of metadata, over the manager's request limit", len(stored.FullMetadata))
	}

	w = httptest.NewRecorder()
	s.handleLoadTable(w, registeredTableRequest(http.MethodGet, "events", ""))
	loaded := servedMetadata(t, "load after transaction", w)
	checkServesRegistered(t, "load after transaction", loaded, registered)
	if got := loaded.Properties()["committed"]; got != "true" {
		t.Errorf("load after transaction: property committed = %q, want true", got)
	}
}
