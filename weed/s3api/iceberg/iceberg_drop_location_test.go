package iceberg

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

// A view recorded at another table's location, as one created before the
// location rule could be, is dropped without purging that table's files.
func TestDropViewKeepsAnotherRecordsFiles(t *testing.T) {
	fc := newMemFiler()
	_, ordersPath := seedOrders(t, fc)
	viewMeta, err := json.Marshal(map[string]any{
		"name":             "alias",
		"namespace":        "finance",
		"ownerAccountId":   "mallory",
		"versionToken":     "v1",
		"metadataVersion":  1,
		"metadataLocation": "s3://" + ownerTestBucket + "/finance/orders/metadata/v1.metadata.json",
	})
	if err != nil {
		t.Fatal(err)
	}
	fc.seed(s3tables.GetTablePath(ownerTestBucket, "finance", "alias"), &filer_pb.Entry{
		Name:        "alias",
		IsDirectory: true,
		Extended: map[string][]byte{
			s3tables.ExtendedKeyMetadata:  viewMeta,
			s3tables.ExtendedKeyEntryType: []byte(s3tables.EntryTypeView),
		},
	})
	s := NewServer(fc, nil)

	r := httptest.NewRequest(http.MethodDelete, "/v1/"+ownerTestBucket+"/namespaces/finance/views/alias", nil)
	r = mux.SetURLVars(r, map[string]string{"prefix": ownerTestBucket, "namespace": "finance", "view": "alias"})
	w := httptest.NewRecorder()
	s.handleDropView(w, withActions(r, "mallory", creatorActions...))

	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d (body: %s)", w.Code, http.StatusNoContent, w.Body.String())
	}
	if _, ok := fc.entries[s3tables.GetTablePath(ownerTestBucket, "finance", "alias")]; ok {
		t.Errorf("dropped view still recorded")
	}
	if _, ok := fc.entries[ordersPath]; !ok {
		t.Errorf("dropping the view deleted finance.orders")
	}
	if _, ok := fc.entries[ordersPath+"/data/f.parquet"]; !ok {
		t.Errorf("dropping the view deleted the files of finance.orders")
	}
}

// A table recorded over files that were already there drops without purging
// them: they were not written for it.
func TestDropTableKeepsOlderFilesAtItsLocation(t *testing.T) {
	fc := newMemFiler()
	seedNamespace(fc, ownerTestBucket, "finance", "alice")
	otherPath := s3tables.GetTablePath(ownerTestBucket, "finance", "other")
	fc.seed(otherPath, &filer_pb.Entry{Name: "other", IsDirectory: true})
	fc.seed(otherPath+"/data", &filer_pb.Entry{Name: "data", IsDirectory: true})
	fc.seed(otherPath+"/data/f.parquet", &filer_pb.Entry{Name: "f.parquet", Content: []byte("x"),
		Attributes: &filer_pb.FuseAttributes{Crtime: time.Now().Add(-time.Hour).Unix()}})
	tableMeta, err := json.Marshal(map[string]any{
		"name":             "alias",
		"namespace":        "finance",
		"format":           "ICEBERG",
		"createdAt":        time.Now(),
		"ownerAccountId":   "mallory",
		"versionToken":     "v1",
		"metadataVersion":  1,
		"metadataLocation": "s3://" + ownerTestBucket + "/finance/other/metadata/v1.metadata.json",
	})
	if err != nil {
		t.Fatal(err)
	}
	fc.seed(s3tables.GetTablePath(ownerTestBucket, "finance", "alias"), &filer_pb.Entry{
		Name:        "alias",
		IsDirectory: true,
		Extended:    map[string][]byte{s3tables.ExtendedKeyMetadata: tableMeta},
	})
	s := NewServer(fc, nil)

	r := httptest.NewRequest(http.MethodDelete, "/v1/"+ownerTestBucket+"/namespaces/finance/tables/alias", nil)
	r = mux.SetURLVars(r, map[string]string{"prefix": ownerTestBucket, "namespace": "finance", "table": "alias"})
	w := httptest.NewRecorder()
	s.handleDropTable(w, withActions(r, "mallory", creatorActions...))

	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d (body: %s)", w.Code, http.StatusNoContent, w.Body.String())
	}
	if _, ok := fc.entries[otherPath+"/data/f.parquet"]; !ok {
		t.Errorf("dropping the table deleted files that predate it")
	}
}

// The engine of a staged create writes its first files before the commit
// registers the table. They are the table's, so a drop purges them.
func TestDropStagedTablePurgesFilesWrittenBeforeItsCommit(t *testing.T) {
	const bucket = "warehouse"
	fc := newMemFiler()
	seedNamespace(fc, bucket, "finance", "alice")
	s := NewServer(fc, nil)

	w := httptest.NewRecorder()
	s.handleCreateTable(w, newCreateTableRequest(t, bucket, "finance", `{"name":"orders","stage-create":true}`, "alice"))
	staged := decodeCreated(t, w)
	stagedAt := time.Now().Add(-time.Minute)
	for p, entry := range fc.entries {
		if path.Dir(p) != stageCreateMarkerDir(bucket, []string{"finance"}, "orders") || entry.IsDirectory {
			continue
		}
		var marker stageCreateMarker
		if err := json.Unmarshal(entry.Content, &marker); err != nil {
			t.Fatal(err)
		}
		marker.CreatedAt = stagedAt.UTC().Format(time.RFC3339Nano)
		entry.Content, _ = json.Marshal(marker)
	}
	locationPath := path.Join(s3tables.TablesPath, strings.TrimPrefix(staged.Metadata.Location, "s3://"))
	fc.seed(locationPath, &filer_pb.Entry{Name: path.Base(locationPath), IsDirectory: true})
	fc.seed(locationPath+"/data", &filer_pb.Entry{Name: "data", IsDirectory: true})
	fc.seed(locationPath+"/data/f.parquet", &filer_pb.Entry{Name: "f.parquet", Content: []byte("x"),
		Attributes: &filer_pb.FuseAttributes{Crtime: stagedAt.Add(30 * time.Second).Unix()}})

	body := `{"requirements":[{"type":"assert-create"}],"updates":[{"action":"assign-uuid","uuid":"` + staged.Metadata.TableUUID + `"}]}`
	r := httptest.NewRequest(http.MethodPost, "/v1/"+bucket+"/namespaces/finance/tables/orders", strings.NewReader(body))
	r = mux.SetURLVars(r, map[string]string{"prefix": bucket, "namespace": "finance", "table": "orders"})
	w = httptest.NewRecorder()
	s.handleUpdateTable(w, r.WithContext(s3_constants.SetIdentityNameInContext(r.Context(), "alice")))
	decodeCreated(t, w)

	r = httptest.NewRequest(http.MethodDelete, "/v1/"+bucket+"/namespaces/finance/tables/orders", nil)
	r = mux.SetURLVars(r, map[string]string{"prefix": bucket, "namespace": "finance", "table": "orders"})
	w = httptest.NewRecorder()
	s.handleDropTable(w, r.WithContext(s3_constants.SetIdentityNameInContext(r.Context(), "alice")))

	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d (body: %s)", w.Code, http.StatusNoContent, w.Body.String())
	}
	if _, ok := fc.entries[locationPath+"/data/f.parquet"]; ok {
		t.Errorf("drop kept a file the staged create wrote before its commit")
	}
}
