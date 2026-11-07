package iceberg

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
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
