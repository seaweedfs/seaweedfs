package iceberg

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/view"
	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

const ownerTestBucket = "warehouse"

var creatorActions = []string{"s3tables:CreateTable", "s3tables:GetTable", "s3tables:UpdateTable", "s3tables:CreateView", "s3tables:GetView", "s3tables:DeleteView"}

// seedOrders registers finance.orders with a data file, and returns the
// location of its metadata file.
func seedOrders(t *testing.T, fc *memFiler) (string, string) {
	t.Helper()
	seedNamespace(fc, ownerTestBucket, "finance", "alice")
	location, _ := seedRegistrableMetadata(t, fc, "orders")
	seedOwnedTable(fc, ownerTestBucket, "finance", "orders", "alice")
	ordersPath := s3tables.GetTablePath(ownerTestBucket, "finance", "orders")
	fc.seed(ordersPath+"/data/f.parquet", &filer_pb.Entry{Name: "f.parquet", Content: []byte("x")})
	return location, ordersPath
}

func createViewAt(t *testing.T, name, location string) *http.Request {
	t.Helper()
	schema := iceberg.NewSchemaWithIdentifiers(0, nil,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true})
	ver, err := view.NewVersionFromSQL(1, schema.ID, "SELECT 1", table.Identifier{"finance"})
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(CreateViewRequest{Name: name, Schema: schema, ViewVersion: ver, Location: location})
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodPost, "/v1/"+ownerTestBucket+"/namespaces/finance/views", bytes.NewReader(body))
	return mux.SetURLVars(r, map[string]string{"prefix": ownerTestBucket, "namespace": "finance"})
}

// A caller allowed to create tables in finance cannot point a new table or view
// at the files of finance.orders, and gets no credentials or metadata file
// written there.
func TestCreatePathsRefuseAnotherTablesLocation(t *testing.T) {
	t.Setenv("ICEBERG_ENABLE_STAGE_CREATE", "true")
	ordersLocation := "s3://" + ownerTestBucket + "/finance/orders"
	cases := map[string]func(s *Server, metadataLocation string) *httptest.ResponseRecorder{
		"register": func(s *Server, metadataLocation string) *httptest.ResponseRecorder {
			r := httptest.NewRequest(http.MethodPost, "/v1/"+ownerTestBucket+"/namespaces/finance/register",
				strings.NewReader(`{"name":"alias","metadata-location":"`+metadataLocation+`"}`))
			r = mux.SetURLVars(r, map[string]string{"prefix": ownerTestBucket, "namespace": "finance"})
			w := httptest.NewRecorder()
			s.handleRegisterTable(w, withActions(r, "mallory", creatorActions...))
			return w
		},
		"create": func(s *Server, _ string) *httptest.ResponseRecorder {
			r := newCreateTableRequest(t, ownerTestBucket, "finance", `{"name":"alias","location":"`+ordersLocation+`"}`, "mallory")
			w := httptest.NewRecorder()
			s.handleCreateTable(w, withActions(r, "mallory", creatorActions...))
			return w
		},
		"stage-create": func(s *Server, _ string) *httptest.ResponseRecorder {
			r := newCreateTableRequest(t, ownerTestBucket, "finance", `{"name":"alias","location":"`+ordersLocation+`/data","stage-create":true}`, "mallory")
			w := httptest.NewRecorder()
			s.handleCreateTable(w, withActions(r, "mallory", creatorActions...))
			return w
		},
		"create-view": func(s *Server, _ string) *httptest.ResponseRecorder {
			w := httptest.NewRecorder()
			s.handleCreateView(w, withActions(createViewAt(t, "alias", ordersLocation), "mallory", creatorActions...))
			return w
		},
	}
	for name, run := range cases {
		t.Run(name, func(t *testing.T) {
			fc := &writeLogFiler{memFiler: newMemFiler()}
			metadataLocation, _ := seedOrders(t, fc.memFiler)
			s := NewServer(fc, nil)

			w := run(s, metadataLocation)

			if w.Code != http.StatusConflict {
				t.Errorf("status = %d, want %d (body: %s)", w.Code, http.StatusConflict, w.Body.String())
			}
			if len(fc.created) != 0 {
				t.Errorf("refused create wrote %v", fc.created)
			}
			if _, ok := fc.entries[s3tables.GetTablePath(ownerTestBucket, "finance", "alias")]; ok {
				t.Errorf("refused create left a catalog record")
			}
		})
	}
}

// Registration is authorized before the metadata file is read, so a caller
// without CreateTable learns nothing about the files of the bucket.
func TestRegisterTableAuthorizedBeforeRead(t *testing.T) {
	fc := newMemFiler()
	metadataLocation, _ := seedOrders(t, fc)
	s := NewServer(fc, nil)

	for _, location := range []string{metadataLocation, "s3://" + ownerTestBucket + "/finance/orders/metadata/missing.metadata.json"} {
		r := httptest.NewRequest(http.MethodPost, "/v1/"+ownerTestBucket+"/namespaces/finance/register",
			strings.NewReader(`{"name":"alias","metadata-location":"`+location+`"}`))
		r = mux.SetURLVars(r, map[string]string{"prefix": ownerTestBucket, "namespace": "finance"})
		w := httptest.NewRecorder()
		s.handleRegisterTable(w, withActions(r, "reader", "s3tables:GetTable"))
		if w.Code != http.StatusForbidden {
			t.Errorf("%s: status = %d, want %d (body: %s)", location, w.Code, http.StatusForbidden, w.Body.String())
		}
	}
}
