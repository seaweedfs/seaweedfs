package iceberg

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path"
	"strings"
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

// writeLogFiler records every file the handlers create, including files they
// delete again before answering, which the final entry map cannot show.
type writeLogFiler struct {
	*memFiler
	created []string
}

func (f *writeLogFiler) WithFilerClient(_ bool, fn func(client filer_pb.SeaweedFilerClient) error) error {
	return fn(f)
}

func (f *writeLogFiler) CreateEntry(ctx context.Context, in *filer_pb.CreateEntryRequest, opts ...grpc.CallOption) (*filer_pb.CreateEntryResponse, error) {
	if !in.Entry.IsDirectory {
		f.created = append(f.created, path.Join(in.Directory, in.Entry.Name))
	}
	return f.memFiler.CreateEntry(ctx, in, opts...)
}

// seedOwnedTable registers an existing table owned by owner.
func seedOwnedTable(fc *memFiler, bucket, namespace, tableName, owner string) {
	internal := map[string]any{
		"name":             tableName,
		"namespace":        namespace,
		"format":           "ICEBERG",
		"ownerAccountId":   owner,
		"versionToken":     "v1",
		"metadataVersion":  1,
		"metadataLocation": "s3://" + bucket + "/" + namespace + "/" + tableName + "/metadata/v1.metadata.json",
		"metadata": s3tables.TableMetadata{
			Iceberg: &s3tables.IcebergMetadata{TableUUID: "00000000-0000-0000-0000-000000000001"},
		},
	}
	metaBytes, _ := json.Marshal(internal)
	fc.seed(s3tables.GetTablePath(bucket, namespace, tableName), &filer_pb.Entry{
		Name:        tableName,
		IsDirectory: true,
		Extended:    map[string][]byte{s3tables.ExtendedKeyMetadata: metaBytes},
	})
}

// seedOwnedView registers an existing view owned by owner.
func seedOwnedView(fc *memFiler, bucket, namespace, viewName, owner string, fullMetadata []byte) {
	internal := map[string]any{
		"name":             viewName,
		"namespace":        namespace,
		"format":           "ICEBERG",
		"ownerAccountId":   owner,
		"versionToken":     "v1",
		"metadataVersion":  1,
		"metadataLocation": "s3://" + bucket + "/" + namespace + "/" + viewName + "/metadata/v1.metadata.json",
	}
	if len(fullMetadata) > 0 {
		internal["metadata"] = s3tables.TableMetadata{FullMetadata: fullMetadata}
	}
	metaBytes, _ := json.Marshal(internal)
	fc.seed(s3tables.GetTablePath(bucket, namespace, viewName), &filer_pb.Entry{
		Name:        viewName,
		IsDirectory: true,
		Extended: map[string][]byte{
			s3tables.ExtendedKeyMetadata:  metaBytes,
			s3tables.ExtendedKeyEntryType: []byte(s3tables.EntryTypeView),
		},
	})
}

// withActions gives the request an identity allowed only the listed actions.
func withActions(r *http.Request, name string, actions ...string) *http.Request {
	ctx := s3_constants.SetIdentityNameInContext(r.Context(), name)
	return r.WithContext(s3_constants.SetIdentityInContext(ctx, &struct {
		Name    string
		Actions []string
	}{Name: name, Actions: actions}))
}

func viewMetadataJSON(t *testing.T, location string) []byte {
	t.Helper()
	schema := iceberg.NewSchemaWithIdentifiers(0, nil,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true})
	ver, err := view.NewVersionFromSQL(1, schema.ID, "SELECT 1", table.Identifier{"finance"})
	if err != nil {
		t.Fatal(err)
	}
	md, err := view.NewMetadata(ver, schema, location, iceberg.Properties{})
	if err != nil {
		t.Fatal(err)
	}
	fullMetadata, err := json.Marshal(md)
	if err != nil {
		t.Fatal(err)
	}
	return fullMetadata
}

// A caller that may load a table but not commit to it must be refused before
// the commit stages its metadata file in the table's directory. The commit
// paths used to write that file, reach the UpdateTable authorization, and
// delete the file again on the way out.
func TestCommitDeniedBeforeStagingMetadata(t *testing.T) {
	const bucket = "warehouse"
	cases := map[string]func(s *Server) *httptest.ResponseRecorder{
		"commit-table": func(s *Server) *httptest.ResponseRecorder {
			r := httptest.NewRequest(http.MethodPost, "/v1/"+bucket+"/namespaces/finance/tables/quarterly_reports",
				strings.NewReader(`{"requirements":[],"updates":[{"action":"set-properties","updates":{"k":"v"}}]}`))
			r = mux.SetURLVars(r, map[string]string{"prefix": bucket, "namespace": "finance", "table": "quarterly_reports"})
			w := httptest.NewRecorder()
			s.handleUpdateTable(w, withActions(r, "reader", "s3tables:GetTable"))
			return w
		},
		"commit-transaction": func(s *Server) *httptest.ResponseRecorder {
			body := `{"table-changes":[{"identifier":{"namespace":["finance"],"name":"quarterly_reports"},"requirements":[],"updates":[{"action":"set-properties","updates":{"k":"v"}}]}]}`
			r := httptest.NewRequest(http.MethodPost, "/v1/"+bucket+"/transactions/commit", strings.NewReader(body))
			r = mux.SetURLVars(r, map[string]string{"prefix": bucket})
			w := httptest.NewRecorder()
			s.handleCommitTransaction(w, withActions(r, "reader", "s3tables:GetTable"))
			return w
		},
		"commit-view": func(s *Server) *httptest.ResponseRecorder {
			r := httptest.NewRequest(http.MethodPost, "/v1/"+bucket+"/namespaces/finance/views/quarterly_view",
				strings.NewReader(`{"requirements":[],"updates":[{"action":"set-properties","updates":{"k":"v"}}]}`))
			r = mux.SetURLVars(r, map[string]string{"prefix": bucket, "namespace": "finance", "view": "quarterly_view"})
			w := httptest.NewRecorder()
			s.handleUpdateView(w, withActions(r, "reader", "s3tables:GetView"))
			return w
		},
	}
	for name, run := range cases {
		t.Run(name, func(t *testing.T) {
			fc := &writeLogFiler{memFiler: newMemFiler()}
			seedNamespace(fc.memFiler, bucket, "finance", "alice")
			seedOwnedTable(fc.memFiler, bucket, "finance", "quarterly_reports", "alice")
			seedOwnedView(fc.memFiler, bucket, "finance", "quarterly_view", "alice", viewMetadataJSON(t, "s3://"+bucket+"/finance/quarterly_view"))
			s := NewServer(fc, nil)

			w := run(s)

			if w.Code != http.StatusForbidden {
				t.Errorf("status = %d, want %d (body: %s)", w.Code, http.StatusForbidden, w.Body.String())
			}
			if len(fc.created) != 0 {
				t.Errorf("refused commit wrote %v", fc.created)
			}
		})
	}
}

// The owner, and a caller whose own identity carries UpdateTable, still
// stage their metadata file and land the commit.
func TestCommitAllowedStagesMetadata(t *testing.T) {
	const bucket = "warehouse"
	callers := map[string]func(r *http.Request) *http.Request{
		"owner": func(r *http.Request) *http.Request {
			return r.WithContext(s3_constants.SetIdentityNameInContext(r.Context(), "alice"))
		},
		"identity-actions": func(r *http.Request) *http.Request {
			return withActions(r, "writer", "s3tables:GetTable", "s3tables:UpdateTable")
		},
	}
	for name, as := range callers {
		t.Run(name, func(t *testing.T) {
			fc := &writeLogFiler{memFiler: newMemFiler()}
			seedNamespace(fc.memFiler, bucket, "finance", "alice")
			seedOwnedTable(fc.memFiler, bucket, "finance", "quarterly_reports", "alice")
			s := NewServer(fc, nil)

			r := httptest.NewRequest(http.MethodPost, "/v1/"+bucket+"/namespaces/finance/tables/quarterly_reports",
				strings.NewReader(`{"requirements":[],"updates":[{"action":"set-properties","updates":{"k":"v"}}]}`))
			r = mux.SetURLVars(r, map[string]string{"prefix": bucket, "namespace": "finance", "table": "quarterly_reports"})
			w := httptest.NewRecorder()
			s.handleUpdateTable(w, as(r))

			if w.Code != http.StatusOK {
				t.Fatalf("status = %d, want %d (body: %s)", w.Code, http.StatusOK, w.Body.String())
			}
			if len(fc.created) != 1 || !strings.HasSuffix(fc.created[0], ".metadata.json") {
				t.Fatalf("created = %v, want one metadata file", fc.created)
			}
		})
	}
}

// Same staging gate on the view commit path: the owner, and a caller whose own
// identity carries UpdateView, stage a metadata file and land the commit.
func TestViewCommitAllowedStagesMetadata(t *testing.T) {
	const bucket = "warehouse"
	fullMetadata := viewMetadataJSON(t, "s3://"+bucket+"/finance/quarterly_view")

	callers := map[string]func(r *http.Request) *http.Request{
		"owner": func(r *http.Request) *http.Request {
			return r.WithContext(s3_constants.SetIdentityNameInContext(r.Context(), "alice"))
		},
		"identity-actions": func(r *http.Request) *http.Request {
			return withActions(r, "writer", "s3tables:GetView", "s3tables:UpdateView")
		},
	}
	for name, as := range callers {
		t.Run(name, func(t *testing.T) {
			fc := &writeLogFiler{memFiler: newMemFiler()}
			seedNamespace(fc.memFiler, bucket, "finance", "alice")
			seedOwnedView(fc.memFiler, bucket, "finance", "quarterly_view", "alice", fullMetadata)
			s := NewServer(fc, nil)

			r := httptest.NewRequest(http.MethodPost, "/v1/"+bucket+"/namespaces/finance/views/quarterly_view",
				strings.NewReader(`{"requirements":[],"updates":[{"action":"set-properties","updates":{"k":"v"}}]}`))
			r = mux.SetURLVars(r, map[string]string{"prefix": bucket, "namespace": "finance", "view": "quarterly_view"})
			w := httptest.NewRecorder()
			s.handleUpdateView(w, as(r))

			if w.Code != http.StatusOK {
				t.Fatalf("status = %d, want %d (body: %s)", w.Code, http.StatusOK, w.Body.String())
			}
			if len(fc.created) != 1 || !strings.HasSuffix(fc.created[0], ".metadata.json") {
				t.Fatalf("created = %v, want one metadata file", fc.created)
			}
		})
	}
}
