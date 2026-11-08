package iceberg

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

// A table created without a location gets a directory named after it and its
// UUID, so it never lands on files another table left under its name.
func TestCreateTableWithoutLocationUsesItsOwnDirectory(t *testing.T) {
	const bucket = "warehouse"
	for _, body := range []string{`{"name":"orders"}`, `{"name":"orders","location":"s3://warehouse"}`} {
		fc := newMemFiler()
		seedNamespace(fc, bucket, "finance", "alice")
		s := NewServer(fc, nil)

		w := httptest.NewRecorder()
		s.handleCreateTable(w, newCreateTableRequest(t, bucket, "finance", body, "alice"))
		if w.Code != http.StatusOK {
			t.Fatalf("%s: status = %d (body: %s)", body, w.Code, w.Body.String())
		}
		var created struct {
			MetadataLocation string `json:"metadata-location"`
			Metadata         struct {
				Location  string `json:"location"`
				TableUUID string `json:"table-uuid"`
			} `json:"metadata"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &created); err != nil {
			t.Fatalf("decode: %v", err)
		}
		want := "s3://warehouse/finance/orders-" + created.Metadata.TableUUID
		if created.Metadata.TableUUID == "" || created.Metadata.Location != want {
			t.Errorf("%s: location = %q, want %q", body, created.Metadata.Location, want)
		}
		if !strings.HasPrefix(created.MetadataLocation, want+"/metadata/") {
			t.Errorf("%s: metadata-location = %q, want one under %s", body, created.MetadataLocation, want)
		}
	}
}

func TestTableLocation(t *testing.T) {
	for name, tc := range map[string]struct {
		resp s3tables.GetTableResponse
		want string
	}{
		"metadata":  {s3tables.GetTableResponse{MetadataLocation: "s3://b/ns/t-1/metadata/v2.metadata.json", WarehouseLocation: "s3://b/ns/t-0"}, "s3://b/ns/t-1"},
		"warehouse": {s3tables.GetTableResponse{WarehouseLocation: "s3://b/ns/t-0/"}, "s3://b/ns/t-0"},
		"legacy":    {s3tables.GetTableResponse{}, "s3://b/ns/t"},
	} {
		if got := tableLocation(tc.resp, "b", []string{"ns"}, "t"); got != tc.want {
			t.Errorf("%s: tableLocation = %q, want %q", name, got, tc.want)
		}
	}
}

// Two creates can both find a name free. The one the catalog registers second
// is a conflict and leaves nothing in the directory the server picked for it.
func TestCreateTableThatLosesTheRaceIsConflict(t *testing.T) {
	const bucket = "warehouse"
	fc := newMemFiler()
	seedNamespace(fc, bucket, "finance", "alice")
	s := NewServer(fc, nil)
	first := httptest.NewRecorder()
	raceAtSecondLookup(fc, s3tables.GetTablePath(bucket, "finance", "orders"), func() {
		s.handleCreateTable(first, newCreateTableRequest(t, bucket, "finance", `{"name":"orders"}`, "alice"))
	})
	second := httptest.NewRecorder()
	s.handleCreateTable(second, newCreateTableRequest(t, bucket, "finance", `{"name":"orders"}`, "alice"))

	won := decodeCreated(t, first)
	if second.Code != http.StatusConflict {
		t.Fatalf("losing create = %d (%s), want 409", second.Code, second.Body.String())
	}
	wonDir := path.Join(s3tables.TablesPath, strings.TrimPrefix(won.Metadata.Location, "s3://"))
	for p := range fc.entries {
		if strings.HasPrefix(p, s3tables.GetTablePath(bucket, "finance", "orders-")) && p != wonDir && !strings.HasPrefix(p, wonDir+"/") {
			t.Errorf("losing create left %s", p)
		}
	}
}

// Two commits of one staged table pick the same metadata file name. The one
// that finds the table registered, and moved on to newer metadata, fails
// without removing the file the registered table's history still names.
func TestStagedCommitThatLosesTheRaceFails(t *testing.T) {
	const bucket = "warehouse"
	fc := newMemFiler()
	seedNamespace(fc, bucket, "finance", "alice")
	s := NewServer(fc, nil)

	tableUUID := uuid.New()
	location := "s3://" + bucket + "/finance/orders-" + tableUUID.String()
	base, err := newTableMetadata(tableUUID, location, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	input := createOnCommitInput{
		bucketARN:    buildTableBucketARN(bucket),
		markerBucket: bucket,
		namespace:    []string{"finance"},
		tableName:    "orders",
		identityName: "alice",
		location:     location,
		tableUUID:    tableUUID,
		baseMetadata: base,
	}
	if _, reqErr := s.finalizeCreateOnCommit(context.Background(), input); reqErr != nil {
		t.Fatalf("first commit: %+v", reqErr)
	}
	ctx := context.Background()
	mgr := s3tables.NewManagerClient(fc)
	var registered s3tables.GetTableResponse
	if err := s.tablesManager.Execute(ctx, mgr, "GetTable", &s3tables.GetTableRequest{TableBucketARN: input.bucketARN, Namespace: input.namespace, Name: "orders"}, &registered, "alice"); err != nil {
		t.Fatal(err)
	}
	if err := s.tablesManager.Execute(ctx, mgr, "UpdateTable", &s3tables.UpdateTableRequest{
		TableBucketARN: input.bucketARN, Namespace: input.namespace, Name: "orders", VersionToken: registered.VersionToken,
		MetadataVersion: 2, MetadataLocation: location + "/metadata/v2.metadata.json",
	}, nil, "alice"); err != nil {
		t.Fatal(err)
	}

	_, reqErr := s.finalizeCreateOnCommit(ctx, input)
	if reqErr == nil || reqErr.status != http.StatusConflict {
		t.Fatalf("second commit error = %+v, want a conflict", reqErr)
	}
	published := path.Join(s3tables.TablesPath, strings.TrimPrefix(location, "s3://"), "metadata", "v1.metadata.json")
	if _, ok := fc.entries[published]; !ok {
		t.Errorf("losing commit removed %s", published)
	}
}

// Two clients can stage the same name, each at a location of its own. A commit
// lands at the location of its own stage, never at the location of another
// stage, even once its own marker is gone.
func TestStagedCommitKeepsItsOwnLocation(t *testing.T) {
	const bucket = "warehouse"
	for _, ownMarkerGone := range []bool{false, true} {
		fc := newMemFiler()
		seedNamespace(fc, bucket, "finance", "alice")
		s := NewServer(fc, nil)
		stage := func() createdResult {
			w := httptest.NewRecorder()
			s.handleCreateTable(w, newCreateTableRequest(t, bucket, "finance", `{"name":"orders","stage-create":true}`, "alice"))
			return decodeCreated(t, w)
		}
		first, second := stage(), stage()
		if ownMarkerGone {
			markerDir := stageCreateMarkerDir(bucket, []string{"finance"}, "orders")
			for p, entry := range fc.entries {
				if path.Dir(p) == markerDir && strings.Contains(string(entry.Content), first.Metadata.TableUUID) {
					delete(fc.entries, p)
				}
			}
		}

		body := `{"requirements":[{"type":"assert-create"}],"updates":[{"action":"assign-uuid","uuid":"` + first.Metadata.TableUUID + `"}]}`
		r := httptest.NewRequest(http.MethodPost, "/v1/"+bucket+"/namespaces/finance/tables/orders", strings.NewReader(body))
		r = mux.SetURLVars(r, map[string]string{"prefix": bucket, "namespace": "finance", "table": "orders"})
		r = r.WithContext(s3_constants.SetIdentityNameInContext(r.Context(), "alice"))
		w := httptest.NewRecorder()
		s.handleUpdateTable(w, r)

		committed := decodeCreated(t, w)
		if strings.HasPrefix(committed.MetadataLocation, second.Metadata.Location+"/") {
			t.Errorf("own marker gone %v: commit landed at the other stage's %s", ownMarkerGone, committed.MetadataLocation)
		}
		if !ownMarkerGone && (!strings.HasPrefix(committed.MetadataLocation, first.Metadata.Location+"/metadata/") || committed.Metadata.Location != first.Metadata.Location) {
			t.Errorf("commit landed at %s (location %s), want under %s", committed.MetadataLocation, committed.Metadata.Location, first.Metadata.Location)
		}
	}
}

type createdResult struct {
	MetadataLocation string `json:"metadata-location"`
	Metadata         struct {
		Location  string `json:"location"`
		TableUUID string `json:"table-uuid"`
		ViewUUID  string `json:"view-uuid"`
	} `json:"metadata"`
}

func decodeCreated(t *testing.T, w *httptest.ResponseRecorder) createdResult {
	t.Helper()
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d (body: %s)", w.Code, w.Body.String())
	}
	var created createdResult
	if err := json.Unmarshal(w.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return created
}

// raceAtSecondLookup runs create at the second lookup of record: after the
// handler found the name free, as the catalog looks at it again.
func raceAtSecondLookup(fc *memFiler, record string, create func()) {
	seen := 0
	fc.onLookup = func(p string) {
		if p != record {
			return
		}
		seen++
		if seen == 2 {
			fc.onLookup = nil
			create()
		}
	}
}
