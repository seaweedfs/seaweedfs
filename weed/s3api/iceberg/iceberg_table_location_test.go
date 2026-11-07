package iceberg

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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
// is answered with the registered table, not with metadata in a directory the
// catalog never recorded.
func TestCreateTableThatLosesTheRaceReturnsTheRegisteredTable(t *testing.T) {
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

	won, lost := decodeCreated(t, first), decodeCreated(t, second)
	if lost != won {
		t.Errorf("losing create answered %+v, want the registered table %+v", lost, won)
	}
}

type createdResult struct {
	MetadataLocation string `json:"metadata-location"`
	Metadata         struct {
		Location  string `json:"location"`
		TableUUID string `json:"table-uuid"`
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
