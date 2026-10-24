package iceberg

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

// seedPoisonedTable registers a table entry whose stored metadataLocation
// points outside its own bucket via a ".." segment, simulating a value
// persisted verbatim by the raw S3Tables UpdateTable API.
func seedPoisonedTable(t *testing.T, fc *memFiler, bucket, namespace, tableName, metadataLocation string) {
	t.Helper()
	meta := s3tables.TableMetadata{
		Iceberg: &s3tables.IcebergMetadata{TableUUID: "00000000-0000-0000-0000-000000000001"},
	}
	internal := map[string]any{
		"name":             tableName,
		"namespace":        namespace,
		"format":           "ICEBERG",
		"ownerAccountId":   s3_constants.AccountAdminId,
		"versionToken":     "v1",
		"metadataVersion":  1,
		"metadataLocation": metadataLocation,
		"metadata":         meta,
	}
	metaBytes, _ := json.Marshal(internal)
	fc.seed(s3tables.GetTablePath(bucket, namespace, tableName), &filer_pb.Entry{
		Name:        tableName,
		IsDirectory: true,
		Extended:    map[string][]byte{s3tables.ExtendedKeyMetadata: metaBytes},
	})
}

func TestCommitTableRejectsStoredTraversalLocation(t *testing.T) {
	const attacker = "attacker"
	const victim = "victim"
	fc := newMemFiler()
	seedNamespace(fc, attacker, "ns", s3_constants.AccountAdminId)
	seedPoisonedTable(t, fc, attacker, "ns", "t",
		"s3://attacker/../victim/planted/metadata/v1.metadata.json")

	s := NewServer(fc, nil)

	r := httptest.NewRequest(http.MethodPost, "/v1/"+attacker+"/namespaces/ns/tables/t",
		strings.NewReader(`{"requirements":[],"updates":[]}`))
	r = mux.SetURLVars(r, map[string]string{"prefix": attacker, "namespace": "ns", "table": "t"})
	r = r.WithContext(s3_constants.SetIdentityNameInContext(r.Context(), s3_constants.AccountAdminId))

	w := httptest.NewRecorder()
	s.handleUpdateTable(w, r)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d (body=%s)", w.Code, http.StatusBadRequest, w.Body.String())
	}
	for p := range fc.entries {
		if strings.Contains(p, "/"+victim+"/") && !strings.Contains(p, "/"+attacker+"/") {
			t.Fatalf("cross-tenant write escaped into victim tree at %s (status=%d body=%s)", p, w.Code, w.Body.String())
		}
	}
}

func TestCommitTransactionRejectsStoredTraversalLocation(t *testing.T) {
	const attacker = "attacker"
	const victim = "victim"
	fc := newMemFiler()
	seedNamespace(fc, attacker, "ns", s3_constants.AccountAdminId)
	seedPoisonedTable(t, fc, attacker, "ns", "t",
		"s3://attacker/../victim/planted/metadata/v1.metadata.json")

	s := NewServer(fc, nil)

	body := `{"table-changes":[{"identifier":{"namespace":["ns"],"name":"t"},"requirements":[],"updates":[]}]}`
	r := httptest.NewRequest(http.MethodPost, "/v1/"+attacker+"/transactions/commit",
		strings.NewReader(body))
	r = mux.SetURLVars(r, map[string]string{"prefix": attacker})
	r = r.WithContext(s3_constants.SetIdentityNameInContext(r.Context(), s3_constants.AccountAdminId))

	w := httptest.NewRecorder()
	s.handleCommitTransaction(w, r)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d (body=%s)", w.Code, http.StatusBadRequest, w.Body.String())
	}
	for p := range fc.entries {
		if strings.Contains(p, "/"+victim+"/") && !strings.Contains(p, "/"+attacker+"/") {
			t.Fatalf("cross-tenant transaction write escaped into victim tree at %s (status=%d body=%s)", p, w.Code, w.Body.String())
		}
	}
}
