package iceberg

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/mux"
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
