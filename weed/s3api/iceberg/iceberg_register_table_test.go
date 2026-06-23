package iceberg

import (
	"encoding/json"
	"testing"
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
