package iceberg

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/view"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

func TestIsViewNotFound(t *testing.T) {
	cases := []struct {
		err  error
		want bool
	}{
		{nil, false},
		{&s3tables.S3TablesError{Type: s3tables.ErrCodeNoSuchView, Message: "view x not found"}, true},
		{&s3tables.S3TablesError{Type: s3tables.ErrCodeNoSuchNamespace, Message: "namespace x not found"}, true},
		{errors.New("view x not found"), true},
		{&s3tables.S3TablesError{Type: s3tables.ErrCodeViewAlreadyExists, Message: "already exists"}, false},
		{errors.New("connection refused"), false},
	}
	for _, c := range cases {
		if got := isViewNotFound(c.err); got != c.want {
			t.Errorf("isViewNotFound(%v) = %v, want %v", c.err, got, c.want)
		}
	}
}

func TestIsViewAlreadyExists(t *testing.T) {
	cases := []struct {
		err  error
		want bool
	}{
		{nil, false},
		{&s3tables.S3TablesError{Type: s3tables.ErrCodeViewAlreadyExists, Message: "already exists"}, true},
		{errors.New("view already exists"), true},
		{&s3tables.S3TablesError{Type: s3tables.ErrCodeNoSuchView, Message: "not found"}, false},
		{errors.New("connection refused"), false},
	}
	for _, c := range cases {
		if got := isViewAlreadyExists(c.err); got != c.want {
			t.Errorf("isViewAlreadyExists(%v) = %v, want %v", c.err, got, c.want)
		}
	}
}

func TestViewPropertiesCopiesAndIsNonNil(t *testing.T) {
	if got := viewProperties(nil); got == nil {
		t.Fatal("viewProperties(nil) = nil, want non-nil")
	}
	src := iceberg.Properties{"k": "v"}
	out := viewProperties(src)
	out["k2"] = "v2"
	if _, ok := src["k2"]; ok {
		t.Fatal("viewProperties did not copy: mutation leaked into source")
	}
	if out["k"] != "v" {
		t.Fatalf("viewProperties dropped key: got %q", out["k"])
	}
}

// TestViewResponseRoundTrip verifies a built view metadata marshals into the
// ViewResponse wire shape and parses back via the iceberg-go view parser.
func TestViewResponseRoundTrip(t *testing.T) {
	schema := iceberg.NewSchemaWithIdentifiers(0, nil,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
	)
	ver, err := view.NewVersionFromSQL(1, schema.ID, "SELECT 1", table.Identifier{"ns"})
	if err != nil {
		t.Fatal(err)
	}
	md, err := view.NewMetadata(ver, schema, "s3://bucket/ns/v", iceberg.Properties{})
	if err != nil {
		t.Fatal(err)
	}

	resp := ViewResponse{
		MetadataLocation: "s3://bucket/ns/v/metadata/v1.metadata.json",
		Metadata:         md,
		Config:           iceberg.Properties{},
	}
	data, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	if _, ok := raw["metadata-location"]; !ok {
		t.Fatal("response missing metadata-location")
	}
	if _, ok := raw["metadata"]; !ok {
		t.Fatal("response missing metadata")
	}

	var back ViewResponse
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatalf("unmarshal back: %v", err)
	}
	if back.Metadata == nil {
		t.Fatal("metadata nil after round-trip")
	}
	if back.Metadata.CurrentVersion().VersionID != 1 {
		t.Fatalf("current version = %d, want 1", back.Metadata.CurrentVersion().VersionID)
	}
	if back.MetadataLocation != resp.MetadataLocation {
		t.Fatalf("metadata-location = %q, want %q", back.MetadataLocation, resp.MetadataLocation)
	}
}
