package iceberg

import (
	"encoding/json"
	"slices"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
)

func TestNameMappingFromSchema(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "label", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 3, Name: "tags", Type: &iceberg.ListType{
			ElementID: 4, Element: iceberg.PrimitiveTypes.String,
		}},
		iceberg.NestedField{ID: 5, Name: "attrs", Type: &iceberg.MapType{
			KeyID: 6, KeyType: iceberg.PrimitiveTypes.String,
			ValueID: 7, ValueType: &iceberg.StructType{FieldList: []iceberg.NestedField{
				{ID: 8, Name: "inner", Type: iceberg.PrimitiveTypes.Int32},
			}},
		}},
	)

	mapping := nameMappingFromSchema(schema)
	got, err := json.Marshal(mapping)
	if err != nil {
		t.Fatalf("marshal mapping: %v", err)
	}

	want := `[{"names":["id"],"field-id":1},{"names":["label"],"field-id":2},` +
		`{"names":["tags"],"field-id":3,"fields":[{"names":["element"],"field-id":4}]},` +
		`{"names":["attrs"],"field-id":5,"fields":[{"names":["key"],"field-id":6},` +
		`{"names":["value"],"field-id":7,"fields":[{"names":["inner"],"field-id":8}]}]}]`
	if string(got) != want {
		t.Fatalf("mapping = %s\nwant %s", got, want)
	}
}

func TestEnsureDefaultNameMapping(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	props := ensureDefaultNameMapping(nil, schema)
	mappingJSON, ok := props[table.DefaultNameMappingKey]
	if !ok {
		t.Fatalf("property %s not set", table.DefaultNameMappingKey)
	}
	var mapping iceberg.NameMapping
	if err := json.Unmarshal([]byte(mappingJSON), &mapping); err != nil {
		t.Fatalf("stored mapping is not valid NameMapping JSON: %v", err)
	}
	if len(mapping) != 1 || mapping[0].ID() != 1 {
		t.Fatalf("unexpected mapping: %s", mappingJSON)
	}

	// An existing mapping is preserved.
	existing := iceberg.Properties{table.DefaultNameMappingKey: "custom"}
	if got := ensureDefaultNameMapping(existing, schema)[table.DefaultNameMappingKey]; got != "custom" {
		t.Fatalf("existing mapping overwritten: %s", got)
	}

	// An empty schema gets no mapping.
	if got := ensureDefaultNameMapping(nil, iceberg.NewSchema(0)); len(got) != 0 {
		t.Fatalf("empty schema produced properties: %v", got)
	}
}

// evolveMetadataSchema returns serialized metadata whose schema renamed
// "label" to "tag" and gained an "extra" column while the properties still
// carry the creation-time mapping, simulating a schema-evolution commit.
func evolveMetadataSchema(t *testing.T, base table.Metadata) []byte {
	t.Helper()

	raw, err := json.Marshal(base)
	if err != nil {
		t.Fatalf("marshal base metadata: %v", err)
	}
	evolved := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "tag", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 3, Name: "extra", Type: iceberg.PrimitiveTypes.String},
	)
	schemaJSON, err := json.Marshal(evolved)
	if err != nil {
		t.Fatalf("marshal evolved schema: %v", err)
	}
	var metadata map[string]json.RawMessage
	if err := json.Unmarshal(raw, &metadata); err != nil {
		t.Fatalf("unmarshal metadata: %v", err)
	}
	metadata["schemas"] = json.RawMessage("[" + string(schemaJSON) + "]")
	edited, err := json.Marshal(metadata)
	if err != nil {
		t.Fatalf("marshal edited metadata: %v", err)
	}
	return edited
}

func TestRefreshDefaultNameMappingOnSchemaEvolution(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "label", Type: iceberg.PrimitiveTypes.String},
	)
	base, err := newTableMetadata(uuid.New(), "s3://bucket/ns/tbl", schema, nil, nil, nil)
	if err != nil {
		t.Fatalf("newTableMetadata: %v", err)
	}

	edited := evolveMetadataSchema(t, base)
	updated, err := table.ParseMetadataBytes(edited)
	if err != nil {
		t.Fatalf("parse evolved metadata: %v", err)
	}

	refreshed := refreshDefaultNameMapping(edited, updated)
	final, err := table.ParseMetadataBytes(refreshed)
	if err != nil {
		t.Fatalf("parse refreshed metadata: %v", err)
	}
	var mapping iceberg.NameMapping
	if err := json.Unmarshal([]byte(final.Properties()[table.DefaultNameMappingKey]), &mapping); err != nil {
		t.Fatalf("parse refreshed mapping: %v", err)
	}
	if len(mapping) != 3 || mapping[2].ID() != 3 || mapping[2].Names[0] != "extra" {
		t.Fatalf("mapping was not refreshed for the evolved schema: %s", final.Properties()[table.DefaultNameMappingKey])
	}
	// The renamed column keeps its historical name so files written under the
	// old physical name stay resolvable.
	if mapping[1].ID() != 2 || !slices.Contains(mapping[1].Names, "tag") || !slices.Contains(mapping[1].Names, "label") {
		t.Fatalf("renamed column lost a name: %s", final.Properties()[table.DefaultNameMappingKey])
	}
}

func TestRefreshDefaultNameMappingKeepsUserNames(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "label", Type: iceberg.PrimitiveTypes.String},
	)
	base, err := newTableMetadata(uuid.New(), "s3://bucket/ns/tbl", schema, nil, nil,
		iceberg.Properties{table.DefaultNameMappingKey: `[{"names":["custom"],"field-id":1}]`})
	if err != nil {
		t.Fatalf("newTableMetadata: %v", err)
	}

	edited := evolveMetadataSchema(t, base)
	updated, err := table.ParseMetadataBytes(edited)
	if err != nil {
		t.Fatalf("parse evolved metadata: %v", err)
	}

	refreshed := refreshDefaultNameMapping(edited, updated)
	final, err := table.ParseMetadataBytes(refreshed)
	if err != nil {
		t.Fatalf("parse refreshed metadata: %v", err)
	}
	var mapping iceberg.NameMapping
	if err := json.Unmarshal([]byte(final.Properties()[table.DefaultNameMappingKey]), &mapping); err != nil {
		t.Fatalf("parse refreshed mapping: %v", err)
	}
	// The user's alias survives the merge alongside the schema name.
	if mapping[0].ID() != 1 || !slices.Contains(mapping[0].Names, "custom") || !slices.Contains(mapping[0].Names, "id") {
		t.Fatalf("user alias was dropped: %s", final.Properties()[table.DefaultNameMappingKey])
	}
}

func TestCreateTableSetsDefaultNameMapping(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "label", Type: iceberg.PrimitiveTypes.String},
	)
	metadata, err := newTableMetadata(uuid.New(), "s3://bucket/ns/tbl", schema, nil, nil, nil)
	if err != nil {
		t.Fatalf("newTableMetadata: %v", err)
	}
	if _, ok := metadata.Properties()[table.DefaultNameMappingKey]; !ok {
		t.Fatalf("newTableMetadata did not set %s: %v", table.DefaultNameMappingKey, metadata.Properties())
	}
}
