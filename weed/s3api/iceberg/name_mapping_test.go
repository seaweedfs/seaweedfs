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

func TestRefreshDefaultNameMappingDropsReassignedNames(t *testing.T) {
	// The stored mapping was derived from a schema whose ids were later
	// reassigned (id<->name swapped). The merged mapping must follow the
	// current schema exactly: duplicating names across ids makes Java readers
	// reject the whole mapping.
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String},
	)
	metadata, err := newTableMetadata(uuid.New(), "s3://bucket/ns/tbl", schema, nil, nil,
		iceberg.Properties{table.DefaultNameMappingKey: `[{"names":["name"],"field-id":1},{"names":["id"],"field-id":2}]`})
	if err != nil {
		t.Fatalf("newTableMetadata: %v", err)
	}
	raw, err := json.Marshal(metadata)
	if err != nil {
		t.Fatalf("marshal metadata: %v", err)
	}

	refreshed := refreshDefaultNameMapping(raw, metadata)
	final, err := table.ParseMetadataBytes(refreshed)
	if err != nil {
		t.Fatalf("parse refreshed metadata: %v", err)
	}
	var mapping iceberg.NameMapping
	if err := json.Unmarshal([]byte(final.Properties()[table.DefaultNameMappingKey]), &mapping); err != nil {
		t.Fatalf("parse refreshed mapping: %v", err)
	}
	seen := map[string]int{}
	for _, field := range mapping {
		for _, name := range field.Names {
			if prior, dup := seen[name]; dup {
				t.Fatalf("name %q mapped to both field %d and field %d: %s",
					name, prior, field.ID(), final.Properties()[table.DefaultNameMappingKey])
			}
			seen[name] = field.ID()
		}
	}
	if seen["id"] != 1 || seen["name"] != 2 {
		t.Fatalf("current schema names must win: %s", final.Properties()[table.DefaultNameMappingKey])
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
	// Request ids are deliberately non-sequential: the metadata constructor
	// reassigns fresh ids, and the stamped mapping must follow the final
	// schema, not the request.
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 7, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 3, Name: "label", Type: iceberg.PrimitiveTypes.String},
	)
	metadata, err := newTableMetadata(uuid.New(), "s3://bucket/ns/tbl", schema, nil, nil, nil)
	if err != nil {
		t.Fatalf("newTableMetadata: %v", err)
	}
	mappingJSON, ok := metadata.Properties()[table.DefaultNameMappingKey]
	if !ok {
		t.Fatalf("newTableMetadata did not set %s: %v", table.DefaultNameMappingKey, metadata.Properties())
	}
	var mapping iceberg.NameMapping
	if err := json.Unmarshal([]byte(mappingJSON), &mapping); err != nil {
		t.Fatalf("parse stamped mapping: %v", err)
	}
	finalSchema := metadata.CurrentSchema()
	if len(mapping) != len(finalSchema.Fields()) {
		t.Fatalf("mapping has %d entries, schema has %d", len(mapping), len(finalSchema.Fields()))
	}
	for i, field := range finalSchema.Fields() {
		if mapping[i].ID() != field.ID || mapping[i].Names[0] != field.Name {
			t.Fatalf("mapping entry %d = %v, want id %d name %s", i, mapping[i], field.ID, field.Name)
		}
	}
}
