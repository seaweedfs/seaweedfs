package s3tablestest

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/twmb/avro"
	"github.com/twmb/avro/ocf"
)

// ForeignPartitionManifest rewrites a manifest iceberg-go wrote so its
// partition field carries the Avro union a foreign writer emits: [<type>, null]
// rather than [null, <type>]. Both are valid Avro, but iceberg-go reads a
// partition field's logical type from the union's last branch, so the ordering
// decides whether it hands back an Iceberg value or the rich Go value the Avro
// decoder produced.
//
// logicalType is stamped onto the value branch, since iceberg-go writes a day
// partition as a bare int, and decoded replaces the partition value in the
// record, standing in for what the foreign writer encoded.
func ForeignPartitionManifest(
	t *testing.T,
	schema *iceberg.Schema,
	spec iceberg.PartitionSpec,
	entry iceberg.ManifestEntry,
	manifestPath, logicalType string,
	decoded any,
) ([]byte, iceberg.ManifestFile) {
	t.Helper()

	var original bytes.Buffer
	manifest, err := iceberg.WriteManifest(manifestPath, &original, 2, spec, schema, 1, []iceberg.ManifestEntry{entry})
	if err != nil {
		t.Fatalf("write base manifest: %v", err)
	}

	reader, err := ocf.NewReader(bytes.NewReader(original.Bytes()))
	if err != nil {
		t.Fatalf("open base manifest: %v", err)
	}
	metadata := reader.Metadata()
	var record map[string]any
	if err := reader.Decode(&record); err != nil {
		t.Fatalf("decode base manifest: %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("close base manifest: %v", err)
	}

	fieldName := spec.Field(0).Name

	var schemaDoc map[string]any
	if err := json.Unmarshal(metadata["avro.schema"], &schemaDoc); err != nil {
		t.Fatalf("decode manifest schema: %v", err)
	}
	dataFile := findAvroField(t, schemaDoc, "data_file")
	partition := findAvroField(t, dataFile, "partition")
	partitionFields, ok := partition["fields"].([]any)
	if !ok {
		t.Fatalf("partition schema fields have type %T", partition["fields"])
	}
	for _, rawField := range partitionFields {
		field, ok := rawField.(map[string]any)
		if !ok || field["name"] != fieldName {
			continue
		}
		fieldType, ok := field["type"].([]any)
		if !ok || len(fieldType) != 2 {
			t.Fatalf("%s schema type = %#v, want nullable union", fieldName, field["type"])
		}
		valueType, ok := fieldType[1].(map[string]any)
		if !ok {
			primitive, primitiveOK := fieldType[1].(string)
			if !primitiveOK {
				t.Fatalf("%s value branch = %T, want Avro type", fieldName, fieldType[1])
			}
			valueType = map[string]any{"type": primitive}
		}
		valueType["logicalType"] = logicalType
		field["type"] = []any{valueType, "null"}
		break
	}

	dataFileRecord, ok := record["data_file"].(map[string]any)
	if !ok {
		t.Fatalf("decoded data_file = %T, want record", record["data_file"])
	}
	partitionRecord, ok := dataFileRecord["partition"].(map[string]any)
	if !ok {
		t.Fatalf("decoded partition = %T, want record", dataFileRecord["partition"])
	}
	partitionRecord[fieldName] = decoded

	foreignSchemaJSON, err := json.Marshal(schemaDoc)
	if err != nil {
		t.Fatalf("encode foreign manifest schema: %v", err)
	}
	foreignSchema, err := avro.Parse(string(foreignSchemaJSON))
	if err != nil {
		t.Fatalf("parse foreign manifest schema: %v", err)
	}
	userMetadata := make(map[string][]byte)
	for key, value := range metadata {
		if key != "avro.schema" && key != "avro.codec" {
			userMetadata[key] = value
		}
	}
	var foreign bytes.Buffer
	writer, err := ocf.NewWriter(&foreign, foreignSchema, ocf.WithSchema(string(foreignSchemaJSON)), ocf.WithMetadata(userMetadata))
	if err != nil {
		t.Fatalf("create foreign manifest writer: %v", err)
	}
	if err := writer.Encode(record); err != nil {
		t.Fatalf("encode foreign manifest: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close foreign manifest writer: %v", err)
	}

	return foreign.Bytes(), iceberg.NewManifestFile(2, manifest.FilePath(), int64(foreign.Len()), manifest.PartitionSpecID(), 1).AddedFiles(1).SequenceNum(1, 1).Build()
}

func findAvroField(t *testing.T, record map[string]any, name string) map[string]any {
	t.Helper()
	fields, ok := record["fields"].([]any)
	if !ok {
		t.Fatalf("Avro record fields have type %T", record["fields"])
	}
	for _, rawField := range fields {
		field, ok := rawField.(map[string]any)
		if ok && field["name"] == name {
			fieldType, ok := field["type"].(map[string]any)
			if ok {
				return fieldType
			}
			t.Fatalf("Avro field %q type has type %T", name, field["type"])
		}
	}
	t.Fatalf("Avro field %q not found", name)
	return nil
}
