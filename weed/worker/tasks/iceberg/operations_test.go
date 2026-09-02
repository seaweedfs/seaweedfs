package iceberg

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
	"github.com/twmb/avro"
	"github.com/twmb/avro/ocf"
	"google.golang.org/protobuf/proto"
)

func TestRewriteManifestsNormalizesForeignDayPartitions(t *testing.T) {
	fs, client := startFakeFiler(t)
	setup := tableSetup{
		BucketName: "test-bucket",
		Namespace:  "analytics",
		TableName:  "events",
		Snapshots: []table.Snapshot{{
			SnapshotID:   1,
			TimestampMs:  time.Now().UnixMilli(),
			ManifestList: "metadata/snap-1.avro",
		}},
	}
	// Reuse the standard table/filer fixture, then replace its unpartitioned
	// metadata with the date-partitioned table used by this regression test.
	populateTable(t, fs, setup)
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "event_time", Type: iceberg.PrimitiveTypes.Timestamp, Required: true},
	)
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "event_time_day", Transform: iceberg.DayTransform{},
	})
	meta := buildPartitionedTestMetadata(t, setup, schema, spec)
	replaceTestTableMetadata(t, fs, setup, meta)

	metaDir := path.Join(s3tables.TablesPath, setup.BucketName, setup.tablePath(), "metadata")
	var manifests []iceberg.ManifestFile
	for i := 0; i < 3; i++ {
		day := iceberg.Date(20737 + i)
		dfBuilder, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentData,
			setup.fileRef("data", fmt.Sprintf("foreign-%d.parquet", i)), iceberg.ParquetFile,
			map[int]any{1000: day}, nil, nil, 1, 1)
		if err != nil {
			t.Fatalf("build data file %d: %v", i, err)
		}
		snapshotID := int64(1)
		entry := iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapshotID, nil, nil, dfBuilder.Build())
		manifestName := fmt.Sprintf("foreign-manifest-%d.avro", i)
		manifestPath := setup.fileRef("metadata", manifestName)
		foreignBytes, manifest := makeForeignDayManifest(t, schema, spec, entry, manifestPath)
		foreignEntries, err := iceberg.ReadManifest(manifest, bytes.NewReader(foreignBytes), true)
		if err != nil {
			t.Fatalf("read foreign manifest %d: %v", i, err)
		}
		if _, ok := foreignEntries[0].DataFile().Partition()[1000].(time.Time); !ok {
			t.Fatalf("foreign manifest %d did not expose time.Time partition", i)
		}
		fs.putEntry(metaDir, manifestName, &filer_pb.Entry{
			Name: manifestName, Attributes: &filer_pb.FuseAttributes{Mtime: time.Now().Unix()}, Content: foreignBytes,
		})
		manifests = append(manifests, manifest)
	}

	var manifestList bytes.Buffer
	seqNum := int64(1)
	if err := iceberg.WriteManifestList(meta.Version(), &manifestList, 1, nil, &seqNum, 0, manifests); err != nil {
		t.Fatalf("write manifest list: %v", err)
	}
	fs.putEntry(metaDir, "snap-1.avro", &filer_pb.Entry{
		Name: "snap-1.avro", Attributes: &filer_pb.FuseAttributes{Mtime: time.Now().Unix()}, Content: manifestList.Bytes(),
	})

	result, _, err := NewHandler(nil).rewriteManifests(context.Background(), client, setup.BucketName, setup.tablePath(), Config{
		MinManifestsToRewrite: 3,
		MaxCommitRetries:      3,
	})
	if err != nil {
		t.Fatalf("rewriteManifests failed for foreign day partitions: %v", err)
	}
	if result != "rewrote 3 manifests into 1 (3 entries)" {
		t.Fatalf("rewriteManifests result = %q, want 3 manifests merged", result)
	}
}

func TestNormalizeDayPartitionTimesFromForeignManifest(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "event_time", Type: iceberg.PrimitiveTypes.Timestamp, Required: true},
	)
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1},
		FieldID:   1000,
		Name:      "event_time_day",
		Transform: iceberg.DayTransform{},
	})

	value := iceberg.Date(20737)
	dfBuilder, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentData, "data/file.parquet", iceberg.ParquetFile, map[int]any{
		1000: value,
	}, nil, nil, 1, 1)
	if err != nil {
		t.Fatalf("build data file: %v", err)
	}
	snapshotID := int64(1)
	entry := iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapshotID, nil, nil, dfBuilder.Build())

	foreignBytes, foreignManifest := makeForeignDayManifest(t, schema, spec, entry, "metadata/foreign-manifest.avro")
	foreignEntries, err := iceberg.ReadManifest(foreignManifest, bytes.NewReader(foreignBytes), true)
	if err != nil {
		t.Fatalf("read foreign manifest: %v", err)
	}
	foreignValue := foreignEntries[0].DataFile().Partition()[1000]
	if _, ok := foreignValue.(time.Time); !ok {
		t.Fatalf("foreign manifest partition value = %T, want time.Time", foreignValue)
	}

	normalizeDayPartitionTimes(foreignEntries, spec)

	if got := foreignEntries[0].DataFile().Partition()[1000]; got != value {
		t.Fatalf("normalized partition value = %#v, want %#v", got, value)
	}

	var buf bytes.Buffer
	manifest, err := iceberg.WriteManifest("metadata/manifest.avro", &buf, 2, spec, schema, snapshotID, foreignEntries)
	if err != nil {
		t.Fatalf("write normalized manifest: %v", err)
	}
	entries, err := iceberg.ReadManifest(manifest, bytes.NewReader(buf.Bytes()), true)
	if err != nil {
		t.Fatalf("read normalized manifest: %v", err)
	}
	if got := entries[0].DataFile().Partition()[1000]; got != int32(value) {
		t.Fatalf("round-tripped partition value = %#v (%T), want %d (int32)", got, got, value)
	}
}

func buildPartitionedTestMetadata(t *testing.T, setup tableSetup, schema *iceberg.Schema, spec iceberg.PartitionSpec) table.Metadata {
	t.Helper()
	meta, err := table.NewMetadata(schema, &spec, table.UnsortedSortOrder, "s3://test-bucket/test-table", nil)
	if err != nil {
		t.Fatalf("create partitioned metadata: %v", err)
	}
	builder, err := table.MetadataBuilderFromBase(meta, "s3://test-bucket/test-table")
	if err != nil {
		t.Fatalf("create metadata builder: %v", err)
	}
	snapshot := setup.Snapshots[0]
	if err := builder.AddSnapshot(&snapshot); err != nil {
		t.Fatalf("add snapshot: %v", err)
	}
	if err := builder.SetSnapshotRef(table.MainBranch, snapshot.SnapshotID, table.BranchRef); err != nil {
		t.Fatalf("set main branch: %v", err)
	}
	meta, err = builder.Build()
	if err != nil {
		t.Fatalf("build partitioned metadata: %v", err)
	}
	return meta
}

func replaceTestTableMetadata(t *testing.T, fs *fakeFilerServer, setup tableSetup, meta table.Metadata) {
	t.Helper()
	fullMetadataJSON, err := json.Marshal(meta)
	if err != nil {
		t.Fatalf("marshal partitioned metadata: %v", err)
	}
	internalMeta, err := json.Marshal(map[string]any{
		"metadataVersion":  1,
		"metadataLocation": setup.fileRef("metadata", "v1.metadata.json"),
		"metadata":         map[string]any{"fullMetadata": json.RawMessage(fullMetadataJSON)},
	})
	if err != nil {
		t.Fatalf("marshal metadata xattr: %v", err)
	}
	nsDir := path.Join(s3tables.TablesPath, setup.BucketName, setup.Namespace)
	entry := fs.getEntry(nsDir, setup.TableName)
	if entry == nil {
		t.Fatalf("table entry missing")
	}
	updated := proto.Clone(entry).(*filer_pb.Entry)
	updated.Extended = map[string][]byte{
		s3tables.ExtendedKeyMetadata:        internalMeta,
		s3tables.ExtendedKeyMetadataVersion: metadataVersionXattr(1),
	}
	fs.putEntry(nsDir, setup.TableName, updated)
}

// makeForeignDayManifest changes a valid manifest's partition union from the
// Iceberg-go shape [null, int] to the shape emitted by some foreign writers:
// [date, null]. The latter is valid Avro, but iceberg-go uses the last union
// branch to discover logical types and therefore exposes the date as time.Time.
func makeForeignDayManifest(t *testing.T, schema *iceberg.Schema, spec iceberg.PartitionSpec, entry iceberg.ManifestEntry, manifestPath string) ([]byte, iceberg.ManifestFile) {
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
		if !ok || field["name"] != "event_time_day" {
			continue
		}
		fieldType, ok := field["type"].([]any)
		if !ok || len(fieldType) != 2 {
			t.Fatalf("event_time_day schema type = %#v, want nullable union", field["type"])
		}
		dateType, ok := fieldType[1].(map[string]any)
		if !ok {
			primitive, primitiveOK := fieldType[1].(string)
			if !primitiveOK {
				t.Fatalf("event_time_day value branch = %T, want Avro type", fieldType[1])
			}
			dateType = map[string]any{"type": primitive}
		}
		dateType["logicalType"] = "date"
		field["type"] = []any{dateType, "null"}
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
	partitionRecord["event_time_day"] = time.Unix(int64(valueDays(entry))*24*60*60, 0).UTC()

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

func valueDays(entry iceberg.ManifestEntry) iceberg.Date {
	return entry.DataFile().Partition()[1000].(iceberg.Date)
}
