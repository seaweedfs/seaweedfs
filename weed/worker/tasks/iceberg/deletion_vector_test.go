package iceberg

import (
	"bytes"
	"context"
	"encoding/json"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/parquet-go/parquet-go"

	filer_pb "github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

// A deletion-vector entry names its data file in referenced_data_file; the
// match against candidates is on the normalized path either side takes.
func TestDeletionVectorPathMatching(t *testing.T) {
	spec := *iceberg.UnpartitionedSpec
	snapID := int64(1)

	newEntry := func(path, ref string) iceberg.ManifestEntry {
		dfb, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentPosDeletes, path,
			iceberg.PuffinFile, map[int]any{}, nil, nil, 0, 8)
		if err != nil {
			t.Fatalf("build dv file: %v", err)
		}
		if ref != "" {
			dfb.ReferencedDataFile(ref)
		}
		return iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapID, nil, nil, dfb.Build())
	}

	paths, err := deletionVectorPaths([]iceberg.ManifestEntry{
		newEntry("metadata/dv.puffin", "s3://test-bucket/ns/tbl/data/f1.parquet"),
	}, "test-bucket", "ns/tbl")
	if err != nil {
		t.Fatalf("deletionVectorPaths: %v", err)
	}
	if !paths["ns/tbl/data/f1.parquet"] {
		t.Fatalf("dv paths = %v, want the normalized f1 path", paths)
	}

	dataEntry := iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapID, nil, nil,
		mustDataFile(t, spec, "data/f1.parquet", 3))
	otherEntry := iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapID, nil, nil,
		mustDataFile(t, spec, "data/f2.parquet", 2))
	kept, err := excludeDeletionVectorFiles([]iceberg.ManifestEntry{dataEntry, otherEntry}, paths, "test-bucket", "ns/tbl")
	if err != nil {
		t.Fatalf("excludeDeletionVectorFiles: %v", err)
	}
	if len(kept) != 1 || kept[0].DataFile().FilePath() != "data/f2.parquet" {
		t.Fatalf("kept %d entries, want only f2", len(kept))
	}
}

// A Puffin deletion vector is not a Parquet position-delete file: it must not
// reach readPositionDeleteFile, and the data file it covers must stay
// uncompacted until something applies the vector. Other files still merge and
// ordinary Parquet position deletes still apply.
func TestCompactDataFilesDeletionVector(t *testing.T) {
	fs, client := startFakeFiler(t)
	ctx := context.Background()

	setup := tableSetup{BucketName: "test-bucket", Namespace: "ns", TableName: "tbl"}
	spec := *iceberg.UnpartitionedSpec
	snapID := int64(1)

	putDataFileRows(t, fs, "f1.parquet", []lineageDataRow{{1, "a"}, {2, "b"}})
	putDataFileRows(t, fs, "f2.parquet", []lineageDataRow{{3, "c"}, {4, "d"}})
	putDataFileRows(t, fs, "f3.parquet", []lineageDataRow{{5, "e"}})

	dataEntries := []iceberg.ManifestEntry{
		iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapID, nil, nil,
			mustDataFile(t, spec, "data/f1.parquet", 2)),
		iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapID, nil, nil,
			mustDataFile(t, spec, "data/f2.parquet", 2)),
		iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapID, nil, nil,
			mustDataFile(t, spec, "data/f3.parquet", 1)),
	}

	// An ordinary Parquet position delete against f2's first row.
	type posRow struct {
		FilePath string `parquet:"file_path"`
		Pos      int64  `parquet:"pos"`
	}
	var pdBuf bytes.Buffer
	w := parquet.NewWriter(&pdBuf, parquet.SchemaOf(new(posRow)))
	if err := w.Write(&posRow{"data/f2.parquet", 0}); err != nil {
		t.Fatalf("write pos delete: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close pos delete: %v", err)
	}
	dataDir := "/buckets/test-bucket/ns/tbl/data"
	fs.putEntry(dataDir, "pd1.parquet", &filer_pb.Entry{Name: "pd1.parquet", Content: pdBuf.Bytes()})
	// The vector itself is never opened, but the file should exist like a real
	// engine's write would leave it.
	fs.putEntry(dataDir, "dv.puffin", &filer_pb.Entry{Name: "dv.puffin", Content: []byte("puffin-bytes")})

	posDF, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentPosDeletes,
		setup.fileRef("data", "pd1.parquet"), iceberg.ParquetFile, map[int]any{}, nil, nil, 1, int64(pdBuf.Len()))
	if err != nil {
		t.Fatalf("build pos delete file: %v", err)
	}
	dvDF, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentPosDeletes,
		setup.fileRef("data", "dv.puffin"), iceberg.PuffinFile, map[int]any{}, nil, nil, 0, 12)
	if err != nil {
		t.Fatalf("build dv file: %v", err)
	}
	dvDF.ReferencedDataFile("s3://test-bucket/ns/tbl/data/f1.parquet")
	deleteEntries := []iceberg.ManifestEntry{
		iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapID, nil, nil, posDF.Build()),
		iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapID, nil, nil, dvDF.Build()),
	}

	writeV3Table(t, fs, setup, dataEntries, deleteEntries, 0, 5)

	handler := NewHandler(nil)
	config := Config{
		TargetFileSizeBytes: 4096,
		MinInputFiles:       2,
		ApplyDeletes:        true,
	}
	result, _, err := handler.compactDataFiles(ctx, client, setup.BucketName, setup.tablePath(), config, nil)
	if err != nil {
		t.Fatalf("compactDataFiles: %v", err)
	}
	if !strings.Contains(result, "compacted 2 files into 1") {
		t.Fatalf("unexpected result: %q", result)
	}

	state, err := loadCurrentMetadata(ctx, client, setup.BucketName, setup.tablePath())
	if err != nil {
		t.Fatalf("loadCurrentMetadata: %v", err)
	}
	manifests, err := loadCurrentManifests(ctx, client, setup.BucketName, state.DataPath, state.Metadata)
	if err != nil {
		t.Fatalf("loadCurrentManifests: %v", err)
	}

	var dataMfs, deleteMfs []iceberg.ManifestFile
	for _, mf := range manifests {
		if mf.ManifestContent() == iceberg.ManifestContentData {
			dataMfs = append(dataMfs, mf)
		} else {
			deleteMfs = append(deleteMfs, mf)
		}
	}
	if len(deleteMfs) != 1 {
		t.Fatalf("delete manifests carried = %d, want 1 (the vector still applies to f1)", len(deleteMfs))
	}

	var merged iceberg.DataFile
	var f1Live bool
	for _, mf := range dataMfs {
		manifestData, err := loadFileByIcebergPath(ctx, client, setup.BucketName, state.DataPath, mf.FilePath())
		if err != nil {
			t.Fatalf("read manifest: %v", err)
		}
		entries, err := s3tables.ReadManifest(mf, manifestData, false, specByID(state.Metadata), state.Metadata.CurrentSchema())
		if err != nil {
			t.Fatalf("parse manifest: %v", err)
		}
		for _, e := range entries {
			df := e.DataFile()
			switch {
			case e.Status() == iceberg.EntryStatusADDED:
				merged = df
			case e.Status() == iceberg.EntryStatusEXISTING && df.FilePath() == "data/f1.parquet":
				f1Live = true
			}
		}
	}
	if !f1Live {
		t.Fatal("f1 is covered by a deletion vector and must stay live")
	}
	if merged == nil {
		t.Fatal("no merged file added")
	}

	mergedData, err := loadFileByIcebergPath(ctx, client, setup.BucketName, state.DataPath, merged.FilePath())
	if err != nil {
		t.Fatalf("read merged file: %v", err)
	}
	ids, _, rowIDs, _ := mergedColumns(t, mergedData)
	if len(ids) != 2 || ids[0] != int64(4) || ids[1] != int64(5) {
		t.Fatalf("merged ids = %v, want [4 5] (f2 minus its deleted row, f3)", ids)
	}
	if rowIDs[0] != int64(3) || rowIDs[1] != int64(4) {
		t.Fatalf("merged _row_id = %v, want [3 4]", rowIDs)
	}
}

// Detection applies the same exclusion compaction does: a table whose only
// binable files are all covered by deletion vectors is not a candidate, while
// unaffected files still qualify.
func TestDetectSchedulesDeletionVector(t *testing.T) {
	spec := *iceberg.UnpartitionedSpec
	snapID := int64(1)

	dataEntry := func(name string, count int64) iceberg.ManifestEntry {
		return iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapID, nil, nil,
			mustDataFile(t, spec, name, count))
	}
	dvEntry := func(t *testing.T, ref string) iceberg.ManifestEntry {
		dfb, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentPosDeletes,
			"data/dv.puffin", iceberg.PuffinFile, map[int]any{}, nil, nil, 0, 12)
		if err != nil {
			t.Fatalf("build dv file: %v", err)
		}
		dfb.ReferencedDataFile(ref)
		return iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapID, nil, nil, dfb.Build())
	}

	config := Config{
		SnapshotRetentionMs: hoursToMs(24 * 365),
		MaxSnapshotsToKeep:  10,
		TargetFileSizeBytes: 4096,
		MinInputFiles:       2,
		Operations:          "compact",
		ApplyDeletes:        true,
	}

	t.Run("unaffected files stay eligible", func(t *testing.T) {
		fs, client := startFakeFiler(t)
		setup := tableSetup{BucketName: "test-bucket", Namespace: "ns", TableName: "tbl"}
		writeV3Table(t, fs, setup,
			[]iceberg.ManifestEntry{dataEntry("data/f1.parquet", 1), dataEntry("data/f2.parquet", 1), dataEntry("data/f3.parquet", 1)},
			[]iceberg.ManifestEntry{dvEntry(t, "data/f1.parquet")},
			0, 3)

		tables, err := NewHandler(nil).scanTablesForMaintenance(context.Background(), client, config, "", "", "", 0)
		if err != nil {
			t.Fatalf("scanTablesForMaintenance: %v", err)
		}
		if len(tables) != 1 {
			t.Fatalf("expected 1 compaction candidate (f2+f3 still binable), got %d", len(tables))
		}
	})

	t.Run("all binable files covered", func(t *testing.T) {
		fs, client := startFakeFiler(t)
		setup := tableSetup{BucketName: "test-bucket", Namespace: "ns", TableName: "tbl"}
		writeV3Table(t, fs, setup,
			[]iceberg.ManifestEntry{dataEntry("data/f1.parquet", 1), dataEntry("data/f2.parquet", 1)},
			[]iceberg.ManifestEntry{dvEntry(t, "data/f1.parquet")},
			0, 2)

		tables, err := NewHandler(nil).scanTablesForMaintenance(context.Background(), client, config, "", "", "", 0)
		if err != nil {
			t.Fatalf("scanTablesForMaintenance: %v", err)
		}
		if len(tables) != 0 {
			t.Fatalf("expected no candidates (only the covered f1 and the lone f2 remain), got %d", len(tables))
		}
	})
}

// writeV3Table stores a format-version-3 table in the fake filer: a data
// manifest whose entries carry no first_row_id (so readers exercise manifest
// first_row_id inheritance), an optional delete manifest, and the manifest
// list + snapshot that commit them at sequence number 1.
func writeV3Table(t *testing.T, fs *fakeFilerServer, setup tableSetup,
	dataManifestEntries, deleteManifestEntries []iceberg.ManifestEntry,
	listFirstRowID, addedRows int64) table.Metadata {
	t.Helper()

	schema := newTestSchema()
	spec := *iceberg.UnpartitionedSpec
	location := "s3://" + setup.BucketName + "/" + setup.dataPath()

	base, err := table.NewMetadata(schema, &spec, table.UnsortedSortOrder, location, nil)
	if err != nil {
		t.Fatalf("new metadata: %v", err)
	}
	builder, err := table.MetadataBuilderFromBase(base, location)
	if err != nil {
		t.Fatalf("metadata builder: %v", err)
	}
	if err := builder.SetFormatVersion(3); err != nil {
		t.Fatalf("set format version: %v", err)
	}
	if _, err := builder.Build(); err != nil {
		t.Fatalf("build v3 metadata: %v", err)
	}

	snapID := int64(1)
	var manifestBuf bytes.Buffer
	dataMf, err := iceberg.WriteManifest(setup.fileRef("metadata", "m0.avro"), &manifestBuf, 3, spec, schema, snapID, dataManifestEntries)
	if err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	metaDir := path.Join(s3tables.TablesPath, setup.BucketName, setup.dataPath(), "metadata")
	fs.putEntry(metaDir, "m0.avro", &filer_pb.Entry{
		Name:    "m0.avro",
		Content: manifestBuf.Bytes(),
	})

	manifests := []iceberg.ManifestFile{dataMf}
	if len(deleteManifestEntries) > 0 {
		delMf, delBytes := writeDeleteManifestForTest(
			t, setup.fileRef("metadata", "del.avro"), 3, spec, schema, snapID, deleteManifestEntries)
		fs.putEntry(metaDir, "del.avro", &filer_pb.Entry{
			Name:    "del.avro",
			Content: delBytes,
		})
		manifests = append(manifests, delMf)
	}

	var listBuf bytes.Buffer
	seqNum := int64(1)
	if err := iceberg.WriteManifestList(3, &listBuf, snapID, nil, &seqNum, listFirstRowID, manifests); err != nil {
		t.Fatalf("write manifest list: %v", err)
	}
	fs.putEntry(metaDir, "snap-1.avro", &filer_pb.Entry{
		Name:    "snap-1.avro",
		Content: listBuf.Bytes(),
	})

	snap := table.Snapshot{
		SnapshotID:     snapID,
		SequenceNumber: seqNum,
		TimestampMs:    time.Now().UnixMilli(),
		ManifestList:   setup.fileRef("metadata", "snap-1.avro"),
		FirstRowID:     ptrInt64(listFirstRowID),
		AddedRows:      &addedRows,
	}
	if err := builder.AddSnapshot(&snap); err != nil {
		t.Fatalf("add snapshot: %v", err)
	}
	if err := builder.SetSnapshotRef(table.MainBranch, snapID, table.BranchRef); err != nil {
		t.Fatalf("set snapshot ref: %v", err)
	}
	meta, err := builder.Build()
	if err != nil {
		t.Fatalf("build metadata with snapshot: %v", err)
	}

	fullMetadataJSON, err := json.Marshal(meta)
	if err != nil {
		t.Fatalf("marshal metadata: %v", err)
	}
	internalMeta := map[string]interface{}{
		"metadataVersion":  1,
		"metadataLocation": setup.fileRef("metadata", "v1.metadata.json"),
		"metadata":         map[string]interface{}{"fullMetadata": json.RawMessage(fullMetadataJSON)},
	}
	xattr, err := json.Marshal(internalMeta)
	if err != nil {
		t.Fatalf("marshal xattr: %v", err)
	}
	bucketPath := path.Join(s3tables.TablesPath, setup.BucketName)
	fs.putEntry(s3tables.TablesPath, setup.BucketName, &filer_pb.Entry{
		Name:        setup.BucketName,
		IsDirectory: true,
		Extended:    map[string][]byte{s3tables.ExtendedKeyTableBucket: []byte("true")},
	})
	fs.putEntry(bucketPath, setup.Namespace, &filer_pb.Entry{Name: setup.Namespace, IsDirectory: true})
	fs.putEntry(path.Join(bucketPath, setup.Namespace), setup.TableName, &filer_pb.Entry{
		Name:        setup.TableName,
		IsDirectory: true,
		Extended: map[string][]byte{
			s3tables.ExtendedKeyMetadata:        xattr,
			s3tables.ExtendedKeyMetadataVersion: metadataVersionXattr(1),
		},
	})
	return meta
}

func ptrInt64(v int64) *int64 { return &v }
