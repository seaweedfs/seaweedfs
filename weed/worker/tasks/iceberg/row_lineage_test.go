package iceberg

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/parquet-go/parquet-go"

	filer_pb "github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

type lineageDataRow struct {
	ID   int64  `parquet:"id"`
	Name string `parquet:"name"`
}

func putDataFileRows[T any](t *testing.T, fs *fakeFilerServer, name string, rows []T) {
	t.Helper()
	var buf bytes.Buffer
	w := parquet.NewWriter(&buf, parquet.SchemaOf(new(T)))
	for _, r := range rows {
		if err := w.Write(&r); err != nil {
			t.Fatalf("write row: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	fs.putEntry("/buckets/test-bucket/ns/tbl/data", name, &filer_pb.Entry{
		Name:    name,
		Content: buf.Bytes(),
	})
}

func lineageEntry(t *testing.T, path string, firstRowID, dataSeq, recordCount int64) iceberg.ManifestEntry {
	t.Helper()
	dfb, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, iceberg.EntryContentData,
		path, iceberg.ParquetFile, map[int]any{}, nil, nil, recordCount, 1024)
	if err != nil {
		t.Fatalf("build data file: %v", err)
	}
	snapID := int64(1)
	return iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapID, &dataSeq, nil,
		dfb.FirstRowID(firstRowID).Build())
}

// mergedColumns returns each merged row's (id, name, _row_id,
// _last_updated_sequence_number): the two lineage columns the v3 merge
// appends after the data columns.
func mergedColumns(t *testing.T, data []byte) (ids, names, rowIDs, seqs []any) {
	t.Helper()
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("open merged: %v", err)
	}
	fields := f.Schema().Fields()
	if len(fields) != 4 {
		t.Fatalf("merged schema has %d fields, want 4 (id, name, _row_id, _last_updated_sequence_number)", len(fields))
	}
	if fields[2].Name() != iceberg.RowIDColumnName || fields[2].ID() != iceberg.RowIDFieldID {
		t.Fatalf("field 2 = %s id %d, want %s id %d", fields[2].Name(), fields[2].ID(), iceberg.RowIDColumnName, iceberg.RowIDFieldID)
	}
	if fields[3].Name() != iceberg.LastUpdatedSequenceNumberColumnName || fields[3].ID() != iceberg.LastUpdatedSequenceNumberFieldID {
		t.Fatalf("field 3 = %s id %d, want %s id %d", fields[3].Name(), fields[3].ID(), iceberg.LastUpdatedSequenceNumberColumnName, iceberg.LastUpdatedSequenceNumberFieldID)
	}
	for _, rg := range f.RowGroups() {
		rows := rg.Rows()
		buf := make([]parquet.Row, 8)
		for {
			n, err := rows.ReadRows(buf)
			for _, row := range buf[:n] {
				ids = append(ids, row[0].Int64())
				names = append(names, row[1].String())
				if row[2].IsNull() {
					rowIDs = append(rowIDs, nil)
				} else {
					rowIDs = append(rowIDs, row[2].Int64())
				}
				if row[3].IsNull() {
					seqs = append(seqs, nil)
				} else {
					seqs = append(seqs, row[3].Int64())
				}
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("read merged rows: %v", err)
			}
		}
		rows.Close()
	}
	return
}

// On a v3 table the merged file carries every row's identity: the
// materialized value when the input has it, else the one inheritance
// gives it (the input file's first_row_id plus the row's position, the
// entry's data sequence number).
func TestMergeParquetFilesRowLineage(t *testing.T) {
	fs, client := startFakeFiler(t)
	putDataFileRows(t, fs, "file1.parquet", []lineageDataRow{{1, "alice"}, {2, "bob"}, {3, "carl"}})
	putDataFileRows(t, fs, "file2.parquet", []lineageDataRow{{4, "dave"}, {5, "eve"}})

	entries := []iceberg.ManifestEntry{
		lineageEntry(t, "data/file1.parquet", 100, 7, 3),
		lineageEntry(t, "data/file2.parquet", 200, 9, 2),
	}

	merged, count, err := mergeParquetFiles(context.Background(), client, "test-bucket", "ns/tbl",
		entries, nil, nil, nil, true)
	if err != nil {
		t.Fatalf("mergeParquetFiles: %v", err)
	}
	if count != 5 {
		t.Fatalf("expected 5 merged rows, got %d", count)
	}

	_, _, rowIDs, seqs := mergedColumns(t, merged)
	wantIDs := []any{int64(100), int64(101), int64(102), int64(200), int64(201)}
	wantSeqs := []any{int64(7), int64(7), int64(7), int64(9), int64(9)}
	for i := range wantIDs {
		if rowIDs[i] != wantIDs[i] {
			t.Errorf("row %d _row_id = %v, want %v", i, rowIDs[i], wantIDs[i])
		}
		if seqs[i] != wantSeqs[i] {
			t.Errorf("row %d _last_updated_sequence_number = %v, want %v", i, seqs[i], wantSeqs[i])
		}
	}
}

// A position delete removes a row without renumbering the survivors:
// each kept row's _row_id is the file's first_row_id plus its position
// before the delete.
func TestMergeParquetFilesRowLineagePositionDelete(t *testing.T) {
	fs, client := startFakeFiler(t)
	putDataFileRows(t, fs, "file1.parquet", []lineageDataRow{{1, "alice"}, {2, "bob"}, {3, "carl"}})

	entries := []iceberg.ManifestEntry{lineageEntry(t, "data/file1.parquet", 100, 7, 3)}
	posDeletes := map[string][]int64{"ns/tbl/data/file1.parquet": {1}}

	merged, count, err := mergeParquetFiles(context.Background(), client, "test-bucket", "ns/tbl",
		entries, posDeletes, nil, nil, true)
	if err != nil {
		t.Fatalf("mergeParquetFiles: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2 merged rows, got %d", count)
	}
	ids, _, rowIDs, _ := mergedColumns(t, merged)
	if ids[0] != int64(1) || ids[1] != int64(3) {
		t.Fatalf("merged ids = %v, want [1 3]", ids)
	}
	if rowIDs[0] != int64(100) || rowIDs[1] != int64(102) {
		t.Fatalf("merged _row_id = %v, want [100 102]", rowIDs)
	}
}

// A file an engine already wrote with lineage columns materializes its
// rows' values; merged with a plain file, each input keeps its own
// provenance.
func TestMergeParquetFilesRowLineageMaterialized(t *testing.T) {
	fs, client := startFakeFiler(t)

	type materializedRow struct {
		ID     int64  `parquet:"id"`
		Name   string `parquet:"name"`
		RowID  int64  `parquet:"_row_id,id(2147483540)"`
		LastUp int64  `parquet:"_last_updated_sequence_number,id(2147483539)"`
	}
	putDataFileRows(t, fs, "plain.parquet", []lineageDataRow{{1, "alice"}})
	putDataFileRows(t, fs, "mats.parquet", []materializedRow{{2, "bob", 5000, 42}})

	entries := []iceberg.ManifestEntry{
		lineageEntry(t, "data/plain.parquet", 100, 7, 1),
		lineageEntry(t, "data/mats.parquet", 200, 9, 1),
	}
	merged, count, err := mergeParquetFiles(context.Background(), client, "test-bucket", "ns/tbl",
		entries, nil, nil, nil, true)
	if err != nil {
		t.Fatalf("mergeParquetFiles: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2 merged rows, got %d", count)
	}
	_, _, rowIDs, seqs := mergedColumns(t, merged)
	if rowIDs[0] != int64(100) || rowIDs[1] != int64(5000) {
		t.Errorf("_row_id = %v, want [100 5000]", rowIDs)
	}
	if seqs[0] != int64(7) || seqs[1] != int64(42) {
		t.Errorf("_last_updated_sequence_number = %v, want [7 42]", seqs)
	}
}

// The v3 manifest list assigns first_row_id to manifests that have
// none, starting at the snapshot's first-row-id, and reports the range
// it assigned as the snapshot's added-rows.
func TestWriteManifestListV3RowIDs(t *testing.T) {
	schema := newTestSchema()
	spec := *iceberg.UnpartitionedSpec
	snapID := int64(12)
	dataSeq := int64(3)

	entry := iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapID, &dataSeq, nil,
		mustDataFile(t, spec, "data/a.parquet", 4))
	var manifestBuf bytes.Buffer
	mf, err := iceberg.WriteManifest("metadata/m0.avro", &manifestBuf, 3, spec, schema, snapID,
		[]iceberg.ManifestEntry{entry})
	if err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	var listBuf bytes.Buffer
	parent := int64(10)
	seqNum := int64(3)
	addedRows, err := writeManifestList(3, &listBuf, snapID, &parent, &seqNum, 500, []iceberg.ManifestFile{mf})
	if err != nil {
		t.Fatalf("writeManifestList: %v", err)
	}
	if addedRows == nil || *addedRows != 4 {
		t.Fatalf("addedRows = %v, want 4", addedRows)
	}

	readBack, err := iceberg.ReadManifestList(&listBuf)
	if err != nil {
		t.Fatalf("read manifest list: %v", err)
	}
	if len(readBack) != 1 || readBack[0].FirstRowID() == nil || *readBack[0].FirstRowID() != 500 {
		t.Fatalf("manifest first_row_id = %v, want 500", readBack)
	}
}

func mustDataFile(t *testing.T, spec iceberg.PartitionSpec, path string, recordCount int64) iceberg.DataFile {
	t.Helper()
	dfb, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentData, path, iceberg.ParquetFile,
		map[int]any{}, nil, nil, recordCount, 1024)
	if err != nil {
		t.Fatalf("build data file: %v", err)
	}
	return dfb.Build()
}

// A plan that assigned row IDs from a next-row-id the table no longer
// has is stale, like a moved head; so is a format version that changed
// since planning.
func TestCheckRowLineagePlan(t *testing.T) {
	if err := checkRowLineagePlan(3, 2, 0, 0); err == nil {
		t.Error("version change must be stale")
	}
	if err := checkRowLineagePlan(3, 3, 100, 200); err == nil {
		t.Error("moved next-row-id must be stale")
	}
	if err := checkRowLineagePlan(2, 2, 0, 0); err != nil {
		t.Errorf("v2 plan is never row-lineage stale: %v", err)
	}
	if err := checkRowLineagePlan(3, 3, 100, 100); err != nil {
		t.Errorf("unchanged next-row-id must not be stale: %v", err)
	}
}

// A data column named like a lineage column under a different field id
// refuses the bin rather than writing a doubled column.
func TestLineageLayoutRejectsReservedNameUnderOtherID(t *testing.T) {
	type badRow struct {
		ID    int64 `parquet:"id"`
		RowID int64 `parquet:"_row_id,id(7)"`
	}
	schema := parquet.SchemaOf(new(badRow))
	if _, err := newLineageLayout(schema); err == nil {
		t.Fatal("reserved name under a non-reserved field id must fail the layout")
	}
}

// Compaction on a v3 table reads row lineage the way a reader does: the
// entries in the stored manifest carry no first_row_id, so the ones
// compactDataFiles merges are the inherited values (the manifest-list
// first_row_id plus the preceding files' record counts). The merged file
// materializes those IDs and its new entry names its own first row's.
func TestCompactDataFilesV3RowLineageManifestRead(t *testing.T) {
	fs, client := startFakeFiler(t)
	ctx := context.Background()

	setup := tableSetup{BucketName: "test-bucket", Namespace: "ns", TableName: "tbl"}
	spec := *iceberg.UnpartitionedSpec
	snapID := int64(1)

	putDataFileRows(t, fs, "f1.parquet", []lineageDataRow{{1, "a"}, {2, "b"}, {3, "c"}})
	putDataFileRows(t, fs, "f2.parquet", []lineageDataRow{{4, "d"}, {5, "e"}})

	entries := []iceberg.ManifestEntry{
		iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapID, nil, nil,
			mustDataFile(t, spec, "data/f1.parquet", 3)),
		iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapID, nil, nil,
			mustDataFile(t, spec, "data/f2.parquet", 2)),
	}
	writeV3Table(t, fs, setup, entries, nil, 0, 5)

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
	newSnap := state.Metadata.CurrentSnapshot()
	if newSnap == nil || newSnap.SnapshotID == snapID {
		t.Fatalf("no new snapshot committed: %+v", newSnap)
	}
	if newSnap.FirstRowID == nil || *newSnap.FirstRowID != 5 {
		t.Fatalf("new snapshot first-row-id = %v, want 5", *newSnap.FirstRowID)
	}

	manifestListData, err := loadFileByIcebergPath(ctx, client, setup.BucketName, state.DataPath, newSnap.ManifestList)
	if err != nil {
		t.Fatalf("read new manifest list: %v", err)
	}
	manifests, err := s3tables.ReadManifestList(manifestListData)
	if err != nil {
		t.Fatalf("parse new manifest list: %v", err)
	}
	if len(manifests) != 1 {
		t.Fatalf("new snapshot has %d manifests, want 1", len(manifests))
	}
	manifestData, err := loadFileByIcebergPath(ctx, client, setup.BucketName, state.DataPath, manifests[0].FilePath())
	if err != nil {
		t.Fatalf("read new manifest: %v", err)
	}
	newEntries, err := s3tables.ReadManifest(manifests[0], manifestData, false, specByID(state.Metadata), state.Metadata.CurrentSchema())
	if err != nil {
		t.Fatalf("parse new manifest: %v", err)
	}

	var merged iceberg.DataFile
	var deletedPaths int
	for _, e := range newEntries {
		switch e.Status() {
		case iceberg.EntryStatusADDED:
			merged = e.DataFile()
		case iceberg.EntryStatusDELETED:
			deletedPaths++
		}
	}
	if deletedPaths != 2 {
		t.Fatalf("new manifest has %d deleted entries, want 2", deletedPaths)
	}
	if merged == nil {
		t.Fatal("new manifest has no added entry")
	}
	if merged.FirstRowID() == nil || *merged.FirstRowID() != 0 {
		t.Fatalf("merged entry first_row_id = %v, want 0", merged.FirstRowID())
	}

	mergedData, err := loadFileByIcebergPath(ctx, client, setup.BucketName, state.DataPath, merged.FilePath())
	if err != nil {
		t.Fatalf("read merged file: %v", err)
	}
	_, _, rowIDs, seqs := mergedColumns(t, mergedData)
	wantRowIDs := []any{int64(0), int64(1), int64(2), int64(3), int64(4)}
	for i, want := range wantRowIDs {
		if rowIDs[i] != want {
			t.Fatalf("row %d _row_id = %v, want %v", i, rowIDs[i], want)
		}
		if seqs[i] != int64(1) {
			t.Fatalf("row %d _last_updated_sequence_number = %v, want 1", i, seqs[i])
		}
	}
}
