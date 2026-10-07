package iceberg

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"

	filer_pb "github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

type boundedFile struct {
	Name         string
	IDs          []int64
	Lower, Upper int64
	SizeBytes    int64
	NoBounds     bool
}

// populateBoundedTable is populateTableWithDeleteFiles narrowed to data
// files whose manifest entries carry bounds on the id column.
func populateBoundedTable(t *testing.T, fs *fakeFilerServer, setup tableSetup, files []boundedFile) {
	t.Helper()
	populateBoundedTableSorted(t, fs, setup, files, table.UnsortedSortOrder)
}

func populateBoundedTableSorted(t *testing.T, fs *fakeFilerServer, setup tableSetup, files []boundedFile, sortOrder table.SortOrder) {
	t.Helper()
	schema := newTestSchema()
	spec := *iceberg.UnpartitionedSpec

	meta, err := table.NewMetadata(schema, &spec, sortOrder, "s3://"+setup.BucketName+"/"+setup.dataPath(), nil)
	if err != nil {
		t.Fatalf("create metadata: %v", err)
	}

	bucketPath := path.Join(s3tables.TablesPath, setup.BucketName)
	nsPath := path.Join(bucketPath, setup.Namespace)
	tableFilerPath := path.Join(bucketPath, setup.dataPath())
	metaDir := path.Join(tableFilerPath, "metadata")
	dataDir := path.Join(tableFilerPath, "data")
	version := meta.Version()

	var entries []iceberg.ManifestEntry
	for _, f := range files {
		rows := make([]struct {
			ID   int64
			Name string
		}, len(f.IDs))
		for i, id := range f.IDs {
			rows[i] = struct {
				ID   int64
				Name string
			}{id, fmt.Sprintf("r%d", id)}
		}
		data := writeTestParquetFile(t, fs, dataDir, f.Name, rows)
		size := int64(len(data))
		if f.SizeBytes > 0 {
			size = f.SizeBytes
		}
		dfb, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentData, setup.fileRef("data", f.Name),
			iceberg.ParquetFile, map[int]any{}, nil, nil, int64(len(f.IDs)), size)
		if err != nil {
			t.Fatalf("build data file %s: %v", f.Name, err)
		}
		if !f.NoBounds {
			lo, _ := iceberg.Int64Literal(f.Lower).MarshalBinary()
			hi, _ := iceberg.Int64Literal(f.Upper).MarshalBinary()
			dfb.LowerBoundValues(map[int][]byte{1: lo}).UpperBoundValues(map[int][]byte{1: hi})
		}
		snapID := int64(1)
		entries = append(entries, iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapID, nil, nil, dfb.Build()))
	}

	var manifestBuf bytes.Buffer
	mf, err := iceberg.WriteManifest(setup.fileRef("metadata", "data-manifest-1.avro"), &manifestBuf,
		version, spec, schema, 1, entries)
	if err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	fs.putEntry(metaDir, "data-manifest-1.avro", &filer_pb.Entry{
		Name: "data-manifest-1.avro", Content: manifestBuf.Bytes(),
		Attributes: &filer_pb.FuseAttributes{Mtime: time.Now().Unix(), FileSize: uint64(manifestBuf.Len())},
	})

	var mlBuf bytes.Buffer
	seqNum := int64(1)
	if err := iceberg.WriteManifestList(version, &mlBuf, 1, nil, &seqNum, 0, []iceberg.ManifestFile{mf}); err != nil {
		t.Fatalf("write manifest list: %v", err)
	}
	fs.putEntry(metaDir, "snap-1.avro", &filer_pb.Entry{
		Name: "snap-1.avro", Content: mlBuf.Bytes(),
		Attributes: &filer_pb.FuseAttributes{Mtime: time.Now().Unix(), FileSize: uint64(mlBuf.Len())},
	})

	snap := table.Snapshot{SnapshotID: 1, TimestampMs: time.Now().UnixMilli(),
		ManifestList: setup.fileRef("metadata", "snap-1.avro")}
	builder, err := table.MetadataBuilderFromBase(meta, "s3://"+setup.BucketName+"/"+setup.dataPath())
	if err != nil {
		t.Fatalf("metadata builder: %v", err)
	}
	if err := builder.AddSnapshot(&snap); err != nil {
		t.Fatalf("add snapshot: %v", err)
	}
	if err := builder.SetSnapshotRef(table.MainBranch, snap.SnapshotID, table.BranchRef); err != nil {
		t.Fatalf("set snapshot ref: %v", err)
	}
	meta, err = builder.Build()
	if err != nil {
		t.Fatalf("build metadata: %v", err)
	}

	fullMetadataJSON, _ := json.Marshal(meta)
	xattr, _ := json.Marshal(map[string]interface{}{
		"metadataVersion":  1,
		"metadataLocation": setup.fileRef("metadata", "v1.metadata.json"),
		"metadata":         map[string]interface{}{"fullMetadata": json.RawMessage(fullMetadataJSON)},
	})
	fs.putEntry(path.Join(s3tables.TablesPath), setup.BucketName, &filer_pb.Entry{
		Name: setup.BucketName, IsDirectory: true,
		Extended: map[string][]byte{s3tables.ExtendedKeyTableBucket: []byte("true")},
	})
	fs.putEntry(bucketPath, setup.Namespace, &filer_pb.Entry{Name: setup.Namespace, IsDirectory: true})
	fs.putEntry(nsPath, setup.TableName, &filer_pb.Entry{
		Name: setup.TableName, IsDirectory: true,
		Extended: map[string][]byte{s3tables.ExtendedKeyMetadata: xattr},
	})
}

// A bin's files are merged in the order of their bounds on the
// ordering column, not manifest order, so a merged file is as ordered
// as the sequence of its inputs.
func TestCompactDataFilesKeepsInputOrderByBounds(t *testing.T) {
	fs, client := startFakeFiler(t)
	setup := tableSetup{BucketName: "tb", Namespace: "ns", TableName: "tbl"}
	// manifest order is the reverse of bound order
	populateBoundedTable(t, fs, setup, []boundedFile{
		{Name: "hi.parquet", IDs: []int64{3, 4}, Lower: 3, Upper: 4},
		{Name: "lo.parquet", IDs: []int64{1, 2}, Lower: 1, Upper: 2},
	})

	handler := NewHandler(nil)
	config := Config{TargetFileSizeBytes: 256 * 1024 * 1024, MinInputFiles: 2, MaxCommitRetries: 3}
	if _, _, err := handler.compactDataFiles(context.Background(), client, setup.BucketName, setup.tablePath(), config, nil); err != nil {
		t.Fatalf("compactDataFiles: %v", err)
	}

	df := compactedDataFile(t, client, setup)
	ids := compactedIDs(t, client, setup, df.FilePath())
	want := []int64{1, 2, 3, 4}
	if fmt.Sprint(ids) != fmt.Sprint(want) {
		t.Errorf("merged rows = %v, want %v", ids, want)
	}
}

// An oversized partition is split into runs of consecutive files in
// bound order, so each output covers one contiguous range.
func TestCompactDataFilesSplitsOrderedRuns(t *testing.T) {
	fs, client := startFakeFiler(t)
	setup := tableSetup{BucketName: "tb", Namespace: "ns", TableName: "tbl"}
	// four disjoint ranges, each "4 MB": one bin, split into two runs of two
	populateBoundedTable(t, fs, setup, []boundedFile{
		{Name: "q4.parquet", IDs: []int64{7, 8}, Lower: 7, Upper: 8, SizeBytes: 4 << 20},
		{Name: "q1.parquet", IDs: []int64{1, 2}, Lower: 1, Upper: 2, SizeBytes: 4 << 20},
		{Name: "q3.parquet", IDs: []int64{5, 6}, Lower: 5, Upper: 6, SizeBytes: 4 << 20},
		{Name: "q2.parquet", IDs: []int64{3, 4}, Lower: 3, Upper: 4, SizeBytes: 4 << 20},
	})

	handler := NewHandler(nil)
	config := Config{TargetFileSizeBytes: 8 << 20, MinInputFiles: 2, MaxCommitRetries: 3}
	result, _, err := handler.compactDataFiles(context.Background(), client, setup.BucketName, setup.tablePath(), config, nil)
	if err != nil {
		t.Fatalf("compactDataFiles: %v", err)
	}
	if !strings.Contains(result, "compacted 4 files into 2") {
		t.Fatalf("expected two ordered runs, got %q", result)
	}

	// each output must cover one contiguous range
	state, err := loadCurrentMetadata(context.Background(), client, setup.BucketName, setup.tablePath())
	if err != nil {
		t.Fatalf("loadCurrentMetadata: %v", err)
	}
	manifests, err := loadCurrentManifests(context.Background(), client, setup.BucketName, state.DataPath, state.Metadata)
	if err != nil {
		t.Fatalf("loadCurrentManifests: %v", err)
	}
	var ranges [][2]int64
	for _, mf := range manifests {
		if mf.ManifestContent() != iceberg.ManifestContentData {
			continue
		}
		manifestData, err := loadFileByIcebergPath(context.Background(), client, setup.BucketName, state.DataPath, mf.FilePath())
		if err != nil {
			t.Fatalf("load manifest: %v", err)
		}
		entries, err := iceberg.ReadManifest(mf, bytes.NewReader(manifestData), true)
		if err != nil {
			t.Fatalf("read manifest: %v", err)
		}
		for _, entry := range entries {
			if entry.Status() == iceberg.EntryStatusDELETED {
				continue
			}
			lo, _ := iceberg.LiteralFromBytes(iceberg.PrimitiveTypes.Int64, entry.DataFile().LowerBoundValues()[1])
			hi, _ := iceberg.LiteralFromBytes(iceberg.PrimitiveTypes.Int64, entry.DataFile().UpperBoundValues()[1])
			ranges = append(ranges, [2]int64{int64(lo.(iceberg.Int64Literal)), int64(hi.(iceberg.Int64Literal))})
		}
	}
	if len(ranges) != 2 {
		t.Fatalf("expected 2 output files, got %d", len(ranges))
	}
	// ranges must be contiguous runs, not interleaved: {1..4} and {5..8}
	for _, r := range ranges {
		if r[1]-r[0] != 3 {
			t.Errorf("output covers non-contiguous range %v", r)
		}
	}
}

// An oversized file without bounds can never join a merge, so it must not
// disable bounds ordering for the files that can.
func TestCompactDataFilesOrdersPastIneligibleFile(t *testing.T) {
	fs, client := startFakeFiler(t)
	setup := tableSetup{BucketName: "tb", Namespace: "ns", TableName: "tbl"}
	populateBoundedTable(t, fs, setup, []boundedFile{
		{Name: "hi.parquet", IDs: []int64{3, 4}, Lower: 3, Upper: 4},
		{Name: "lo.parquet", IDs: []int64{1, 2}, Lower: 1, Upper: 2},
		{Name: "huge.parquet", IDs: []int64{9}, SizeBytes: 512 << 20, NoBounds: true},
	})

	handler := NewHandler(nil)
	config := Config{TargetFileSizeBytes: 256 << 20, MinInputFiles: 2, MaxCommitRetries: 3}
	result, _, err := handler.compactDataFiles(context.Background(), client, setup.BucketName, setup.tablePath(), config, nil)
	if err != nil {
		t.Fatalf("compactDataFiles: %v", err)
	}
	if !strings.Contains(result, "compacted 2 files into 1") {
		t.Fatalf("expected only the two eligible files to merge, got %q", result)
	}

	var merged iceberg.DataFile
	for _, df := range liveDataFiles(t, client, setup) {
		if !strings.HasSuffix(df.FilePath(), "huge.parquet") {
			merged = df
		}
	}
	ids := compactedIDs(t, client, setup, merged.FilePath())
	want := []int64{1, 2, 3, 4}
	if fmt.Sprint(ids) != fmt.Sprint(want) {
		t.Errorf("merged rows = %v, want %v", ids, want)
	}
}

// Runs too short to merge in bound order still merge by size, so files that
// cannot form a contiguous run are not stranded for every later pass.
func TestCompactDataFilesMergesStrandedRun(t *testing.T) {
	fs, client := startFakeFiler(t)
	setup := tableSetup{BucketName: "tb", Namespace: "ns", TableName: "tbl"}
	populateBoundedTable(t, fs, setup, []boundedFile{
		{Name: "r1.parquet", IDs: []int64{1}, Lower: 1, Upper: 2, SizeBytes: 4 << 20},
		{Name: "r2.parquet", IDs: []int64{3}, Lower: 3, Upper: 4, SizeBytes: 4 << 20},
		{Name: "r3.parquet", IDs: []int64{5}, Lower: 5, Upper: 6, SizeBytes: 6 << 20},
		{Name: "r4.parquet", IDs: []int64{7}, Lower: 7, Upper: 8, SizeBytes: 3 << 20},
		{Name: "r5.parquet", IDs: []int64{9}, Lower: 9, Upper: 10, SizeBytes: 3 << 20},
	})

	handler := NewHandler(nil)
	config := Config{TargetFileSizeBytes: 10 << 20, MinInputFiles: 3, MaxCommitRetries: 3}
	result, _, err := handler.compactDataFiles(context.Background(), client, setup.BucketName, setup.tablePath(), config, nil)
	if err != nil {
		t.Fatalf("compactDataFiles: %v", err)
	}
	if !strings.Contains(result, "compacted 3 files into 1") {
		t.Fatalf("expected stranded files to merge by size, got %q", result)
	}
}

// A declared sort order is preserved at the cost of compaction progress:
// runs too short to merge are left for later passes instead of being
// repacked out of order.
func TestCompactDataFilesKeepsStrandedRunForSortedTable(t *testing.T) {
	fs, client := startFakeFiler(t)
	setup := tableSetup{BucketName: "tb", Namespace: "ns", TableName: "tbl"}
	sortOrder, err := table.NewSortOrder(1, []table.SortField{{
		SourceIDs: []int{1},
		Transform: iceberg.IdentityTransform{},
		Direction: table.SortASC,
		NullOrder: table.NullsFirst,
	}})
	if err != nil {
		t.Fatalf("new sort order: %v", err)
	}
	populateBoundedTableSorted(t, fs, setup, []boundedFile{
		{Name: "r1.parquet", IDs: []int64{1}, Lower: 1, Upper: 2, SizeBytes: 4 << 20},
		{Name: "r2.parquet", IDs: []int64{3}, Lower: 3, Upper: 4, SizeBytes: 4 << 20},
		{Name: "r3.parquet", IDs: []int64{5}, Lower: 5, Upper: 6, SizeBytes: 6 << 20},
		{Name: "r4.parquet", IDs: []int64{7}, Lower: 7, Upper: 8, SizeBytes: 3 << 20},
		{Name: "r5.parquet", IDs: []int64{9}, Lower: 9, Upper: 10, SizeBytes: 3 << 20},
	}, sortOrder)

	handler := NewHandler(nil)
	config := Config{TargetFileSizeBytes: 10 << 20, MinInputFiles: 3, MaxCommitRetries: 3}
	result, _, err := handler.compactDataFiles(context.Background(), client, setup.BucketName, setup.tablePath(), config, nil)
	if err != nil {
		t.Fatalf("compactDataFiles: %v", err)
	}
	if !strings.Contains(result, "no files eligible for compaction") {
		t.Fatalf("expected underfilled ordered runs to be kept, got %q", result)
	}
}
