package iceberg

import (
	"bytes"
	"context"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/parquet-go/parquet-go"

	filer_pb "github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// compactedDataFile returns the data file the current snapshot's data
// manifest references after a compaction run.
func compactedDataFile(t *testing.T, client filer_pb.SeaweedFilerClient, setup tableSetup) iceberg.DataFile {
	t.Helper()
	state, err := loadCurrentMetadata(context.Background(), client, setup.BucketName, setup.tablePath())
	if err != nil {
		t.Fatalf("loadCurrentMetadata: %v", err)
	}
	manifests, err := loadCurrentManifests(context.Background(), client, setup.BucketName, state.DataPath, state.Metadata)
	if err != nil {
		t.Fatalf("loadCurrentManifests: %v", err)
	}
	var found []iceberg.DataFile
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
			if entry.Status() != iceberg.EntryStatusDELETED {
				found = append(found, entry.DataFile())
			}
		}
	}
	if len(found) != 1 {
		t.Fatalf("expected 1 live data file after compaction, got %d", len(found))
	}
	return found[0]
}

func compactedFileBytes(t *testing.T, client filer_pb.SeaweedFilerClient, setup tableSetup, filePath string) []byte {
	t.Helper()
	state, err := loadCurrentMetadata(context.Background(), client, setup.BucketName, setup.tablePath())
	if err != nil {
		t.Fatalf("loadCurrentMetadata: %v", err)
	}
	data, err := loadFileByIcebergPath(context.Background(), client, setup.BucketName, state.DataPath, filePath)
	if err != nil {
		t.Fatalf("load compacted file: %v", err)
	}
	return data
}

func compactedIDs(t *testing.T, client filer_pb.SeaweedFilerClient, setup tableSetup, filePath string) []int64 {
	t.Helper()
	data := compactedFileBytes(t, client, setup, filePath)
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("open compacted file: %v", err)
	}
	var ids []int64
	for _, rg := range f.RowGroups() {
		rows := rg.Rows()
		buf := make([]parquet.Row, 8)
		for {
			n, err := rows.ReadRows(buf)
			for _, row := range buf[:n] {
				ids = append(ids, row[0].Int64())
			}
			if err != nil {
				break
			}
		}
		rows.Close()
	}
	return ids
}

// A compacted file's manifest entry carries the column metrics readers
// prune files by: counts, sizes, bounds and split offsets read back
// from the file's own footer.
func TestCompactDataFilesRecordsColumnStatistics(t *testing.T) {
	fs, client := startFakeFiler(t)
	setup := tableSetup{BucketName: "tb", Namespace: "ns", TableName: "tbl"}
	populateTableWithDeleteFiles(t, fs, setup,
		[]struct {
			Name string
			Rows []struct {
				ID   int64
				Name string
			}
		}{
			{"d1.parquet", []struct {
				ID   int64
				Name string
			}{{1, "a"}, {2, "b"}}},
			{"d2.parquet", []struct {
				ID   int64
				Name string
			}{{3, "c"}}},
		},
		nil, nil,
	)

	handler := NewHandler(nil)
	config := Config{TargetFileSizeBytes: 256 * 1024 * 1024, MinInputFiles: 2, MaxCommitRetries: 3}
	if _, _, err := handler.compactDataFiles(context.Background(), client, setup.BucketName, setup.tablePath(), config, nil); err != nil {
		t.Fatalf("compactDataFiles: %v", err)
	}

	df := compactedDataFile(t, client, setup)
	if df.ValueCounts()[1] != 3 {
		t.Errorf("value_counts[1] = %d, want 3", df.ValueCounts()[1])
	}
	if len(df.LowerBoundValues()) == 0 || len(df.UpperBoundValues()) == 0 {
		t.Fatal("compacted file has no bounds")
	}
	lo, err := iceberg.LiteralFromBytes(iceberg.PrimitiveTypes.Int64, df.LowerBoundValues()[1])
	if err != nil {
		t.Fatalf("decode lower bound: %v", err)
	}
	hi, err := iceberg.LiteralFromBytes(iceberg.PrimitiveTypes.Int64, df.UpperBoundValues()[1])
	if err != nil {
		t.Fatalf("decode upper bound: %v", err)
	}
	if lo.(iceberg.Int64Literal) != 1 || hi.(iceberg.Int64Literal) != 3 {
		t.Errorf("id bounds = %v..%v, want 1..3", lo, hi)
	}
	if len(df.SplitOffsets()) == 0 {
		t.Error("compacted file has no split_offsets")
	}
}
