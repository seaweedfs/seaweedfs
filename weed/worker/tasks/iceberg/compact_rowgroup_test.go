package iceberg

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/parquet-go/parquet-go"
)

// Compacted files get row groups from the table's
// write.parquet.row-group-limit instead of one row group per file.
func TestCompactDataFilesWritesSeveralRowGroups(t *testing.T) {
	fs, client := startFakeFiler(t)
	setup := tableSetup{BucketName: "tb", Namespace: "ns", TableName: "tbl"}

	type rowsT = struct {
		ID   int64
		Name string
	}
	makeRows := func(lo, hi int64) []rowsT {
		rows := make([]rowsT, 0, hi-lo)
		for i := lo; i < hi; i++ {
			rows = append(rows, rowsT{i, fmt.Sprintf("r%d", i)})
		}
		return rows
	}
	populateTableWithDeleteFiles(t, fs, setup,
		[]struct {
			Name string
			Rows []rowsT
		}{
			{"d1.parquet", makeRows(0, 1500)},
			{"d2.parquet", makeRows(1500, 3000)},
		},
		nil, nil,
	)

	handler := NewHandler(nil)
	config := Config{
		TargetFileSizeBytes: 256 * 1024 * 1024,
		MinInputFiles:       2,
		MaxCommitRetries:    3,
		RowGroupRowLimit:    2000,
	}
	if _, _, err := handler.compactDataFiles(context.Background(), client, setup.BucketName, setup.tablePath(), config, nil); err != nil {
		t.Fatalf("compactDataFiles: %v", err)
	}

	state, err := loadCurrentMetadata(context.Background(), client, setup.BucketName, setup.tablePath())
	if err != nil {
		t.Fatalf("loadCurrentMetadata: %v", err)
	}
	df := compactedDataFile(t, client, setup)
	data, err := loadFileByIcebergPath(context.Background(), client, setup.BucketName, state.DataPath, df.FilePath())
	if err != nil {
		t.Fatalf("load compacted file: %v", err)
	}
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("open compacted file: %v", err)
	}
	if got := len(f.Metadata().RowGroups); got != 2 {
		t.Errorf("row groups = %d, want 2 (3000 rows at row-group-limit 2000)", got)
	}
}

// A configured row-group limit is a cap, not a floor: small explicit limits
// are honored instead of being raised to the estimate floor.
func TestRowsPerRowGroupHonorsConfiguredLimit(t *testing.T) {
	entries := make([]iceberg.ManifestEntry, 0, 2)
	spec := *iceberg.UnpartitionedSpec
	for _, name := range []string{"a.parquet", "b.parquet"} {
		dfb, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentData,
			"s3://b/data/"+name, iceberg.ParquetFile, map[int]any{}, nil, nil, 300, 9000)
		if err != nil {
			t.Fatalf("build data file: %v", err)
		}
		snapID := int64(1)
		entries = append(entries, iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapID, nil, nil, dfb.Build()))
	}
	bin := compactionBin{Entries: entries, TotalSize: 18000}

	if got := rowsPerRowGroup(bin, Config{RowGroupRowLimit: 128}); got != 128 {
		t.Errorf("explicit limit: rowsPerRowGroup = %d, want 128", got)
	}
	// 30 bytes/row, byte cap 128MB: the estimate of ~4.4M rows exceeds the 2M
	// default, so the default holds; the floor lifts a degenerate estimate.
	if got := rowsPerRowGroup(bin, Config{RowGroupSizeBytes: 1024}); got != minRowGroupRows {
		t.Errorf("floored estimate: rowsPerRowGroup = %d, want %d", got, minRowGroupRows)
	}
	if got := rowsPerRowGroup(bin, Config{}); got != defaultRowGroupRowLimit {
		t.Errorf("default: rowsPerRowGroup = %d, want %d", got, defaultRowGroupRowLimit)
	}
}
