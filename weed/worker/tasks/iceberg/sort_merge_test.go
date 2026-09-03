package iceberg

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

// seedSortableTable creates a table sorted ascending on id whose data files are
// deliberately out of order: ids interleave across the files and descend within
// each one, so neither the file order nor the row order is already sorted.
func seedSortableTable(t *testing.T, files, rowsPerFile int) (*fakeFilerServer, filer_pb.SeaweedFilerClient, tableSetup) {
	t.Helper()

	fs, client := startFakeFiler(t)
	sortOrder, err := table.NewSortOrder(1, []table.SortField{{
		SourceIDs: []int{1},
		Transform: iceberg.IdentityTransform{},
		Direction: table.SortASC,
		NullOrder: table.NullsFirst,
	}})
	if err != nil {
		t.Fatalf("new sort order: %v", err)
	}

	type dataRow = struct {
		ID   int64
		Name string
	}
	dataFiles := make([]struct {
		Name string
		Rows []dataRow
	}, 0, files)
	for f := 0; f < files; f++ {
		rows := make([]dataRow, 0, rowsPerFile)
		for i := 0; i < rowsPerFile; i++ {
			id := int64((rowsPerFile-1-i)*files + f)
			rows = append(rows, dataRow{ID: id, Name: fmt.Sprintf("row-%d", id)})
		}
		dataFiles = append(dataFiles, struct {
			Name string
			Rows []dataRow
		}{Name: fmt.Sprintf("d%d.parquet", f), Rows: rows})
	}

	setup := tableSetup{BucketName: "tb", Namespace: "ns", TableName: "tbl"}
	populateTableWithDeleteFilesAndSortOrder(t, fs, setup, dataFiles, nil, nil, sortOrder)
	return fs, client, setup
}

func sortCompactionConfig(spillDir string) Config {
	return Config{
		TargetFileSizeBytes: 256 * 1024 * 1024,
		MinInputFiles:       2,
		MaxCommitRetries:    3,
		ApplyDeletes:        true,
		RewriteStrategy:     "sort",
		SortBufferRows:      minSortBufferRows,
		SortSpillDir:        spillDir,
	}
}

// compactedFileNames lists the outputs a compaction wrote, which is empty when
// every bin was skipped.
func compactedFileNames(fs *fakeFilerServer, setup tableSetup) []string {
	dataDir := path.Join(s3tables.TablesPath, setup.BucketName, setup.tablePath(), "data")
	var names []string
	for _, e := range fs.listDir(dataDir) {
		if strings.HasPrefix(e.Name, "compact-") {
			names = append(names, e.Name)
		}
	}
	return names
}

// A sorted rewrite whose bin does not fit in one buffer has to spill: each
// buffer is encoded as a sorted run and the runs are merged at close. This is
// the case that used to hold every row of the bin in memory, so it is worth
// proving both that the output is globally ordered and that the runs do not
// outlive the job.
func TestCompactDataFilesSortSpillsRunsAndCleansUp(t *testing.T) {
	const files, rowsPerFile = 3, 1200 // 3600 rows against a 1024-row buffer
	fs, client, setup := seedSortableTable(t, files, rowsPerFile)

	spillDir := t.TempDir()
	handler := NewHandler(nil)
	result, _, err := handler.compactDataFiles(context.Background(), client, setup.BucketName, setup.tablePath(), sortCompactionConfig(spillDir), nil)
	if err != nil {
		t.Fatalf("compactDataFiles: %v", err)
	}
	if !strings.Contains(result, "using sort") {
		t.Fatalf("expected sorted compaction result, got %q", result)
	}

	rows := readCompactedRows(t, fs, setup)
	if len(rows) != files*rowsPerFile {
		t.Fatalf("expected %d compacted rows, got %d", files*rowsPerFile, len(rows))
	}
	for i := 1; i < len(rows); i++ {
		if rows[i-1].ID > rows[i].ID {
			t.Fatalf("rows are not sorted by id at %d: %d then %d", i, rows[i-1].ID, rows[i].ID)
		}
	}

	leftovers, err := os.ReadDir(spillDir)
	if err != nil {
		t.Fatalf("read spill dir: %v", err)
	}
	if len(leftovers) != 0 {
		t.Fatalf("expected the sorted runs to be removed, found %d file(s)", len(leftovers))
	}
}

// An empty spill directory at the end of a job cannot tell a run written to
// disk from one held in memory. A directory that cannot hold the runs can: the
// same data compacts with a usable one and does not with an unusable one, which
// it would not do if the runs never reached the pool. A bin whose merge fails
// is logged and skipped rather than failing the job, so the difference shows up
// as a missing output file rather than an error.
func TestCompactDataFilesSortSpillDirDecidesOutcome(t *testing.T) {
	// Two files that together cross the 1024-row buffer, so a run is spilled.
	const files, rowsPerFile = 2, 700

	for _, tc := range []struct {
		name         string
		spillDir     func(t *testing.T) string
		wantOutcomes int
	}{
		{
			name:         "usable",
			spillDir:     func(t *testing.T) string { return t.TempDir() },
			wantOutcomes: 1,
		},
		{
			name:         "unusable",
			spillDir:     func(t *testing.T) string { return filepath.Join(t.TempDir(), "not-created") },
			wantOutcomes: 0,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs, client, setup := seedSortableTable(t, files, rowsPerFile)
			handler := NewHandler(nil)

			if _, _, err := handler.compactDataFiles(context.Background(), client, setup.BucketName, setup.tablePath(), sortCompactionConfig(tc.spillDir(t)), nil); err != nil {
				t.Fatalf("compactDataFiles: %v", err)
			}
			if names := compactedFileNames(fs, setup); len(names) != tc.wantOutcomes {
				t.Fatalf("expected %d compacted file(s), got %v", tc.wantOutcomes, names)
			}
		})
	}
}
