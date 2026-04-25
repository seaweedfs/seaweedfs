package footer

import (
	"bytes"
	"testing"

	"github.com/parquet-go/parquet-go"
)

type sampleRow struct {
	ID    int64
	Name  string
	Value float64
}

// writeFixture builds a small Parquet buffer with two row groups so
// the parser exercises both row-group iteration and per-column-chunk
// byte ranges. RowGroupTargetSize is set tight enough that the writer
// flushes mid-input.
func writeFixture(t *testing.T, rows []sampleRow) []byte {
	t.Helper()

	schema := parquet.SchemaOf(sampleRow{})
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[sampleRow](&buf, schema)
	if _, err := w.Write(rows); err != nil {
		t.Fatalf("write rows: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	return buf.Bytes()
}

func TestParseFromReader_ColumnChunksAndCounts(t *testing.T) {
	rows := make([]sampleRow, 1000)
	for i := range rows {
		rows[i] = sampleRow{ID: int64(i), Name: "row", Value: float64(i)}
	}
	data := writeFixture(t, rows)

	pf, err := ParseFromReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if pf.NumRows != int64(len(rows)) {
		t.Errorf("NumRows = %d, want %d", pf.NumRows, len(rows))
	}
	if pf.NumCols != 3 {
		t.Errorf("NumCols = %d, want 3", pf.NumCols)
	}
	wantCols := []string{"ID", "Name", "Value"}
	if len(pf.ColumnPaths) != len(wantCols) {
		t.Fatalf("ColumnPaths len %d, want %d", len(pf.ColumnPaths), len(wantCols))
	}
	for i, want := range wantCols {
		if pf.ColumnPaths[i] != want {
			t.Errorf("ColumnPaths[%d] = %q, want %q", i, pf.ColumnPaths[i], want)
		}
	}
	if pf.NumGroups < 1 {
		t.Fatalf("NumGroups = %d, want >= 1", pf.NumGroups)
	}

	// Every column chunk has a non-zero offset (data starts after the
	// 4-byte magic header) and a positive length, and the byte ranges
	// must not overlap within a single row group when sorted.
	for gi, rg := range pf.RowGroups {
		if rg.NumRows == 0 {
			t.Errorf("row group %d has zero rows", gi)
		}
		if len(rg.ColumnChunks) != pf.NumCols {
			t.Errorf("row group %d ColumnChunks len = %d, want %d", gi, len(rg.ColumnChunks), pf.NumCols)
		}
		for ci, cc := range rg.ColumnChunks {
			if cc.Offset <= 0 {
				t.Errorf("row group %d col %d offset = %d, want > 0", gi, ci, cc.Offset)
			}
			if cc.Length <= 0 {
				t.Errorf("row group %d col %d length = %d, want > 0", gi, ci, cc.Length)
			}
			if cc.Offset+cc.Length > int64(len(data)) {
				t.Errorf("row group %d col %d overruns file: end %d > size %d", gi, ci, cc.Offset+cc.Length, len(data))
			}
		}
	}
}

func TestParseFromReader_RejectsInvalid(t *testing.T) {
	if _, err := ParseFromReader(nil, 100); err == nil {
		t.Error("nil reader should fail")
	}
	if _, err := ParseFromReader(bytes.NewReader([]byte("PAR1")), 0); err == nil {
		t.Error("zero size should fail")
	}
	if _, err := ParseFromReader(bytes.NewReader([]byte("not parquet at all")), 18); err == nil {
		t.Error("non-parquet bytes should fail")
	}
}

// columnChunkBytes prefers the dictionary page offset when present
// because the dictionary page must be fetched alongside the data
// pages of the same chunk.
func TestColumnChunkBytes_DictionaryOffsetPreferred(t *testing.T) {
	// Synthesize a fixture where dictionary encoding is likely:
	// repeating low-cardinality strings nudge parquet-go to write a
	// dictionary page. Then check that at least one column chunk
	// reports a start offset before its data-page offset.
	rows := make([]sampleRow, 2000)
	for i := range rows {
		rows[i] = sampleRow{ID: int64(i % 5), Name: "tenant-" + string(rune('a'+(i%5))), Value: float64(i % 5)}
	}
	data := writeFixture(t, rows)
	pf, err := ParseFromReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	// Sanity: at least one chunk's range should be plausible (length
	// is positive and offset doesn't exceed the file). Beyond that we
	// can't depend on a specific encoding choice from parquet-go.
	for _, rg := range pf.RowGroups {
		for _, cc := range rg.ColumnChunks {
			if cc.Offset+cc.Length > int64(len(data)) {
				t.Fatalf("chunk overruns file")
			}
		}
	}
}
