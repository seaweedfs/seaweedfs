package iceberg

import (
	"bytes"
	"testing"

	"github.com/parquet-go/parquet-go"
)

type lineageRow struct {
	ID    int64  `parquet:"id"`
	RowID *int64 `parquet:"_row_id,optional"`
}

func lineageFile(t *testing.T, ids ...*int64) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[lineageRow](&buf)
	rows := make([]lineageRow, len(ids))
	for i, id := range ids {
		rows[i] = lineageRow{ID: int64(i), RowID: id}
	}
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func i64(v int64) *int64 { return &v }

// A merged v3 file names its first row's id as first_row_id only when every
// row carries a materialized id: a null row would inherit first_row_id plus
// its position, an id another row already holds (high review of the 4.49
// series; 4.49's own code took the first row's id whatever followed).
func TestMergedFileFirstRowIDOnlyWithoutNullIDs(t *testing.T) {
	cases := []struct {
		name string
		ids  []*int64
		want *int64
	}{
		{"every row materialized", []*int64{i64(40), i64(41), i64(42)}, i64(40)},
		{"materialized first, a null later", []*int64{i64(40), nil, i64(42)}, nil},
		{"null first", []*int64{nil, i64(41)}, nil},
		{"all null", []*int64{nil, nil}, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := mergedFileFirstRowID(lineageFile(t, c.ids...))
			if err != nil {
				t.Fatal(err)
			}
			switch {
			case c.want == nil && got != nil:
				t.Fatalf("first_row_id %d, want none", *got)
			case c.want != nil && (got == nil || *got != *c.want):
				t.Fatalf("first_row_id %v, want %d", got, *c.want)
			}
		})
	}
}
