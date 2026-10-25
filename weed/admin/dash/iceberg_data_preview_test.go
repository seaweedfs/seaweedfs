package dash

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
)

func TestIcebergLocationToFilerPath(t *testing.T) {
	tests := []struct {
		location string
		want     string
		wantErr  bool
	}{
		{location: "s3://warehouse/ns/tbl/data/f1.parquet", want: "/buckets/warehouse/ns/tbl/data/f1.parquet"},
		{location: "s3a://warehouse/ns/tbl/metadata/snap-1.avro", want: "/buckets/warehouse/ns/tbl/metadata/snap-1.avro"},
		{location: "/buckets/warehouse/ns/tbl/data/f1.parquet", want: "/buckets/warehouse/ns/tbl/data/f1.parquet"},
		{location: "data/f1.parquet", want: "/buckets/warehouse/ns/tbl/data/f1.parquet"},
		{location: "metadata/v3.metadata.json", want: "/buckets/warehouse/ns/tbl/metadata/v3.metadata.json"},
		{location: "s3://warehouse/../../etc/passwd", wantErr: true},
		{location: "/etc/passwd", wantErr: true},
		{location: "../../../etc/passwd", wantErr: true},
	}
	for _, tc := range tests {
		got, err := icebergLocationToFilerPath(tc.location, "warehouse", "ns", "tbl")
		if tc.wantErr {
			if err == nil {
				t.Errorf("icebergLocationToFilerPath(%q) = %q, want error", tc.location, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("icebergLocationToFilerPath(%q): %v", tc.location, err)
			continue
		}
		if got != tc.want {
			t.Errorf("icebergLocationToFilerPath(%q) = %q, want %q", tc.location, got, tc.want)
		}
	}
}

func TestFormatParquetRow(t *testing.T) {
	type record struct {
		ID    int64   `parquet:"id"`
		Name  string  `parquet:"name"`
		Score float64 `parquet:"score"`
		Blob  []byte  `parquet:"blob"`
		Note  *string `parquet:"note,optional"`
	}

	var buf bytes.Buffer
	w := parquet.NewGenericWriter[record](&buf)
	note := "hello"
	if _, err := w.Write([]record{
		{ID: 1, Name: "alpha", Score: 1.5, Blob: []byte{0xff, 0xfe}, Note: &note},
		{ID: 2, Name: "beta", Score: -2.25, Blob: []byte("text"), Note: nil},
	}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	pf, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatal(err)
	}
	leafColumns := pf.Schema().Columns()
	cols := make([]string, len(leafColumns))
	for i, c := range leafColumns {
		cols[i] = strings.Join(c, ".")
	}

	reader := parquet.NewReader(pf)
	defer reader.Close()
	rows := make([]parquet.Row, 4)
	n, err := reader.ReadRows(rows)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("got %d rows, want 2", n)
	}

	byName := func(cells []string, name string) string {
		for i, col := range cols {
			if col == name {
				return cells[i]
			}
		}
		t.Fatalf("column %q not found in %v", name, cols)
		return ""
	}

	first := formatParquetRow(rows[0], len(cols))
	if got := byName(first, "id"); got != "1" {
		t.Errorf("id = %q, want 1", got)
	}
	if got := byName(first, "name"); got != "alpha" {
		t.Errorf("name = %q, want alpha", got)
	}
	if got := byName(first, "score"); got != "1.5" {
		t.Errorf("score = %q, want 1.5", got)
	}
	if got := byName(first, "blob"); got != "0xfffe" {
		t.Errorf("blob = %q, want 0xfffe", got)
	}
	if got := byName(first, "note"); got != "hello" {
		t.Errorf("note = %q, want hello", got)
	}

	second := formatParquetRow(rows[1], len(cols))
	if got := byName(second, "blob"); got != "text" {
		t.Errorf("blob = %q, want text", got)
	}
	if got := byName(second, "note"); got != "" {
		t.Errorf("note = %q, want empty for null", got)
	}
}

func TestTruncateCell(t *testing.T) {
	long := strings.Repeat("界", 200)
	got := truncateCell(long)
	if len(got) > icebergPreviewMaxCellChars+len("…") {
		t.Errorf("truncated cell too long: %d", len(got))
	}
	if !strings.HasSuffix(got, "…") {
		t.Errorf("truncated cell should end with ellipsis")
	}
	if short := truncateCell("abc"); short != "abc" {
		t.Errorf("short cell changed: %q", short)
	}
}
