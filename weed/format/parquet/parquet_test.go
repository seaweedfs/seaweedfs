package parquet

import (
	"bytes"
	"context"
	"testing"

	parquetgo "github.com/parquet-go/parquet-go"
	"github.com/seaweedfs/seaweedfs/weed/format"
	"github.com/seaweedfs/seaweedfs/weed/format/formattest"
)

type row struct {
	ID   int64  `parquet:"id"`
	Name string `parquet:"name"`
}

// buildParquet writes rowGroups row groups of rowsPerGroup rows each.
func buildParquet(t *testing.T, rowGroups, rowsPerGroup int) []byte {
	t.Helper()
	var buf bytes.Buffer
	writer := parquetgo.NewGenericWriter[row](&buf)
	for g := 0; g < rowGroups; g++ {
		rows := make([]row, rowsPerGroup)
		for i := range rows {
			rows[i] = row{ID: int64(g*rowsPerGroup + i), Name: "some filler content to give row groups a little size"}
		}
		if _, err := writer.Write(rows); err != nil {
			t.Fatalf("Write() error = %v", err)
		}
		if err := writer.Flush(); err != nil {
			t.Fatalf("Flush() error = %v", err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	return buf.Bytes()
}

func TestIndex(t *testing.T) {
	data := buildParquet(t, 3, 100)
	size := int64(len(data))
	layout, err := Adapter{}.Index(context.Background(), bytes.NewReader(data), size)
	if err != nil {
		t.Fatalf("Index() error = %v", err)
	}
	// one extent per row group plus the trailing footer extent
	if len(layout.ExtentSizes) != 4 {
		t.Fatalf("extents = %v, want 4", layout.ExtentSizes)
	}
	if err := layout.Validate(size); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	formattest.EncodeRoundTrip(t, layout)

	// extent cuts must land on the row-group starts the footer declares
	file, err := parquetgo.OpenFile(bytes.NewReader(data), size)
	if err != nil {
		t.Fatalf("OpenFile() error = %v", err)
	}
	var offset int64
	for i, extentSize := range layout.ExtentSizes[:3] {
		if i > 0 {
			want := file.Metadata().RowGroups[i].Columns[0].MetaData.DataPageOffset
			if dictionary := file.Metadata().RowGroups[i].Columns[0].MetaData.DictionaryPageOffset; dictionary > 0 && dictionary < want {
				want = dictionary
			}
			if offset != want {
				t.Fatalf("extent %d starts at %d, row group starts at %d", i, offset, want)
			}
		}
		offset += extentSize
	}
}

func TestIndexSingleRowGroup(t *testing.T) {
	data := buildParquet(t, 1, 10)
	layout, err := Adapter{}.Index(context.Background(), bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("Index() error = %v", err)
	}
	if len(layout.ExtentSizes) != 2 {
		t.Fatalf("extents = %v, want 2", layout.ExtentSizes)
	}
}

func TestIndexRejectsNonParquet(t *testing.T) {
	data := []byte("this is not a parquet file, not even close, but long enough")
	if _, err := (Adapter{}).Index(context.Background(), bytes.NewReader(data), int64(len(data))); err == nil {
		t.Fatalf("Index() accepted junk")
	}
}

func TestIndexTruncations(t *testing.T) {
	formattest.IndexTruncations(t, Adapter{}, buildParquet(t, 2, 50))
}

func TestSniff(t *testing.T) {
	data := buildParquet(t, 1, 10)
	head, tail := data[:4], data[len(data)-4:]
	if !(Adapter{}).Sniff(format.Hint{Head: head, Tail: tail}) {
		t.Fatalf("Sniff() rejected parquet magic")
	}
	if (Adapter{}).Sniff(format.Hint{Head: []byte("PAR1"), Tail: []byte("nope")}) {
		t.Fatalf("Sniff() accepted missing tail magic")
	}
}
