package iceberg

import (
	"bytes"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
)

// The position-delete file must carry the spec's field ids and a
// dictionary-encoded file_path: PyIceberg reads delete files with
// file_path as a dictionary column, which PyArrow cannot decode from
// DELTA_LENGTH_BYTE_ARRAY, and by-id readers resolve columns by field id.
func TestPositionDeleteFileHasFieldIDsAndDictionaryPaths(t *testing.T) {
	rows := []positionDeleteRow{
		{FilePath: "s3://tb/ns/tbl/data/a.parquet", Pos: 0},
		{FilePath: "s3://tb/ns/tbl/data/a.parquet", Pos: 7},
		{FilePath: "s3://tb/ns/tbl/data/b.parquet", Pos: 3},
	}
	data, err := writePositionDeleteFile(rows)
	if err != nil {
		t.Fatal(err)
	}
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]int{"file_path": 2147483546, "pos": 2147483545}
	for _, field := range f.Schema().Fields() {
		if id, ok := want[field.Name()]; !ok || field.ID() != id {
			t.Errorf("field %s id = %d, want %d", field.Name(), field.ID(), id)
		}
	}
	for _, rg := range f.Metadata().RowGroups {
		for _, col := range rg.Columns {
			if col.MetaData.PathInSchema[0] != "file_path" {
				continue
			}
			dict := false
			for _, enc := range col.MetaData.Encoding {
				if enc == format.DeltaLengthByteArray {
					t.Errorf("file_path is DELTA_LENGTH_BYTE_ARRAY: %v", col.MetaData.Encoding)
				}
				if enc == format.RLEDictionary || enc == format.PlainDictionary {
					dict = true
				}
			}
			if !dict {
				t.Errorf("file_path is not dictionary-encoded: %v", col.MetaData.Encoding)
			}
		}
	}
}
