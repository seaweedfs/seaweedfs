package iceberg

import (
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

// A Lance dataset registered through the Lance namespace's Iceberg REST adapter
// arrives as an Iceberg table with a placeholder schema and table_type=lance,
// and keeps its fragments under data/ where the orphan cleaner walks. Every
// fragment is unreferenced by the Iceberg metadata, so a maintenance pass would
// delete the dataset.
func TestIsIcebergTableEntry(t *testing.T) {
	cases := []struct {
		name       string
		extended   map[string][]byte
		properties iceberg.Properties
		want       bool
	}{
		{
			name: "plain table",
			want: true,
		},
		{
			name:       "iceberg table_type is honoured case-insensitively",
			properties: iceberg.Properties{tableTypeProperty: "ICEBERG"},
			want:       true,
		},
		{
			name:       "lance table registered through the iceberg adapter",
			properties: iceberg.Properties{tableTypeProperty: "lance"},
			want:       false,
		},
		{
			name:     "view",
			extended: map[string][]byte{s3tables.ExtendedKeyEntryType: []byte(s3tables.EntryTypeView)},
			want:     false,
		},
		{
			name:     "explicit table marker",
			extended: map[string][]byte{s3tables.ExtendedKeyEntryType: []byte(s3tables.EntryTypeTable)},
			want:     true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			meta := buildTestMetadata(t, nil, nil, 0, c.properties)
			if got := isIcebergTableEntry(c.extended, meta); got != c.want {
				t.Fatalf("isIcebergTableEntry() = %v, want %v", got, c.want)
			}
		})
	}
}
