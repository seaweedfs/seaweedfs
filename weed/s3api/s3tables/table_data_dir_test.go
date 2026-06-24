package s3tables

import (
	"path"
	"testing"
)

func TestTableDataDirFromMetadataLocation(t *testing.T) {
	cases := []struct {
		loc  string
		want string
	}{
		{"s3://warehouse/sales/orders/metadata/v1.metadata.json", path.Join(TablesPath, "warehouse/sales/orders")},
		{"s3://warehouse/sales/orders/metadata/00003-9f1c.metadata.json", path.Join(TablesPath, "warehouse/sales/orders")},
		{"s3://warehouse/ns/tbl", path.Join(TablesPath, "warehouse/ns/tbl")},
		{"", ""},
	}
	for _, c := range cases {
		if got := tableDataDirFromMetadataLocation(c.loc); got != c.want {
			t.Errorf("tableDataDirFromMetadataLocation(%q) = %q, want %q", c.loc, got, c.want)
		}
	}
}
