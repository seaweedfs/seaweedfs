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
		if got := TableDataDirFromMetadataLocation(c.loc); got != c.want {
			t.Errorf("TableDataDirFromMetadataLocation(%q) = %q, want %q", c.loc, got, c.want)
		}
	}
}

func TestMetadataVersionFromLocationUniqueSuffix(t *testing.T) {
	cases := map[string]int{
		"s3://bkt/ns/t/metadata/v4.metadata.json":                                      4,
		"s3://bkt/ns/t/metadata/v4-0a1b2c3d-4e5f-6789-abcd-ef0123456789.metadata.json": 4,
		"s3://bkt/ns/t/metadata/00007-0a1b2c3d.metadata.json":                          7,
		"s3://bkt/ns/t/metadata/whatever.metadata.json":                                1,
	}
	for location, want := range cases {
		if got := metadataVersionFromLocation(location); got != want {
			t.Errorf("metadataVersionFromLocation(%q) = %d, want %d", location, got, want)
		}
	}
}
