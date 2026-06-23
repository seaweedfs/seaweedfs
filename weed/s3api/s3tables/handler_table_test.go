package s3tables

import "testing"

func TestMetadataVersionFromLocation(t *testing.T) {
	cases := []struct {
		location string
		want     int
	}{
		{"s3://bucket/ns/tbl/metadata/v1.metadata.json", 1},
		{"s3://bucket/ns/tbl/metadata/v7.metadata.json", 7},
		{"v42.metadata.json", 42},
		{"s3://bucket/ns/tbl/metadata/00003-9f1c2b3a-4d5e-6f70-8192-a3b4c5d6e7f8.metadata.json", 3},
		{"00012-abcdef.metadata.json", 12},
		{"s3://bucket/ns/tbl/metadata/00000-abc.metadata.json", 1},
		{"s3://bucket/ns/tbl/metadata/v0.metadata.json", 1},
		{"s3://bucket/ns/tbl/metadata/garbage.metadata.json", 1},
		{"", 1},
	}
	for _, c := range cases {
		if got := metadataVersionFromLocation(c.location); got != c.want {
			t.Errorf("metadataVersionFromLocation(%q) = %d, want %d", c.location, got, c.want)
		}
	}
}
