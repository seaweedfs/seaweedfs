package command

import (
	"reflect"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

func TestParseBucketList(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want []string
	}{
		{"empty", "", nil},
		{"single", "one", []string{"one"}},
		{"multi", "one,two,three", []string{"one", "two", "three"}},
		{"trims whitespace", " one , two , three ", []string{"one", "two", "three"}},
		{"drops empty entries", "one,,two,", []string{"one", "two"}},
		{"dedupes preserving order", "one,two,one,three,two", []string{"one", "two", "three"}},
		{"only commas", ",,,", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseBucketList(tt.in)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseBucketList(%q) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

func TestParseTableBucketList(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want []tableBucketEntry
	}{
		{"empty", "", nil},
		{"defaults to iceberg", "warehouse", []tableBucketEntry{{"warehouse", s3tables.FormatIceberg}}},
		{"explicit format", "vectors:LANCE", []tableBucketEntry{{"vectors", s3tables.FormatLance}}},
		{"format is case insensitive", "vectors:lance", []tableBucketEntry{{"vectors", s3tables.FormatLance}}},
		{"mixed formats", "warehouse,vectors:LANCE", []tableBucketEntry{{"warehouse", s3tables.FormatIceberg}, {"vectors", s3tables.FormatLance}}},
		{"trims whitespace", " warehouse , vectors : LANCE ", []tableBucketEntry{{"warehouse", s3tables.FormatIceberg}, {"vectors", s3tables.FormatLance}}},
		{"drops unsupported format", "warehouse,vectors:DELTA", []tableBucketEntry{{"warehouse", s3tables.FormatIceberg}}},
		{"empty format is the default", "warehouse:", []tableBucketEntry{{"warehouse", s3tables.FormatIceberg}}},
		{"dedupes by name", "vectors:LANCE,vectors:ICEBERG", []tableBucketEntry{{"vectors", s3tables.FormatLance}}},
		{"drops empty entries", "one,,two:LANCE,", []tableBucketEntry{{"one", s3tables.FormatIceberg}, {"two", s3tables.FormatLance}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseTableBucketList(tt.in)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseTableBucketList(%q) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

// A bucket's format decides which endpoint has to be up for it, so turning one
// catalog off must not change what the other format means.
func TestMiniServesTableFormat(t *testing.T) {
	icebergPort, lancePort := *miniS3Options.portIceberg, *miniS3Options.portLance
	t.Cleanup(func() {
		*miniS3Options.portIceberg, *miniS3Options.portLance = icebergPort, lancePort
	})

	*miniS3Options.portIceberg, *miniS3Options.portLance = 8181, 9101
	if !miniServesTableFormat(s3tables.FormatIceberg) || !miniServesTableFormat(s3tables.FormatLance) {
		t.Errorf("both endpoints up: want both formats served")
	}

	*miniS3Options.portIceberg = 0
	if miniServesTableFormat(s3tables.FormatIceberg) {
		t.Errorf("iceberg port 0: want ICEBERG unserved")
	}
	if !miniServesTableFormat(s3tables.FormatLance) {
		t.Errorf("iceberg port 0: want LANCE still served")
	}

	*miniS3Options.portLance = 0
	if miniServesTableFormat(s3tables.FormatLance) {
		t.Errorf("lance port 0: want LANCE unserved")
	}

	if miniServesTableFormat("DELTA") {
		t.Errorf("unknown format: want unserved")
	}
}

// The Iceberg catalog looks its default warehouse up by name, so a Lance entry
// or a leftover :FORMAT suffix in S3_TABLE_BUCKET names a bucket it cannot find.
func TestIcebergRoutingNames(t *testing.T) {
	tests := []struct {
		name string
		in   []tableBucketEntry
		want []string
	}{
		{"none", nil, nil},
		{"iceberg only", []tableBucketEntry{{"warehouse", s3tables.FormatIceberg}}, []string{"warehouse"}},
		{"lance is not routable", []tableBucketEntry{{"vectors", s3tables.FormatLance}}, nil},
		{"lance first", []tableBucketEntry{{"vectors", s3tables.FormatLance}, {"warehouse", s3tables.FormatIceberg}}, []string{"warehouse"}},
		{"order preserved", []tableBucketEntry{{"raw", s3tables.FormatIceberg}, {"vectors", s3tables.FormatLance}, {"curated", s3tables.FormatIceberg}}, []string{"raw", "curated"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := icebergRoutingNames(tt.in)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("icebergRoutingNames(%v) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

// A spec arriving through S3_TABLE_BUCKET carries suffixes too, and the catalog
// reads that same variable, so parsing and routing have to agree on the name.
func TestIcebergRoutingNamesFromEnvSpec(t *testing.T) {
	got := icebergRoutingNames(parseTableBucketList("vectors:LANCE,warehouse"))
	if want := []string{"warehouse"}; !reflect.DeepEqual(got, want) {
		t.Errorf("routing names for env spec = %v, want %v", got, want)
	}
}
