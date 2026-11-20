package app

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3vectors"
)

func TestGroupDigits(t *testing.T) {
	for n, want := range map[float64]string{0: "0", 999: "999", 1000: "1,000", 12400: "12,400", 2140312: "2,140,312", 8899.6: "8,900"} {
		if got := groupDigits(n); got != want {
			t.Errorf("groupDigits(%v) = %q, want %q", n, got, want)
		}
	}
}

func TestNonFilterableJSONIsAlwaysAList(t *testing.T) {
	for _, index := range []*s3vectors.Index{
		{},
		{MetadataConfiguration: &s3vectors.MetadataConfiguration{}},
		{MetadataConfiguration: &s3vectors.MetadataConfiguration{NonFilterableMetadataKeys: []string{}}},
	} {
		if got := nonFilterableJSON(index); got != "[]" {
			t.Errorf("%+v: %s", index.MetadataConfiguration, got)
		}
	}
	index := &s3vectors.Index{MetadataConfiguration: &s3vectors.MetadataConfiguration{NonFilterableMetadataKeys: []string{"body"}}}
	if got := nonFilterableJSON(index); got != `["body"]` {
		t.Error(got)
	}
}
