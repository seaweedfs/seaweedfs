package app

import (
	"context"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/admin/dash"
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

// Removing or cancelling a key's index asks in a Bootstrap modal, as
// deleting an index does, not in the browser's confirm().
func TestMetadataKeyRemovalAsksInAModal(t *testing.T) {
	data := dash.S3VectorsBucketData{
		Bucket: dash.S3VectorsBucketSummary{Name: "b"},
		Panels: []dash.S3VectorsIndexPanel{{
			Index: &s3vectors.Index{IndexName: "i"},
			Keys: []dash.S3VectorsMetadataKeyRow{
				{Key: "author", State: "ready"},
				{Key: "category", State: "building"},
			},
		}},
	}
	var page strings.Builder
	if err := S3VectorsBucketDetails(data).Render(context.Background(), &page); err != nil {
		t.Fatal(err)
	}
	html := page.String()
	if strings.Contains(html, "' the index of metadata key ' +") {
		t.Error("removing a key still asks with confirm()")
	}
	for _, want := range []string{
		`id="removeS3VectorsMetadataKeyModal"`,
		`id="removeS3VectorsMetadataKeyBtn"`,
		`new bootstrap.Modal(removeModalEl)`,
		`data-key="author" data-action="Remove"`,
		`data-key="category" data-action="Cancel"`,
	} {
		if !strings.Contains(html, want) {
			t.Errorf("missing %s", want)
		}
	}
}
