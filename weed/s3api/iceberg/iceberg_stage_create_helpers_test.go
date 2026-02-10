package iceberg

import (
	"testing"

	"github.com/apache/iceberg-go/table"
)

func TestHasAssertCreateRequirement(t *testing.T) {
	requirements := table.Requirements{table.AssertCreate()}
	if !hasAssertCreateRequirement(requirements) {
		t.Fatalf("hasAssertCreateRequirement() = false, want true")
	}

	requirements = table.Requirements{table.AssertDefaultSortOrderID(0)}
	if hasAssertCreateRequirement(requirements) {
		t.Fatalf("hasAssertCreateRequirement() = true, want false")
	}
}

func TestParseMetadataVersionFromLocation(t *testing.T) {
	testCases := []struct {
		location string
		version  int
	}{
		{location: "s3://b/ns/t/metadata/v1.metadata.json", version: 1},
		{location: "s3://b/ns/t/metadata/v25.metadata.json", version: 25},
		{location: "s3://b/ns/t/metadata/current.json", version: 0},
		{location: "", version: 0},
	}

	for _, tc := range testCases {
		if got := parseMetadataVersionFromLocation(tc.location); got != tc.version {
			t.Fatalf("parseMetadataVersionFromLocation(%q) = %d, want %d", tc.location, got, tc.version)
		}
	}
}
