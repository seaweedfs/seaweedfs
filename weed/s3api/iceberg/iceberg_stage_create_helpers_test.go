package iceberg

import (
	"strings"
	"testing"

	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
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
		{location: "v1.metadata.json", version: 1},
		{location: "s3://b/ns/t/metadata/v0.metadata.json", version: 0},
		{location: "s3://b/ns/t/metadata/v-1.metadata.json", version: 0},
		{location: "s3://b/ns/t/metadata/vABC.metadata.json", version: 0},
		{location: "s3://b/ns/t/metadata/current.json", version: 0},
		{location: "", version: 0},
	}

	for _, tc := range testCases {
		t.Run(tc.location, func(t *testing.T) {
			if got := parseMetadataVersionFromLocation(tc.location); got != tc.version {
				t.Errorf("parseMetadataVersionFromLocation(%q) = %d, want %d", tc.location, got, tc.version)
			}
		})
	}
}

func TestStageCreateMarkerNamespaceKey(t *testing.T) {
	key := stageCreateMarkerNamespaceKey([]string{"a", "b"})
	if key == "a\x1fb" {
		t.Fatalf("stageCreateMarkerNamespaceKey() returned unescaped namespace key %q", key)
	}
	if !strings.Contains(key, "%1F") {
		t.Fatalf("stageCreateMarkerNamespaceKey() = %q, want escaped unit separator", key)
	}
}

func TestStageCreateMarkerDir(t *testing.T) {
	dir := stageCreateMarkerDir("warehouse", []string{"ns"}, "orders")
	if !strings.Contains(dir, stageCreateMarkerDirName) {
		t.Fatalf("stageCreateMarkerDir() = %q, want marker dir segment %q", dir, stageCreateMarkerDirName)
	}
	if !strings.HasSuffix(dir, "/orders") {
		t.Fatalf("stageCreateMarkerDir() = %q, want suffix /orders", dir)
	}
}

func TestStageCreateStagedTablePath(t *testing.T) {
	tableUUID := uuid.MustParse("11111111-2222-3333-4444-555555555555")
	stagedPath := stageCreateStagedTablePath([]string{"ns"}, "orders", tableUUID)
	if !strings.Contains(stagedPath, stageCreateMarkerDirName) {
		t.Fatalf("stageCreateStagedTablePath() = %q, want marker dir segment %q", stagedPath, stageCreateMarkerDirName)
	}
	if !strings.HasSuffix(stagedPath, "/"+tableUUID.String()) {
		t.Fatalf("stageCreateStagedTablePath() = %q, want UUID suffix %q", stagedPath, tableUUID.String())
	}
}
