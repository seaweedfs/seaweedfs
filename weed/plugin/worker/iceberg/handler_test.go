package iceberg

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/parquet-go/parquet-go"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

func TestParseConfig(t *testing.T) {
	config := ParseConfig(nil)

	if config.SnapshotRetentionHours != defaultSnapshotRetentionHours {
		t.Errorf("expected SnapshotRetentionHours=%d, got %d", defaultSnapshotRetentionHours, config.SnapshotRetentionHours)
	}
	if config.MaxSnapshotsToKeep != defaultMaxSnapshotsToKeep {
		t.Errorf("expected MaxSnapshotsToKeep=%d, got %d", defaultMaxSnapshotsToKeep, config.MaxSnapshotsToKeep)
	}
	if config.OrphanOlderThanHours != defaultOrphanOlderThanHours {
		t.Errorf("expected OrphanOlderThanHours=%d, got %d", defaultOrphanOlderThanHours, config.OrphanOlderThanHours)
	}
	if config.MaxCommitRetries != defaultMaxCommitRetries {
		t.Errorf("expected MaxCommitRetries=%d, got %d", defaultMaxCommitRetries, config.MaxCommitRetries)
	}
	if config.Operations != defaultOperations {
		t.Errorf("expected Operations=%q, got %q", defaultOperations, config.Operations)
	}
}

func TestParseOperations(t *testing.T) {
	tests := []struct {
		input    string
		expected []string
		wantErr  bool
	}{
		{"all", []string{"compact", "expire_snapshots", "remove_orphans", "rewrite_manifests"}, false},
		{"", []string{"compact", "expire_snapshots", "remove_orphans", "rewrite_manifests"}, false},
		{"expire_snapshots", []string{"expire_snapshots"}, false},
		{"compact", []string{"compact"}, false},
		{"rewrite_manifests,expire_snapshots", []string{"expire_snapshots", "rewrite_manifests"}, false},
		{"compact,expire_snapshots", []string{"compact", "expire_snapshots"}, false},
		{"remove_orphans, rewrite_manifests", []string{"remove_orphans", "rewrite_manifests"}, false},
		{"expire_snapshots,remove_orphans,rewrite_manifests", []string{"expire_snapshots", "remove_orphans", "rewrite_manifests"}, false},
		{"compact,expire_snapshots,remove_orphans,rewrite_manifests", []string{"compact", "expire_snapshots", "remove_orphans", "rewrite_manifests"}, false},
		{"unknown_op", nil, true},
		{"expire_snapshots,bad_op", nil, true},
	}

	for _, tc := range tests {
		result, err := parseOperations(tc.input)
		if tc.wantErr {
			if err == nil {
				t.Errorf("parseOperations(%q) expected error, got %v", tc.input, result)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseOperations(%q) unexpected error: %v", tc.input, err)
			continue
		}
		if len(result) != len(tc.expected) {
			t.Errorf("parseOperations(%q) = %v, want %v", tc.input, result, tc.expected)
			continue
		}
		for i := range result {
			if result[i] != tc.expected[i] {
				t.Errorf("parseOperations(%q)[%d] = %q, want %q", tc.input, i, result[i], tc.expected[i])
			}
		}
	}
}

func TestExtractMetadataVersion(t *testing.T) {
	tests := []struct {
		input    string
		expected int
	}{
		{"v1.metadata.json", 1},
		{"v5.metadata.json", 5},
		{"v100.metadata.json", 100},
		{"v0.metadata.json", 0},
		{"invalid.metadata.json", 0},
		{"metadata.json", 0},
		{"", 0},
		{"v.metadata.json", 0},
		{"v7-1709766000.metadata.json", 7},
		{"v42-abc123.metadata.json", 42},
		{"v5-.metadata.json", 5},
	}

	for _, tc := range tests {
		result := extractMetadataVersion(tc.input)
		if result != tc.expected {
			t.Errorf("extractMetadataVersion(%q) = %d, want %d", tc.input, result, tc.expected)
		}
	}
}

func TestNeedsMaintenanceNoSnapshots(t *testing.T) {
	config := Config{
		SnapshotRetentionHours: 24,
		MaxSnapshotsToKeep:     2,
	}

	meta := buildTestMetadata(t, nil)
	if needsMaintenance(meta, config) {
		t.Error("expected no maintenance for table with no snapshots")
	}
}

func TestNeedsMaintenanceExceedsMaxSnapshots(t *testing.T) {
	config := Config{
		SnapshotRetentionHours: 24 * 365, // very long retention
		MaxSnapshotsToKeep:     2,
	}

	now := time.Now().UnixMilli()
	snapshots := []table.Snapshot{
		{SnapshotID: 1, TimestampMs: now, ManifestList: "metadata/snap-1.avro"},
		{SnapshotID: 2, TimestampMs: now + 1, ManifestList: "metadata/snap-2.avro"},
		{SnapshotID: 3, TimestampMs: now + 2, ManifestList: "metadata/snap-3.avro"},
	}
	meta := buildTestMetadata(t, snapshots)
	if !needsMaintenance(meta, config) {
		t.Error("expected maintenance for table exceeding max snapshots")
	}
}

func TestNeedsMaintenanceWithinLimits(t *testing.T) {
	config := Config{
		SnapshotRetentionHours: 24 * 365, // very long retention
		MaxSnapshotsToKeep:     5,
	}

	now := time.Now().UnixMilli()
	snapshots := []table.Snapshot{
		{SnapshotID: 1, TimestampMs: now, ManifestList: "metadata/snap-1.avro"},
	}
	meta := buildTestMetadata(t, snapshots)
	if needsMaintenance(meta, config) {
		t.Error("expected no maintenance for table within limits")
	}
}

func TestNeedsMaintenanceOldSnapshot(t *testing.T) {
	// Use a retention of 0 hours so that any snapshot is considered "old"
	config := Config{
		SnapshotRetentionHours: 0, // instant expiry
		MaxSnapshotsToKeep:     10,
	}

	now := time.Now().UnixMilli()
	snapshots := []table.Snapshot{
		{SnapshotID: 1, TimestampMs: now - 1, ManifestList: "metadata/snap-1.avro"},
	}
	meta := buildTestMetadata(t, snapshots)
	// With 0 retention, any snapshot with timestamp < now should need maintenance
	if !needsMaintenance(meta, config) {
		t.Error("expected maintenance for table with expired snapshot")
	}
}

func TestCapabilityAndDescriptor(t *testing.T) {
	handler := NewHandler(nil)

	cap := handler.Capability()
	if cap.JobType != jobType {
		t.Errorf("expected job type %q, got %q", jobType, cap.JobType)
	}
	if !cap.CanDetect {
		t.Error("expected CanDetect=true")
	}
	if !cap.CanExecute {
		t.Error("expected CanExecute=true")
	}

	desc := handler.Descriptor()
	if desc.JobType != jobType {
		t.Errorf("expected job type %q, got %q", jobType, desc.JobType)
	}
	if desc.AdminConfigForm == nil {
		t.Error("expected admin config form")
	}
	if desc.WorkerConfigForm == nil {
		t.Error("expected worker config form")
	}
	if desc.AdminRuntimeDefaults == nil {
		t.Error("expected admin runtime defaults")
	}
	if desc.AdminRuntimeDefaults.Enabled {
		t.Error("expected disabled by default")
	}
}

func TestBuildMaintenanceProposal(t *testing.T) {
	handler := NewHandler(nil)

	now := time.Now().UnixMilli()
	snapshots := []table.Snapshot{
		{SnapshotID: 1, TimestampMs: now},
		{SnapshotID: 2, TimestampMs: now + 1},
	}
	meta := buildTestMetadata(t, snapshots)

	info := tableInfo{
		BucketName: "my-bucket",
		Namespace:  "analytics",
		TableName:  "events",
		TablePath:  "analytics/events",
		Metadata:   meta,
	}

	proposal := handler.buildMaintenanceProposal(info, "localhost:8888")

	expectedDedupe := "iceberg_maintenance:my-bucket/analytics/events"
	if proposal.DedupeKey != expectedDedupe {
		t.Errorf("expected dedupe key %q, got %q", expectedDedupe, proposal.DedupeKey)
	}
	if proposal.JobType != jobType {
		t.Errorf("expected job type %q, got %q", jobType, proposal.JobType)
	}

	if readStringConfig(proposal.Parameters, "bucket_name", "") != "my-bucket" {
		t.Error("expected bucket_name=my-bucket in parameters")
	}
	if readStringConfig(proposal.Parameters, "namespace", "") != "analytics" {
		t.Error("expected namespace=analytics in parameters")
	}
	if readStringConfig(proposal.Parameters, "table_name", "") != "events" {
		t.Error("expected table_name=events in parameters")
	}
	if readStringConfig(proposal.Parameters, "filer_address", "") != "localhost:8888" {
		t.Error("expected filer_address=localhost:8888 in parameters")
	}
}

func TestManifestRewritePathConsistency(t *testing.T) {
	// Verify that WriteManifest returns a ManifestFile whose FilePath()
	// matches the path we pass in. This ensures the pattern used in
	// rewriteManifests (compute filename once, pass to both WriteManifest
	// and saveFilerFile) produces consistent references.
	schema := newTestSchema()
	spec := *iceberg.UnpartitionedSpec

	snapshotID := int64(42)
	manifestFileName := fmt.Sprintf("merged-%d-%d.avro", snapshotID, int64(1700000000000))
	manifestPath := "metadata/" + manifestFileName

	// Create a minimal manifest entry to write
	dfBuilder, err := iceberg.NewDataFileBuilder(
		spec,
		iceberg.EntryContentData,
		"data/test.parquet",
		iceberg.ParquetFile,
		map[int]any{},
		nil, nil,
		1,    // recordCount
		1024, // fileSizeBytes
	)
	if err != nil {
		t.Fatalf("failed to build data file: %v", err)
	}
	entry := iceberg.NewManifestEntry(
		iceberg.EntryStatusADDED,
		&snapshotID,
		nil, nil,
		dfBuilder.Build(),
	)

	var buf bytes.Buffer
	mf, err := iceberg.WriteManifest(
		manifestPath,
		&buf,
		2, // version
		spec,
		schema,
		snapshotID,
		[]iceberg.ManifestEntry{entry},
	)
	if err != nil {
		t.Fatalf("WriteManifest failed: %v", err)
	}

	if mf.FilePath() != manifestPath {
		t.Errorf("manifest FilePath() = %q, want %q", mf.FilePath(), manifestPath)
	}

	// Verify the filename we'd use for saveFilerFile matches
	if path.Base(mf.FilePath()) != manifestFileName {
		t.Errorf("manifest base name = %q, want %q", path.Base(mf.FilePath()), manifestFileName)
	}
}

func TestManifestRewriteNestedPathConsistency(t *testing.T) {
	// Verify that WriteManifest with nested paths preserves the full path
	// and that loadFileByIcebergPath (via normalizeIcebergPath) would
	// resolve them correctly.
	schema := newTestSchema()
	spec := *iceberg.UnpartitionedSpec
	snapshotID := int64(42)

	testCases := []struct {
		name         string
		manifestPath string
	}{
		{"nested two levels", "metadata/a/b/merged-42-1700000000000.avro"},
		{"nested one level", "metadata/subdir/manifest-42.avro"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dfBuilder, err := iceberg.NewDataFileBuilder(
				spec,
				iceberg.EntryContentData,
				"data/test.parquet",
				iceberg.ParquetFile,
				map[int]any{},
				nil, nil,
				1, 1024,
			)
			if err != nil {
				t.Fatalf("failed to build data file: %v", err)
			}
			entry := iceberg.NewManifestEntry(
				iceberg.EntryStatusADDED,
				&snapshotID,
				nil, nil,
				dfBuilder.Build(),
			)

			var buf bytes.Buffer
			mf, err := iceberg.WriteManifest(
				tc.manifestPath, &buf, 2, spec, schema, snapshotID,
				[]iceberg.ManifestEntry{entry},
			)
			if err != nil {
				t.Fatalf("WriteManifest failed: %v", err)
			}

			if mf.FilePath() != tc.manifestPath {
				t.Errorf("FilePath() = %q, want %q", mf.FilePath(), tc.manifestPath)
			}

			// normalizeIcebergPath should return the path unchanged when already relative
			normalized := normalizeIcebergPath(tc.manifestPath, "bucket", "ns/table")
			if normalized != tc.manifestPath {
				t.Errorf("normalizeIcebergPath(%q) = %q, want %q", tc.manifestPath, normalized, tc.manifestPath)
			}

			// Verify normalization strips S3 scheme prefix correctly
			s3Path := "s3://bucket/ns/table/" + tc.manifestPath
			normalized = normalizeIcebergPath(s3Path, "bucket", "ns/table")
			if normalized != tc.manifestPath {
				t.Errorf("normalizeIcebergPath(%q) = %q, want %q", s3Path, normalized, tc.manifestPath)
			}
		})
	}
}

func TestNormalizeIcebergPath(t *testing.T) {
	tests := []struct {
		name        string
		icebergPath string
		bucket      string
		tablePath   string
		expected    string
	}{
		{
			"relative metadata path",
			"metadata/snap-1.avro",
			"mybucket", "ns/table",
			"metadata/snap-1.avro",
		},
		{
			"relative data path",
			"data/file.parquet",
			"mybucket", "ns/table",
			"data/file.parquet",
		},
		{
			"S3 URL",
			"s3://mybucket/ns/table/metadata/snap-1.avro",
			"mybucket", "ns/table",
			"metadata/snap-1.avro",
		},
		{
			"absolute filer path",
			"/buckets/mybucket/ns/table/data/file.parquet",
			"mybucket", "ns/table",
			"data/file.parquet",
		},
		{
			"nested data path",
			"data/region=us/city=sf/file.parquet",
			"mybucket", "ns/table",
			"data/region=us/city=sf/file.parquet",
		},
		{
			"S3 URL nested",
			"s3://mybucket/ns/table/data/region=us/file.parquet",
			"mybucket", "ns/table",
			"data/region=us/file.parquet",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := normalizeIcebergPath(tc.icebergPath, tc.bucket, tc.tablePath)
			if result != tc.expected {
				t.Errorf("normalizeIcebergPath(%q, %q, %q) = %q, want %q",
					tc.icebergPath, tc.bucket, tc.tablePath, result, tc.expected)
			}
		})
	}
}

func TestPartitionKey(t *testing.T) {
	tests := []struct {
		name      string
		partition map[int]any
		expected  string
	}{
		{"empty partition", map[int]any{}, "__unpartitioned__"},
		{"nil partition", nil, "__unpartitioned__"},
		{"single field", map[int]any{1: "us-east"}, "1=\"us-east\""},
		{"multiple fields sorted", map[int]any{3: "2024", 1: "us-east"}, "1=\"us-east\"\x003=\"2024\""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := partitionKey(tc.partition)
			if result != tc.expected {
				t.Errorf("partitionKey(%v) = %q, want %q", tc.partition, result, tc.expected)
			}
		})
	}
}

func TestBuildCompactionBins(t *testing.T) {
	targetSize := int64(256 * 1024 * 1024) // 256MB
	minFiles := 3

	// Create test entries: small files in same partition
	entries := makeTestEntries(t, []testEntrySpec{
		{path: "data/f1.parquet", size: 1024, partition: map[int]any{}},
		{path: "data/f2.parquet", size: 2048, partition: map[int]any{}},
		{path: "data/f3.parquet", size: 4096, partition: map[int]any{}},
	})

	bins := buildCompactionBins(entries, targetSize, minFiles)
	if len(bins) != 1 {
		t.Fatalf("expected 1 bin, got %d", len(bins))
	}
	if len(bins[0].Entries) != 3 {
		t.Errorf("expected 3 entries in bin, got %d", len(bins[0].Entries))
	}
}

func TestBuildCompactionBinsFiltersLargeFiles(t *testing.T) {
	targetSize := int64(4000)
	minFiles := 2

	entries := makeTestEntries(t, []testEntrySpec{
		{path: "data/small1.parquet", size: 1024, partition: map[int]any{}},
		{path: "data/small2.parquet", size: 2048, partition: map[int]any{}},
		{path: "data/large.parquet", size: 5000, partition: map[int]any{}},
	})

	bins := buildCompactionBins(entries, targetSize, minFiles)
	if len(bins) != 1 {
		t.Fatalf("expected 1 bin, got %d", len(bins))
	}
	if len(bins[0].Entries) != 2 {
		t.Errorf("expected 2 entries (large excluded), got %d", len(bins[0].Entries))
	}
}

func TestBuildCompactionBinsMinFilesThreshold(t *testing.T) {
	targetSize := int64(256 * 1024 * 1024)
	minFiles := 5

	entries := makeTestEntries(t, []testEntrySpec{
		{path: "data/f1.parquet", size: 1024, partition: map[int]any{}},
		{path: "data/f2.parquet", size: 2048, partition: map[int]any{}},
	})

	bins := buildCompactionBins(entries, targetSize, minFiles)
	if len(bins) != 0 {
		t.Errorf("expected 0 bins (below min threshold), got %d", len(bins))
	}
}

func TestBuildCompactionBinsMultiplePartitions(t *testing.T) {
	targetSize := int64(256 * 1024 * 1024)
	minFiles := 2

	partA := map[int]any{1: "us-east"}
	partB := map[int]any{1: "eu-west"}

	entries := makeTestEntries(t, []testEntrySpec{
		{path: "data/a1.parquet", size: 1024, partition: partA},
		{path: "data/a2.parquet", size: 2048, partition: partA},
		{path: "data/b1.parquet", size: 1024, partition: partB},
		{path: "data/b2.parquet", size: 2048, partition: partB},
		{path: "data/b3.parquet", size: 4096, partition: partB},
	})

	bins := buildCompactionBins(entries, targetSize, minFiles)
	if len(bins) != 2 {
		t.Fatalf("expected 2 bins (one per partition), got %d", len(bins))
	}
}

func TestSplitOversizedBinRespectsTargetSize(t *testing.T) {
	targetSize := int64(100)
	minFiles := 2

	entries := makeTestEntries(t, []testEntrySpec{
		{path: "data/f1.parquet", size: 80, partition: map[int]any{}},
		{path: "data/f2.parquet", size: 80, partition: map[int]any{}},
		{path: "data/f3.parquet", size: 10, partition: map[int]any{}},
		{path: "data/f4.parquet", size: 10, partition: map[int]any{}},
	})

	bins := splitOversizedBin(compactionBin{
		PartitionKey: "__unpartitioned__",
		Partition:    map[int]any{},
		Entries:      entries,
		TotalSize:    180,
	}, targetSize, minFiles)

	if len(bins) == 0 {
		t.Fatal("expected at least one valid bin")
	}
	for i, bin := range bins {
		if bin.TotalSize > targetSize {
			t.Fatalf("bin %d exceeds target size: got %d want <= %d", i, bin.TotalSize, targetSize)
		}
		if len(bin.Entries) < minFiles {
			t.Fatalf("bin %d does not meet minFiles: got %d want >= %d", i, len(bin.Entries), minFiles)
		}
	}
}

func TestSplitOversizedBinDropsImpossibleRunts(t *testing.T) {
	targetSize := int64(100)
	minFiles := 2

	entries := makeTestEntries(t, []testEntrySpec{
		{path: "data/f1.parquet", size: 60, partition: map[int]any{}},
		{path: "data/f2.parquet", size: 60, partition: map[int]any{}},
		{path: "data/f3.parquet", size: 60, partition: map[int]any{}},
	})

	bins := splitOversizedBin(compactionBin{
		PartitionKey: "__unpartitioned__",
		Partition:    map[int]any{},
		Entries:      entries,
		TotalSize:    180,
	}, targetSize, minFiles)

	if len(bins) != 0 {
		t.Fatalf("expected no valid bins, got %d", len(bins))
	}
}

type testEntrySpec struct {
	path      string
	size      int64
	partition map[int]any
	specID    int32 // partition spec ID; 0 uses UnpartitionedSpec
}

func makeTestEntries(t *testing.T, specs []testEntrySpec) []iceberg.ManifestEntry {
	t.Helper()
	entries := make([]iceberg.ManifestEntry, 0, len(specs))
	for _, spec := range specs {
		partSpec := *iceberg.UnpartitionedSpec
		dfBuilder, err := iceberg.NewDataFileBuilder(
			partSpec,
			iceberg.EntryContentData,
			spec.path,
			iceberg.ParquetFile,
			spec.partition,
			nil, nil,
			1, // recordCount (must be > 0)
			spec.size,
		)
		if err != nil {
			t.Fatalf("failed to build data file %s: %v", spec.path, err)
		}
		snapID := int64(1)
		entry := iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapID, nil, nil, dfBuilder.Build())
		entries = append(entries, entry)
	}
	return entries
}

// makeTestEntriesWithSpec creates manifest entries using specific partition specs.
// Each spec in the specs slice can specify a specID; the entry is built using
// a PartitionSpec with that ID.
func makeTestEntriesWithSpec(t *testing.T, specs []testEntrySpec, partSpecs map[int32]iceberg.PartitionSpec) []iceberg.ManifestEntry {
	t.Helper()
	entries := make([]iceberg.ManifestEntry, 0, len(specs))
	for _, s := range specs {
		ps, ok := partSpecs[s.specID]
		if !ok {
			t.Fatalf("spec ID %d not found in partSpecs map", s.specID)
		}
		dfBuilder, err := iceberg.NewDataFileBuilder(
			ps,
			iceberg.EntryContentData,
			s.path,
			iceberg.ParquetFile,
			s.partition,
			nil, nil,
			1,
			s.size,
		)
		if err != nil {
			t.Fatalf("failed to build data file %s: %v", s.path, err)
		}
		snapID := int64(1)
		entry := iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapID, nil, nil, dfBuilder.Build())
		entries = append(entries, entry)
	}
	return entries
}

func TestBuildCompactionBinsMultipleSpecs(t *testing.T) {
	targetSize := int64(256 * 1024 * 1024)
	minFiles := 2

	// Two partition specs with different IDs
	spec0 := iceberg.NewPartitionSpecID(0)
	spec1 := iceberg.NewPartitionSpecID(1)
	partSpecs := map[int32]iceberg.PartitionSpec{
		0: spec0,
		1: spec1,
	}

	// Entries from two different specs with same partition values should go
	// to separate bins.
	entries := makeTestEntriesWithSpec(t, []testEntrySpec{
		{path: "data/s0-f1.parquet", size: 1024, partition: map[int]any{}, specID: 0},
		{path: "data/s0-f2.parquet", size: 2048, partition: map[int]any{}, specID: 0},
		{path: "data/s1-f1.parquet", size: 1024, partition: map[int]any{}, specID: 1},
		{path: "data/s1-f2.parquet", size: 2048, partition: map[int]any{}, specID: 1},
	}, partSpecs)

	bins := buildCompactionBins(entries, targetSize, minFiles)
	if len(bins) != 2 {
		t.Fatalf("expected 2 bins (one per spec), got %d", len(bins))
	}

	// Verify each bin has entries from only one spec
	specsSeen := make(map[int32]bool)
	for _, bin := range bins {
		specsSeen[bin.SpecID] = true
		for _, entry := range bin.Entries {
			if entry.DataFile().SpecID() != bin.SpecID {
				t.Errorf("bin specID=%d contains entry with specID=%d", bin.SpecID, entry.DataFile().SpecID())
			}
		}
	}
	if !specsSeen[0] || !specsSeen[1] {
		t.Errorf("expected bins for spec 0 and 1, got specs %v", specsSeen)
	}
}

func TestBuildCompactionBinsSingleSpec(t *testing.T) {
	// Verify existing behavior: all entries with spec 0 still group correctly.
	targetSize := int64(256 * 1024 * 1024)
	minFiles := 2

	spec0 := iceberg.NewPartitionSpecID(0)
	partSpecs := map[int32]iceberg.PartitionSpec{0: spec0}

	entries := makeTestEntriesWithSpec(t, []testEntrySpec{
		{path: "data/f1.parquet", size: 1024, partition: map[int]any{}, specID: 0},
		{path: "data/f2.parquet", size: 2048, partition: map[int]any{}, specID: 0},
		{path: "data/f3.parquet", size: 4096, partition: map[int]any{}, specID: 0},
	}, partSpecs)

	bins := buildCompactionBins(entries, targetSize, minFiles)
	if len(bins) != 1 {
		t.Fatalf("expected 1 bin, got %d", len(bins))
	}
	if len(bins[0].Entries) != 3 {
		t.Errorf("expected 3 entries in bin, got %d", len(bins[0].Entries))
	}
	if bins[0].SpecID != 0 {
		t.Errorf("expected specID=0, got %d", bins[0].SpecID)
	}
}

func TestParseConfigApplyDeletes(t *testing.T) {
	// Default: true
	config := ParseConfig(nil)
	if !config.ApplyDeletes {
		t.Error("expected ApplyDeletes=true by default")
	}

	// Explicit false
	config = ParseConfig(map[string]*plugin_pb.ConfigValue{
		"apply_deletes": {Kind: &plugin_pb.ConfigValue_BoolValue{BoolValue: false}},
	})
	if config.ApplyDeletes {
		t.Error("expected ApplyDeletes=false when explicitly set")
	}

	// String "false"
	config = ParseConfig(map[string]*plugin_pb.ConfigValue{
		"apply_deletes": {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: "false"}},
	})
	if config.ApplyDeletes {
		t.Error("expected ApplyDeletes=false when set via string 'false'")
	}
}

func TestCollectPositionDeletes(t *testing.T) {
	fs, client := startFakeFiler(t)

	// Create a position delete Parquet file with file_path + pos columns
	type posDeleteRow struct {
		FilePath string `parquet:"file_path"`
		Pos      int64  `parquet:"pos"`
	}
	var buf bytes.Buffer
	writer := parquet.NewWriter(&buf, parquet.SchemaOf(new(posDeleteRow)))
	rows := []posDeleteRow{
		{"data/file1.parquet", 0},
		{"data/file1.parquet", 5},
		{"data/file1.parquet", 10},
		{"data/file2.parquet", 3},
	}
	for _, r := range rows {
		if err := writer.Write(&r); err != nil {
			t.Fatalf("write pos delete row: %v", err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close pos delete writer: %v", err)
	}

	// Store in fake filer
	dataDir := "/buckets/test-bucket/ns/tbl/data"
	fs.putEntry(dataDir, "pos-delete-1.parquet", &filer_pb.Entry{
		Name:    "pos-delete-1.parquet",
		Content: buf.Bytes(),
	})

	// Create a manifest entry for the position delete file
	spec := *iceberg.UnpartitionedSpec
	dfBuilder, err := iceberg.NewDataFileBuilder(
		spec,
		iceberg.EntryContentPosDeletes,
		"data/pos-delete-1.parquet",
		iceberg.ParquetFile,
		map[int]any{},
		nil, nil,
		int64(len(rows)),
		int64(buf.Len()),
	)
	if err != nil {
		t.Fatalf("build pos delete data file: %v", err)
	}
	snapID := int64(1)
	entry := iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapID, nil, nil, dfBuilder.Build())

	result, err := collectPositionDeletes(context.Background(), client, "test-bucket", "ns/tbl", []iceberg.ManifestEntry{entry})
	if err != nil {
		t.Fatalf("collectPositionDeletes: %v", err)
	}

	// Verify results
	if len(result["data/file1.parquet"]) != 3 {
		t.Errorf("expected 3 positions for file1, got %d", len(result["data/file1.parquet"]))
	}
	if len(result["data/file2.parquet"]) != 1 {
		t.Errorf("expected 1 position for file2, got %d", len(result["data/file2.parquet"]))
	}

	// Verify sorted
	positions := result["data/file1.parquet"]
	for i := 1; i < len(positions); i++ {
		if positions[i] <= positions[i-1] {
			t.Errorf("positions not sorted: %v", positions)
			break
		}
	}
}

func TestCollectEqualityDeletes(t *testing.T) {
	fs, client := startFakeFiler(t)

	// Create an equality delete Parquet file with id + name columns
	type eqDeleteRow struct {
		ID   int64  `parquet:"id"`
		Name string `parquet:"name"`
	}
	var buf bytes.Buffer
	writer := parquet.NewWriter(&buf, parquet.SchemaOf(new(eqDeleteRow)))
	deleteRows := []eqDeleteRow{
		{1, "alice"},
		{2, "bob"},
		{3, "charlie"},
	}
	for _, r := range deleteRows {
		if err := writer.Write(&r); err != nil {
			t.Fatalf("write eq delete row: %v", err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close eq delete writer: %v", err)
	}

	dataDir := "/buckets/test-bucket/ns/tbl/data"
	fs.putEntry(dataDir, "eq-delete-1.parquet", &filer_pb.Entry{
		Name:    "eq-delete-1.parquet",
		Content: buf.Bytes(),
	})

	// Create manifest entry with equality field IDs
	schema := newTestSchema() // has field 1=id, 2=name
	spec := *iceberg.UnpartitionedSpec
	dfBuilder, err := iceberg.NewDataFileBuilder(
		spec,
		iceberg.EntryContentEqDeletes,
		"data/eq-delete-1.parquet",
		iceberg.ParquetFile,
		map[int]any{},
		nil, nil,
		int64(len(deleteRows)),
		int64(buf.Len()),
	)
	if err != nil {
		t.Fatalf("build eq delete data file: %v", err)
	}
	dfBuilder.EqualityFieldIDs([]int{1, 2}) // id + name
	snapID := int64(1)
	entry := iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapID, nil, nil, dfBuilder.Build())

	result, fieldIDs, err := collectEqualityDeletes(context.Background(), client, "test-bucket", "ns/tbl", []iceberg.ManifestEntry{entry}, schema)
	if err != nil {
		t.Fatalf("collectEqualityDeletes: %v", err)
	}

	if len(result) != 3 {
		t.Errorf("expected 3 equality delete keys, got %d", len(result))
	}
	if len(fieldIDs) != 2 {
		t.Errorf("expected 2 field IDs, got %d", len(fieldIDs))
	}
}

func TestMergeParquetFilesWithPositionDeletes(t *testing.T) {
	fs, client := startFakeFiler(t)

	// Create two data files with known rows
	type dataRow struct {
		ID   int64  `parquet:"id"`
		Name string `parquet:"name"`
	}

	writeDataFile := func(name string, rows []dataRow) {
		var buf bytes.Buffer
		w := parquet.NewWriter(&buf, parquet.SchemaOf(new(dataRow)))
		for _, r := range rows {
			if err := w.Write(&r); err != nil {
				t.Fatalf("write row: %v", err)
			}
		}
		if err := w.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
		dataDir := "/buckets/test-bucket/ns/tbl/data"
		fs.putEntry(dataDir, name, &filer_pb.Entry{
			Name:    name,
			Content: buf.Bytes(),
		})
	}

	writeDataFile("file1.parquet", []dataRow{
		{1, "alice"}, {2, "bob"}, {3, "charlie"}, {4, "dave"}, {5, "eve"},
	})
	writeDataFile("file2.parquet", []dataRow{
		{6, "frank"}, {7, "grace"},
	})

	spec := *iceberg.UnpartitionedSpec
	makeEntry := func(path string, size int64) iceberg.ManifestEntry {
		dfb, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentData, path, iceberg.ParquetFile, map[int]any{}, nil, nil, 1, size)
		if err != nil {
			t.Fatalf("build: %v", err)
		}
		snapID := int64(1)
		return iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapID, nil, nil, dfb.Build())
	}

	entries := []iceberg.ManifestEntry{
		makeEntry("data/file1.parquet", 1024),
		makeEntry("data/file2.parquet", 512),
	}

	// Delete rows 1 (bob) and 3 (dave) from file1
	posDeletes := map[string][]int64{
		"data/file1.parquet": {1, 3},
	}

	merged, count, err := mergeParquetFiles(
		context.Background(), client, "test-bucket", "ns/tbl",
		entries, posDeletes, nil, nil, nil,
	)
	if err != nil {
		t.Fatalf("mergeParquetFiles: %v", err)
	}

	// Original: 5 + 2 = 7 rows, deleted 2 = 5 rows
	if count != 5 {
		t.Errorf("expected 5 rows after position deletes, got %d", count)
	}

	// Verify merged output is valid Parquet
	reader := parquet.NewReader(bytes.NewReader(merged))
	defer reader.Close()
	var outputRows []dataRow
	for {
		var r dataRow
		err := reader.Read(&r)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read merged row: %v", err)
		}
		outputRows = append(outputRows, r)
	}

	if len(outputRows) != 5 {
		t.Fatalf("expected 5 output rows, got %d", len(outputRows))
	}

	// Verify bob (id=2) and dave (id=4) are NOT in output
	for _, r := range outputRows {
		if r.ID == 2 || r.ID == 4 {
			t.Errorf("row with id=%d should have been deleted", r.ID)
		}
	}
}

func TestMergeParquetFilesWithEqualityDeletes(t *testing.T) {
	fs, client := startFakeFiler(t)

	type dataRow struct {
		ID   int64  `parquet:"id"`
		Name string `parquet:"name"`
	}

	var buf bytes.Buffer
	w := parquet.NewWriter(&buf, parquet.SchemaOf(new(dataRow)))
	dataRows := []dataRow{
		{1, "alice"}, {2, "bob"}, {3, "charlie"}, {4, "dave"},
	}
	for _, r := range dataRows {
		if err := w.Write(&r); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	dataDir := "/buckets/test-bucket/ns/tbl/data"
	fs.putEntry(dataDir, "data1.parquet", &filer_pb.Entry{
		Name:    "data1.parquet",
		Content: buf.Bytes(),
	})

	spec := *iceberg.UnpartitionedSpec
	dfb, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentData, "data/data1.parquet", iceberg.ParquetFile, map[int]any{}, nil, nil, 4, int64(buf.Len()))
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	snapID := int64(1)
	entries := []iceberg.ManifestEntry{
		iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapID, nil, nil, dfb.Build()),
	}

	// Build equality deletes: delete rows where name="bob" or name="dave"
	// Build the composite key the same way buildEqualityKey does
	eqDeletes := map[string]struct{}{
		"bob":  {},
		"dave": {},
	}

	schema := newTestSchema() // field 1=id, field 2=name
	merged, count, err := mergeParquetFiles(
		context.Background(), client, "test-bucket", "ns/tbl",
		entries, nil, eqDeletes, []int{2}, schema, // field 2 = name
	)
	if err != nil {
		t.Fatalf("mergeParquetFiles: %v", err)
	}

	if count != 2 {
		t.Errorf("expected 2 rows after equality deletes, got %d", count)
	}

	reader := parquet.NewReader(bytes.NewReader(merged))
	defer reader.Close()
	var outputRows []dataRow
	for {
		var r dataRow
		err := reader.Read(&r)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		outputRows = append(outputRows, r)
	}

	for _, r := range outputRows {
		if r.Name == "bob" || r.Name == "dave" {
			t.Errorf("row %q should have been deleted", r.Name)
		}
	}
}

func TestDetectNilRequest(t *testing.T) {
	handler := NewHandler(nil)
	err := handler.Detect(nil, nil, nil)
	if err == nil {
		t.Error("expected error for nil request")
	}
}

func TestExecuteNilRequest(t *testing.T) {
	handler := NewHandler(nil)
	err := handler.Execute(nil, nil, nil)
	if err == nil {
		t.Error("expected error for nil request")
	}
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// buildTestMetadata creates a minimal Iceberg metadata for testing.
// When snapshots is nil or empty, the metadata has no snapshots.
func buildTestMetadata(t *testing.T, snapshots []table.Snapshot) table.Metadata {
	t.Helper()

	schema := newTestSchema()
	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, "s3://test-bucket/test-table", nil)
	if err != nil {
		t.Fatalf("failed to create test metadata: %v", err)
	}

	if len(snapshots) == 0 {
		return meta
	}

	builder, err := table.MetadataBuilderFromBase(meta, "s3://test-bucket/test-table")
	if err != nil {
		t.Fatalf("failed to create metadata builder: %v", err)
	}

	var lastSnapID int64
	for _, snap := range snapshots {
		s := snap // copy
		if err := builder.AddSnapshot(&s); err != nil {
			t.Fatalf("failed to add snapshot %d: %v", snap.SnapshotID, err)
		}
		lastSnapID = snap.SnapshotID
	}

	if err := builder.SetSnapshotRef(table.MainBranch, lastSnapID, table.BranchRef); err != nil {
		t.Fatalf("failed to set snapshot ref: %v", err)
	}

	result, err := builder.Build()
	if err != nil {
		t.Fatalf("failed to build metadata: %v", err)
	}
	return result
}

func newTestSchema() *iceberg.Schema {
	return iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Type: iceberg.PrimitiveTypes.Int64, Name: "id", Required: true},
		iceberg.NestedField{ID: 2, Type: iceberg.PrimitiveTypes.String, Name: "name", Required: false},
	)
}
