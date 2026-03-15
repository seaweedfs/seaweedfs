package iceberg

import (
	"bytes"
	"fmt"
	"path"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
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
}

func makeTestEntries(t *testing.T, specs []testEntrySpec) []iceberg.ManifestEntry {
	t.Helper()
	entries := make([]iceberg.ManifestEntry, 0, len(specs))
	for _, spec := range specs {
		dfBuilder, err := iceberg.NewDataFileBuilder(
			*iceberg.UnpartitionedSpec,
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
