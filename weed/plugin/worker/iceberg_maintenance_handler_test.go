package pluginworker

import (
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
)

func TestParseIcebergMaintenanceConfig(t *testing.T) {
	config := parseIcebergMaintenanceConfig(nil)

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
	}{
		{"all", []string{"compact", "expire_snapshots", "remove_orphans", "rewrite_manifests"}},
		{"", []string{"compact", "expire_snapshots", "remove_orphans", "rewrite_manifests"}},
		{"expire_snapshots", []string{"expire_snapshots"}},
		{"compact", []string{"compact"}},
		{"rewrite_manifests,expire_snapshots", []string{"expire_snapshots", "rewrite_manifests"}},
		{"compact,expire_snapshots", []string{"compact", "expire_snapshots"}},
		{"remove_orphans, rewrite_manifests", []string{"remove_orphans", "rewrite_manifests"}},
		{"expire_snapshots,remove_orphans,rewrite_manifests", []string{"expire_snapshots", "remove_orphans", "rewrite_manifests"}},
		{"compact,expire_snapshots,remove_orphans,rewrite_manifests", []string{"compact", "expire_snapshots", "remove_orphans", "rewrite_manifests"}},
		{"unknown_op", nil},
	}

	for _, tc := range tests {
		result := parseOperations(tc.input)
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
	}

	for _, tc := range tests {
		result := extractMetadataVersion(tc.input)
		if result != tc.expected {
			t.Errorf("extractMetadataVersion(%q) = %d, want %d", tc.input, result, tc.expected)
		}
	}
}

func TestNeedsMaintenanceNoSnapshots(t *testing.T) {
	config := icebergMaintenanceConfig{
		SnapshotRetentionHours: 24,
		MaxSnapshotsToKeep:     2,
	}

	meta := buildTestMetadata(t, nil)
	if needsMaintenance(meta, config) {
		t.Error("expected no maintenance for table with no snapshots")
	}
}

func TestNeedsMaintenanceExceedsMaxSnapshots(t *testing.T) {
	config := icebergMaintenanceConfig{
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
	config := icebergMaintenanceConfig{
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
	config := icebergMaintenanceConfig{
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
	handler := NewIcebergMaintenanceHandler(nil)

	cap := handler.Capability()
	if cap.JobType != icebergMaintenanceJobType {
		t.Errorf("expected job type %q, got %q", icebergMaintenanceJobType, cap.JobType)
	}
	if !cap.CanDetect {
		t.Error("expected CanDetect=true")
	}
	if !cap.CanExecute {
		t.Error("expected CanExecute=true")
	}

	desc := handler.Descriptor()
	if desc.JobType != icebergMaintenanceJobType {
		t.Errorf("expected job type %q, got %q", icebergMaintenanceJobType, desc.JobType)
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
	handler := NewIcebergMaintenanceHandler(nil)

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
	if proposal.JobType != icebergMaintenanceJobType {
		t.Errorf("expected job type %q, got %q", icebergMaintenanceJobType, proposal.JobType)
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

func TestPartitionKey(t *testing.T) {
	tests := []struct {
		name      string
		partition map[int]any
		expected  string
	}{
		{"empty partition", map[int]any{}, "__unpartitioned__"},
		{"nil partition", nil, "__unpartitioned__"},
		{"single field", map[int]any{1: "us-east"}, "1=us-east"},
		{"multiple fields sorted", map[int]any{3: "2024", 1: "us-east"}, "1=us-east,3=2024"},
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
	handler := NewIcebergMaintenanceHandler(nil)
	err := handler.Detect(nil, nil, nil)
	if err == nil {
		t.Error("expected error for nil request")
	}
}

func TestExecuteNilRequest(t *testing.T) {
	handler := NewIcebergMaintenanceHandler(nil)
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
