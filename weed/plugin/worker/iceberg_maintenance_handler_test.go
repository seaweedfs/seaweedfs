package pluginworker

import (
	"bytes"
	"fmt"
	"path"
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
		wantErr  bool
	}{
		{"all", []string{"expire_snapshots", "remove_orphans", "rewrite_manifests"}, false},
		{"", []string{"expire_snapshots", "remove_orphans", "rewrite_manifests"}, false},
		{"expire_snapshots", []string{"expire_snapshots"}, false},
		{"rewrite_manifests,expire_snapshots", []string{"expire_snapshots", "rewrite_manifests"}, false},
		{"remove_orphans, rewrite_manifests", []string{"remove_orphans", "rewrite_manifests"}, false},
		{"expire_snapshots,remove_orphans,rewrite_manifests", []string{"expire_snapshots", "remove_orphans", "rewrite_manifests"}, false},
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
