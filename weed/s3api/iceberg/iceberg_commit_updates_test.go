package iceberg

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/apache/iceberg-go/table"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

func TestParseCommitUpdatesSeparatesStatistics(t *testing.T) {
	raw := []json.RawMessage{
		json.RawMessage(`{"action":"set-statistics","snapshot-id":10,"statistics-path":"s3://bucket/table/metadata/stats.puffin","file-size-in-bytes":100,"file-footer-size-in-bytes":20,"blob-metadata":[]}`),
		json.RawMessage(`{"action":"set-properties","updates":{"k":"v"}}`),
	}

	updates, stats, err := parseCommitUpdates(raw)
	if err != nil {
		t.Fatalf("parseCommitUpdates() error = %v", err)
	}
	if len(stats) != 1 {
		t.Fatalf("statistics updates = %d, want 1", len(stats))
	}
	if stats[0].set == nil || stats[0].set.SnapshotID != 10 {
		t.Fatalf("unexpected statistics update: %#v", stats[0])
	}
	if len(updates) != 1 {
		t.Fatalf("decoded updates = %d, want 1", len(updates))
	}
}

func TestParseCommitUpdatesRejectsIncompleteSetStatistics(t *testing.T) {
	raw := []json.RawMessage{
		json.RawMessage(`{"action":"set-statistics","snapshot-id":10}`),
	}

	_, _, err := parseCommitUpdates(raw)
	if err == nil {
		t.Fatalf("parseCommitUpdates() expected error")
	}
	if !errors.Is(err, ErrIncompleteSetStatistics) {
		t.Fatalf("parseCommitUpdates() error = %v, want ErrIncompleteSetStatistics", err)
	}
}

func TestApplyStatisticsUpdatesUpsertAndRemove(t *testing.T) {
	metadata := []byte(`{"format-version":2,"statistics":[{"snapshot-id":1,"statistics-path":"s3://bucket/stats-1.puffin","file-size-in-bytes":10,"file-footer-size-in-bytes":1,"blob-metadata":[]},{"snapshot-id":2,"statistics-path":"s3://bucket/stats-2.puffin","file-size-in-bytes":20,"file-footer-size-in-bytes":2,"blob-metadata":[]}]} `)

	snapshotID := int64(2)
	setUpdates := []statisticsUpdate{
		{
			set: &statisticsFileForTest,
		},
		{
			remove: &snapshotID,
		},
	}

	updated, err := applyStatisticsUpdates(metadata, setUpdates)
	if err != nil {
		t.Fatalf("applyStatisticsUpdates() error = %v", err)
	}

	var decoded map[string]json.RawMessage
	if err := json.Unmarshal(updated, &decoded); err != nil {
		t.Fatalf("json.Unmarshal(updated) error = %v", err)
	}

	var stats []map[string]any
	if err := json.Unmarshal(decoded["statistics"], &stats); err != nil {
		t.Fatalf("json.Unmarshal(statistics) error = %v", err)
	}
	if len(stats) != 1 {
		t.Fatalf("statistics length = %d, want 1", len(stats))
	}
	if got := int64(stats[0]["snapshot-id"].(float64)); got != 1 {
		t.Fatalf("remaining snapshot-id = %d, want 1", got)
	}
	if got := int64(stats[0]["file-size-in-bytes"].(float64)); got != 11 {
		t.Fatalf("remaining file-size-in-bytes = %d, want 11", got)
	}
}

var statisticsFileForTest = table.StatisticsFile{
	SnapshotID:            1,
	StatisticsPath:        "s3://bucket/stats-1.puffin",
	FileSizeInBytes:       11,
	FileFooterSizeInBytes: 2,
	BlobMetadata:          []table.BlobMetadata{},
}

func TestIsS3TablesConflict(t *testing.T) {
	if !isS3TablesConflict(s3tables.ErrVersionTokenMismatch) {
		t.Fatalf("expected ErrVersionTokenMismatch to be conflict")
	}
	if !isS3TablesConflict(&s3tables.S3TablesError{Type: s3tables.ErrCodeConflict, Message: "Version token mismatch"}) {
		t.Fatalf("expected S3Tables conflict error to be conflict")
	}
	if isS3TablesConflict(errors.New("other")) {
		t.Fatalf("unexpected conflict for non-conflict error")
	}
}
