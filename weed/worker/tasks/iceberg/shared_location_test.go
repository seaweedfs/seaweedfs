package iceberg

import (
	"context"
	"path"
	"testing"
	"time"

	"github.com/apache/iceberg-go/table"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
	"google.golang.org/protobuf/proto"
)

// Maintenance deletes the files a table's metadata does not reference, so a
// table whose location another record also points at is not maintained.
func TestDetectSkipsSharedLocation(t *testing.T) {
	fs, client := startFakeFiler(t)
	now := time.Now().UnixMilli()
	setup := tableSetup{
		BucketName: "test-bucket",
		Namespace:  "analytics",
		TableName:  "events",
		DataPath:   "analytics/events",
		Snapshots: []table.Snapshot{
			{SnapshotID: 1, TimestampMs: now, ManifestList: "metadata/snap-1.avro"},
			{SnapshotID: 2, TimestampMs: now + 1, ManifestList: "metadata/snap-2.avro"},
			{SnapshotID: 3, TimestampMs: now + 2, ManifestList: "metadata/snap-3.avro"},
		},
		Age: 200 * time.Hour,
	}
	populateTable(t, fs, setup)
	config := Config{SnapshotRetentionMs: hoursToMs(0), MaxSnapshotsToKeep: 2, MaxCommitRetries: 3}

	tables, err := NewHandler(nil).scanTablesForMaintenance(context.Background(), client, config, "", "", "", 0)
	if err != nil || len(tables) != 1 {
		t.Fatalf("before the alias: tables = %d, err = %v, want 1 table", len(tables), err)
	}

	nsPath := path.Join(s3tables.TablesPath, setup.BucketName, setup.Namespace)
	alias := proto.Clone(fs.getEntry(nsPath, setup.TableName)).(*filer_pb.Entry)
	alias.Name = "alias"
	fs.putEntry(nsPath, alias.Name, alias)

	tables, err = NewHandler(nil).scanTablesForMaintenance(context.Background(), client, config, "", "", "", 0)
	if err != nil {
		t.Fatalf("scanTablesForMaintenance: %v", err)
	}
	if len(tables) != 0 {
		t.Fatalf("tables sharing one location were scheduled: %+v", tables)
	}
}

// A table created over a renamed table's old name sits on that table's files
// but keeps its own elsewhere, so the renamed table is still maintained.
func TestFilesSharedIgnoresAnEntryOverTheRoot(t *testing.T) {
	roots := map[string][]string{
		"/buckets/b/ns2/moved": {"/buckets/b/ns/old"},
		"/buckets/b/ns/old":    {"/buckets/b/ns/old-1"},
	}
	if filesShared(roots, "/buckets/b/ns2/moved", "/buckets/b/ns/old") {
		t.Error("an entry over the root counted as sharing its files")
	}
	if !filesShared(roots, "/buckets/b/ns/other", "/buckets/b/ns/old/data") {
		t.Error("a root inside another record's files was not shared")
	}
}

// A job scheduled before another record came to share the table's location
// leaves the files alone when it runs.
func TestRemoveOrphansSkipsSharedLocation(t *testing.T) {
	fs, client := startFakeFiler(t)
	setup := tableSetup{
		BucketName: "test-bucket",
		Namespace:  "analytics",
		TableName:  "events",
		DataPath:   "analytics/events",
		Snapshots:  []table.Snapshot{{SnapshotID: 1, TimestampMs: time.Now().UnixMilli(), ManifestList: "metadata/snap-1.avro"}},
	}
	populateTable(t, fs, setup)
	dataDir := path.Join(s3tables.TablesPath, setup.BucketName, setup.tablePath(), "data")
	fs.putEntry(dataDir, "other.parquet", &filer_pb.Entry{
		Name:       "other.parquet",
		Attributes: &filer_pb.FuseAttributes{Mtime: time.Now().Add(-200 * time.Hour).Unix()},
	})
	nsPath := path.Join(s3tables.TablesPath, setup.BucketName, setup.Namespace)
	alias := proto.Clone(fs.getEntry(nsPath, setup.TableName)).(*filer_pb.Entry)
	alias.Name = "alias"
	fs.putEntry(nsPath, alias.Name, alias)

	result, _, err := NewHandler(nil).removeOrphans(context.Background(), client, setup.BucketName, setup.tablePath(), Config{OrphanOlderThanHours: 72, MaxCommitRetries: 3})
	if err != nil {
		t.Fatalf("removeOrphans: %v", err)
	}
	if fs.getEntry(dataDir, "other.parquet") == nil {
		t.Fatalf("a file under a shared location was removed (%q)", result)
	}
}
