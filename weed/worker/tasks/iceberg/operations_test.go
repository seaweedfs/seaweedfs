package iceberg

import (
	"bytes"
	"context"
	"fmt"
	"path"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables/s3tablestest"
)

// Merging manifests a foreign producer wrote used to fail for good on a
// day-partitioned table: the entries carry the time.Time the Avro decoder
// produced, and the manifest writer has no way to encode that as a day.
func TestRewriteManifestsNormalizesForeignDayPartitions(t *testing.T) {
	fs, client := startFakeFiler(t)

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "event_time", Type: iceberg.PrimitiveTypes.Timestamp, Required: true},
	)
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "event_time_day", Transform: iceberg.DayTransform{},
	})
	setup := tableSetup{
		BucketName: "test-bucket",
		Namespace:  "analytics",
		TableName:  "events",
		Schema:     schema,
		Spec:       &spec,
		Snapshots: []table.Snapshot{{
			SnapshotID:   1,
			TimestampMs:  time.Now().UnixMilli(),
			ManifestList: "metadata/snap-1.avro",
		}},
	}
	meta := populateTable(t, fs, setup)

	metaDir := path.Join(s3tables.TablesPath, setup.BucketName, setup.tablePath(), "metadata")
	wantDays := make(map[string]iceberg.Date)
	var manifests []iceberg.ManifestFile
	for i := 0; i < 3; i++ {
		day := iceberg.Date(20737 + i)
		filePath := setup.fileRef("data", fmt.Sprintf("foreign-%d.parquet", i))
		dfBuilder, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentData, filePath, iceberg.ParquetFile,
			map[int]any{1000: day}, nil, nil, 1, 1)
		if err != nil {
			t.Fatalf("build data file %d: %v", i, err)
		}
		snapshotID := int64(1)
		entry := iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapshotID, nil, nil, dfBuilder.Build())

		manifestName := fmt.Sprintf("foreign-manifest-%d.avro", i)
		foreignBytes, manifest := s3tablestest.ForeignPartitionManifest(t, schema, spec, entry,
			setup.fileRef("metadata", manifestName), "date", time.Unix(int64(day)*24*60*60, 0).UTC())
		fs.putEntry(metaDir, manifestName, &filer_pb.Entry{
			Name: manifestName, Attributes: &filer_pb.FuseAttributes{Mtime: time.Now().Unix()}, Content: foreignBytes,
		})
		manifests = append(manifests, manifest)
		wantDays[filePath] = day
	}

	var manifestList bytes.Buffer
	seqNum := int64(1)
	if err := iceberg.WriteManifestList(meta.Version(), &manifestList, 1, nil, &seqNum, 0, manifests); err != nil {
		t.Fatalf("write manifest list: %v", err)
	}
	fs.putEntry(metaDir, "snap-1.avro", &filer_pb.Entry{
		Name: "snap-1.avro", Attributes: &filer_pb.FuseAttributes{Mtime: time.Now().Unix()}, Content: manifestList.Bytes(),
	})

	result, _, err := NewHandler(nil).rewriteManifests(context.Background(), client, setup.BucketName, setup.tablePath(), Config{
		MinManifestsToRewrite: 3,
		MaxCommitRetries:      3,
	})
	if err != nil {
		t.Fatalf("rewriteManifests failed for foreign day partitions: %v", err)
	}
	if result != "rewrote 3 manifests into 1 (3 entries)" {
		t.Fatalf("rewriteManifests result = %q, want 3 manifests merged", result)
	}

	// The merged manifest has to carry the days the foreign manifests held.
	// iceberg-go writes a day partition as a bare Avro int, without the date
	// logical type, so it reads back as int32 rather than iceberg.Date.
	got := make(map[string]any)
	for _, mf := range currentManifests(t, client, setup) {
		manifestData, err := loadFileByIcebergPath(context.Background(), client, setup.BucketName, setup.tablePath(), mf.FilePath())
		if err != nil {
			t.Fatalf("load merged manifest %s: %v", mf.FilePath(), err)
		}
		entries, err := iceberg.ReadManifest(mf, bytes.NewReader(manifestData), true)
		if err != nil {
			t.Fatalf("parse merged manifest %s: %v", mf.FilePath(), err)
		}
		for _, entry := range entries {
			got[entry.DataFile().FilePath()] = entry.DataFile().Partition()[1000]
		}
	}
	if len(got) != len(wantDays) {
		t.Fatalf("merged manifests hold %d entries, want %d", len(got), len(wantDays))
	}
	for filePath, day := range wantDays {
		if got[filePath] != int32(day) {
			t.Errorf("%s partition = %#v (%T), want %d", filePath, got[filePath], got[filePath], day)
		}
	}
}

// currentManifests reads the manifests of the table's current snapshot.
func currentManifests(t *testing.T, client filer_pb.SeaweedFilerClient, setup tableSetup) []iceberg.ManifestFile {
	t.Helper()
	state, err := loadCurrentMetadata(context.Background(), client, setup.BucketName, setup.tablePath())
	if err != nil {
		t.Fatalf("reload metadata: %v", err)
	}
	snapshot := state.Metadata.CurrentSnapshot()
	if snapshot == nil {
		t.Fatal("table has no current snapshot")
	}
	manifestListData, err := loadFileByIcebergPath(context.Background(), client, setup.BucketName, setup.tablePath(), snapshot.ManifestList)
	if err != nil {
		t.Fatalf("load manifest list: %v", err)
	}
	manifests, err := s3tables.ReadManifestList(manifestListData)
	if err != nil {
		t.Fatalf("parse manifest list: %v", err)
	}
	return manifests
}
