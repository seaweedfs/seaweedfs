package iceberg

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

type partitionedTestFile struct {
	Name      string
	Partition map[int]any
	Rows      []struct {
		ID   int64
		Name string
	}
}

func populatePartitionedDataTable(
	t *testing.T,
	fs *fakeFilerServer,
	setup tableSetup,
	partitionSpec iceberg.PartitionSpec,
	manifestGroups [][]partitionedTestFile,
) table.Metadata {
	t.Helper()

	schema := newTestSchema()
	meta, err := table.NewMetadata(schema, &partitionSpec, table.UnsortedSortOrder, "s3://"+setup.BucketName+"/"+setup.tablePath(), nil)
	if err != nil {
		t.Fatalf("create metadata: %v", err)
	}

	bucketsPath := s3tables.TablesPath
	bucketPath := path.Join(bucketsPath, setup.BucketName)
	nsPath := path.Join(bucketPath, setup.Namespace)
	tablePath := path.Join(nsPath, setup.TableName)
	metaDir := path.Join(tablePath, "metadata")
	dataDir := path.Join(tablePath, "data")

	version := meta.Version()
	var manifestFiles []iceberg.ManifestFile
	for idx, group := range manifestGroups {
		entries := make([]iceberg.ManifestEntry, 0, len(group))
		for _, file := range group {
			data := writeTestParquetFile(t, fs, dataDir, file.Name, file.Rows)
			dfBuilder, err := iceberg.NewDataFileBuilder(
				partitionSpec,
				iceberg.EntryContentData,
				path.Join("data", file.Name),
				iceberg.ParquetFile,
				file.Partition,
				nil, nil,
				int64(len(file.Rows)),
				int64(len(data)),
			)
			if err != nil {
				t.Fatalf("build data file %s: %v", file.Name, err)
			}
			snapID := int64(1)
			entries = append(entries, iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapID, nil, nil, dfBuilder.Build()))
		}

		manifestName := fmt.Sprintf("where-manifest-%d.avro", idx+1)
		var manifestBuf bytes.Buffer
		mf, err := iceberg.WriteManifest(path.Join("metadata", manifestName), &manifestBuf, version, partitionSpec, schema, 1, entries)
		if err != nil {
			t.Fatalf("write manifest %d: %v", idx+1, err)
		}
		fs.putEntry(metaDir, manifestName, &filer_pb.Entry{
			Name:       manifestName,
			Content:    manifestBuf.Bytes(),
			Attributes: &filer_pb.FuseAttributes{Mtime: time.Now().Unix(), FileSize: uint64(manifestBuf.Len())},
		})
		manifestFiles = append(manifestFiles, mf)
	}

	var manifestListBuf bytes.Buffer
	seqNum := int64(1)
	if err := iceberg.WriteManifestList(version, &manifestListBuf, 1, nil, &seqNum, 0, manifestFiles); err != nil {
		t.Fatalf("write manifest list: %v", err)
	}
	fs.putEntry(metaDir, "snap-1.avro", &filer_pb.Entry{
		Name:       "snap-1.avro",
		Content:    manifestListBuf.Bytes(),
		Attributes: &filer_pb.FuseAttributes{Mtime: time.Now().Unix(), FileSize: uint64(manifestListBuf.Len())},
	})

	builder, err := table.MetadataBuilderFromBase(meta, "s3://"+setup.BucketName+"/"+setup.tablePath())
	if err != nil {
		t.Fatalf("metadata builder: %v", err)
	}
	snapshot := table.Snapshot{SnapshotID: 1, TimestampMs: time.Now().UnixMilli(), ManifestList: "metadata/snap-1.avro", SequenceNumber: 1}
	if err := builder.AddSnapshot(&snapshot); err != nil {
		t.Fatalf("add snapshot: %v", err)
	}
	if err := builder.SetSnapshotRef(table.MainBranch, 1, table.BranchRef); err != nil {
		t.Fatalf("set snapshot ref: %v", err)
	}
	meta, err = builder.Build()
	if err != nil {
		t.Fatalf("build metadata: %v", err)
	}

	fullMetadataJSON, _ := json.Marshal(meta)
	internalMeta := map[string]interface{}{
		"metadataVersion":  1,
		"metadataLocation": "metadata/v1.metadata.json",
		"metadata":         map[string]interface{}{"fullMetadata": json.RawMessage(fullMetadataJSON)},
	}
	xattr, _ := json.Marshal(internalMeta)

	fs.putEntry(bucketsPath, setup.BucketName, &filer_pb.Entry{
		Name:        setup.BucketName,
		IsDirectory: true,
		Extended:    map[string][]byte{s3tables.ExtendedKeyTableBucket: []byte("true")},
	})
	fs.putEntry(bucketPath, setup.Namespace, &filer_pb.Entry{Name: setup.Namespace, IsDirectory: true})
	fs.putEntry(nsPath, setup.TableName, &filer_pb.Entry{
		Name:        setup.TableName,
		IsDirectory: true,
		Extended: map[string][]byte{
			s3tables.ExtendedKeyMetadata:        xattr,
			s3tables.ExtendedKeyMetadataVersion: metadataVersionXattr(1),
		},
	})

	return meta
}

func TestValidateWhereOperations(t *testing.T) {
	if err := validateWhereOperations("name = 'us'", []string{"compact", "rewrite_manifests"}); err != nil {
		t.Fatalf("unexpected validation error: %v", err)
	}
	if err := validateWhereOperations("name = 'us'", []string{"expire_snapshots"}); err == nil {
		t.Fatal("expected where validation to reject expire_snapshots")
	}
}

func TestSplitWhereConjunctionQuoteAware(t *testing.T) {
	cases := []struct {
		input    string
		expected []string
	}{
		{"a = 1 AND b = 2", []string{"a = 1", "b = 2"}},
		{"a = 'research AND dev'", []string{"a = 'research AND dev'"}},
		{"a IN ('sales AND marketing', 'eng') AND b = 2", []string{"a IN ('sales AND marketing', 'eng')", "b = 2"}},
		{"a = 1 and b = 2", []string{"a = 1", "b = 2"}},
		{"a = 'x' AND b = \"y AND z\"", []string{"a = 'x'", "b = \"y AND z\""}},
	}
	for _, tc := range cases {
		got := splitWhereConjunction(tc.input)
		if len(got) != len(tc.expected) {
			t.Errorf("splitWhereConjunction(%q) = %v, want %v", tc.input, got, tc.expected)
			continue
		}
		for i := range got {
			if got[i] != tc.expected[i] {
				t.Errorf("splitWhereConjunction(%q)[%d] = %q, want %q", tc.input, i, got[i], tc.expected[i])
			}
		}
	}
}

func TestPartitionPredicateMatchesUsesPartitionFieldIDs(t *testing.T) {
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceID:  2,
		FieldID:   1000,
		Name:      "name",
		Transform: iceberg.IdentityTransform{},
	})
	predicate := &partitionPredicate{Clauses: []whereClause{{Field: "name", Literals: []string{"'us'"}}}}

	match, err := predicate.Matches(spec, map[int]any{2: "us"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if match {
		t.Fatal("expected source-column key to not match partition predicate")
	}
}

func TestCompactDataFilesWhereFilter(t *testing.T) {
	fs, client := startFakeFiler(t)

	partitionSpec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceID:  2,
		FieldID:   1000,
		Name:      "name",
		Transform: iceberg.IdentityTransform{},
	})

	setup := tableSetup{BucketName: "tb", Namespace: "ns", TableName: "tbl"}
	populatePartitionedDataTable(t, fs, setup, partitionSpec, [][]partitionedTestFile{
		{
			{Name: "us-1.parquet", Partition: map[int]any{1000: "us"}, Rows: []struct {
				ID   int64
				Name string
			}{{1, "us"}}},
		},
		{
			{Name: "us-2.parquet", Partition: map[int]any{1000: "us"}, Rows: []struct {
				ID   int64
				Name string
			}{{2, "us"}}},
		},
		{
			{Name: "eu-1.parquet", Partition: map[int]any{1000: "eu"}, Rows: []struct {
				ID   int64
				Name string
			}{{3, "eu"}}},
			{Name: "eu-2.parquet", Partition: map[int]any{1000: "eu"}, Rows: []struct {
				ID   int64
				Name string
			}{{4, "eu"}}},
		},
	})

	handler := NewHandler(nil)
	config := Config{
		TargetFileSizeBytes: 256 * 1024 * 1024,
		MinInputFiles:       2,
		MaxCommitRetries:    3,
		Where:               "name = 'us'",
	}

	result, _, err := handler.compactDataFiles(context.Background(), client, setup.BucketName, setup.tablePath(), config, nil)
	if err != nil {
		t.Fatalf("compactDataFiles: %v", err)
	}
	if !strings.Contains(result, "compacted 2 files into 1") {
		t.Fatalf("unexpected result: %q", result)
	}

	meta, _, err := loadCurrentMetadata(context.Background(), client, setup.BucketName, setup.tablePath())
	if err != nil {
		t.Fatalf("loadCurrentMetadata: %v", err)
	}
	manifests, err := loadCurrentManifests(context.Background(), client, setup.BucketName, setup.tablePath(), meta)
	if err != nil {
		t.Fatalf("loadCurrentManifests: %v", err)
	}

	var liveDataPaths []string
	for _, mf := range manifests {
		if mf.ManifestContent() != iceberg.ManifestContentData {
			continue
		}
		manifestData, err := loadFileByIcebergPath(context.Background(), client, setup.BucketName, setup.tablePath(), mf.FilePath())
		if err != nil {
			t.Fatalf("load data manifest: %v", err)
		}
		entries, err := iceberg.ReadManifest(mf, bytes.NewReader(manifestData), true)
		if err != nil {
			t.Fatalf("read data manifest: %v", err)
		}
		for _, entry := range entries {
			liveDataPaths = append(liveDataPaths, entry.DataFile().FilePath())
		}
	}

	if len(liveDataPaths) != 3 {
		t.Fatalf("expected 3 live data files after filtered compaction, got %v", liveDataPaths)
	}
	var compactedCount int
	for _, p := range liveDataPaths {
		switch {
		case strings.HasPrefix(p, "data/compact-"):
			compactedCount++
		case p == "data/eu-1.parquet", p == "data/eu-2.parquet":
		default:
			t.Fatalf("unexpected live data file %q", p)
		}
	}
	if compactedCount != 1 {
		t.Fatalf("expected exactly one compacted file, got %d in %v", compactedCount, liveDataPaths)
	}
}
