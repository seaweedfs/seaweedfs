package iceberg

import (
	"context"
	"encoding/json"
	"path"
	"testing"
	"time"

	"github.com/apache/iceberg-go/table"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables/s3tablestest"
)

// Statistics files (Puffin, partition statistics) live under metadata/ and are
// referenced from the table metadata's "statistics" and "partition-statistics"
// lists, not from any snapshot's manifests. Orphan cleanup must count them as
// referenced, or it deletes a table's statistics once they pass the window.
func TestOrphanCleanupKeepsStatisticsFiles(t *testing.T) {
	const bucket, namespace, tbl = "stats-bucket", "ns", "tbl"
	tableRef := func(name string) string {
		return "s3://" + bucket + "/" + namespace + "/" + tbl + "/metadata/" + name
	}

	base := buildTestMetadata(t, nil, nil, 0, nil, nil, nil)
	raw, err := json.Marshal(base)
	if err != nil {
		t.Fatalf("marshal metadata: %v", err)
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("unmarshal metadata: %v", err)
	}
	doc["statistics"] = []any{map[string]any{
		"snapshot-id": 1, "statistics-path": tableRef("stats-1.puffin"),
		"file-size-in-bytes": 10, "file-footer-size-in-bytes": 5, "blob-metadata": []any{},
	}}
	doc["partition-statistics"] = []any{map[string]any{
		"snapshot-id": 1, "statistics-path": tableRef("partition-stats-1.parquet"),
		"file-size-in-bytes": 10,
	}}
	raw, err = json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal metadata with statistics: %v", err)
	}
	meta, err := table.ParseMetadataBytes(raw)
	if err != nil {
		t.Fatalf("parse metadata with statistics: %v", err)
	}

	filer := s3tablestest.Start(t)
	tablePath := s3tables.GetTablePath(bucket, namespace, tbl)
	metaDir := path.Join(tablePath, "metadata")
	old := time.Now().Add(-30 * 24 * time.Hour)
	filer.Put(tablePath, "metadata", nil)
	filer.PutFile(metaDir, "v1.metadata.json", old)
	filer.PutFile(metaDir, "stats-1.puffin", old)
	filer.PutFile(metaDir, "partition-stats-1.parquet", old)
	filer.PutFile(metaDir, "stats-0.puffin", old) // superseded: a real orphan

	candidates, err := collectOrphanCandidates(context.Background(), filer.Client, bucket, path.Join(namespace, tbl),
		meta, "v1.metadata.json", 1)
	if err != nil {
		t.Fatalf("collect orphan candidates: %v", err)
	}
	doomed := map[string]bool{}
	for _, c := range candidates {
		doomed[c.Entry.Name] = true
	}
	if !doomed["stats-0.puffin"] {
		t.Fatalf("expected the unreferenced stats-0.puffin to be an orphan, got %v", doomed)
	}
	for _, name := range []string{"stats-1.puffin", "partition-stats-1.parquet", "v1.metadata.json"} {
		if doomed[name] {
			t.Errorf("%s is referenced by the table metadata and must not be an orphan", name)
		}
	}
}
