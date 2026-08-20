package iceberg

import (
	"context"
	"encoding/json"
	"path"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables/s3tablestest"
)

// seedAdapterRegisteredLanceTable builds what the Lance namespace's Iceberg REST
// adapter leaves behind: a real Iceberg table with a placeholder schema and
// table_type=lance, whose directory holds a Lance dataset rather than Iceberg
// data files.
func seedAdapterRegisteredLanceTable(t *testing.T, properties iceberg.Properties) (*s3tablestest.MemFiler, string) {
	t.Helper()
	const bucket, namespace, table = "vectors", "ml", "embeddings"

	filer := s3tablestest.Start(t)
	filer.Put(s3tables.TablesPath, bucket, map[string][]byte{s3tables.ExtendedKeyTableBucket: []byte("{}")})

	nsMeta, err := json.Marshal(map[string]any{"namespace": []string{namespace}})
	if err != nil {
		t.Fatalf("marshal namespace metadata: %v", err)
	}
	filer.Put(s3tables.GetTableBucketPath(bucket), namespace, map[string][]byte{s3tables.ExtendedKeyMetadata: nsMeta})

	full, err := json.Marshal(buildTestMetadata(t, nil, nil, 0, properties))
	if err != nil {
		t.Fatalf("marshal iceberg metadata: %v", err)
	}
	envelope, err := json.Marshal(map[string]any{
		"metadataVersion":  1,
		"metadataLocation": "s3://" + bucket + "/" + namespace + "/" + table + "/metadata/v1.metadata.json",
		"metadata":         map[string]any{"fullMetadata": json.RawMessage(full)},
	})
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	filer.Put(s3tables.GetNamespacePath(bucket, namespace), table,
		map[string][]byte{s3tables.ExtendedKeyMetadata: envelope})

	// The Lance dataset. Its fragments live in data/, the same directory the
	// orphan cleaner walks, and the Iceberg metadata references none of them.
	tablePath := s3tables.GetTablePath(bucket, namespace, table)
	old := time.Now().Add(-30 * 24 * time.Hour)
	filer.Put(tablePath, "data", nil)
	filer.PutFile(path.Join(tablePath, "data"), "01111110101110001101011164cadc43919eace9c608107bd9.lance", old)
	filer.Put(tablePath, "_versions", nil)
	filer.PutFile(path.Join(tablePath, "_versions"), "1.manifest", old)
	filer.Put(tablePath, "metadata", nil)
	filer.PutFile(path.Join(tablePath, "metadata"), "v1.metadata.json", old)

	return filer, path.Join(namespace, table)
}

// The hazard this guards against, stated as a test: every fragment of a Lance
// dataset is unreferenced by the Iceberg metadata sitting beside it, so orphan
// cleanup would delete the whole dataset.
func TestOrphanCleanupWouldDeleteAnAdapterRegisteredLanceDataset(t *testing.T) {
	filer, tablePath := seedAdapterRegisteredLanceTable(t, iceberg.Properties{tableTypeProperty: "lance"})

	meta := buildTestMetadata(t, nil, nil, 0, iceberg.Properties{tableTypeProperty: "lance"})
	candidates, err := collectOrphanCandidates(context.Background(), filer.Client, "vectors", tablePath,
		meta, "v1.metadata.json", defaultOrphanOlderThanHours)
	if err != nil {
		t.Fatalf("collect orphan candidates: %v", err)
	}

	var doomed []string
	for _, c := range candidates {
		doomed = append(doomed, path.Join(c.Dir, c.Entry.Name))
	}
	if len(doomed) == 0 {
		t.Fatal("expected the Lance fragments to look like orphans; without that this guard is pointless")
	}
	for _, name := range doomed {
		if path.Ext(name) == ".lance" {
			return
		}
	}
	t.Fatalf("expected a .lance fragment among the orphan candidates, got %v", doomed)
}

// And the guard that stops it: the scan never reaches the table.
func TestScanSkipsAnAdapterRegisteredLanceTable(t *testing.T) {
	handler := &Handler{}
	config := normalizeDetectionConfig(Config{Operations: "all"})

	filer, _ := seedAdapterRegisteredLanceTable(t, iceberg.Properties{tableTypeProperty: "lance"})
	tables, err := handler.scanTablesForMaintenance(context.Background(), filer.Client, config, "", "", "", 0)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(tables) != 0 {
		t.Fatalf("scan picked up a lance table: %v", tables)
	}

	// An ordinary Iceberg table in the same shape is still scanned, so the guard
	// is not simply skipping everything.
	filer, _ = seedAdapterRegisteredLanceTable(t, nil)
	tables, err = handler.scanTablesForMaintenance(context.Background(), filer.Client, config, "", "", "", 0)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(tables) != 1 {
		t.Fatalf("scan skipped an iceberg table: %v", tables)
	}
}
