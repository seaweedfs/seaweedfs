package iceberg

import (
	"bytes"
	"testing"

	"github.com/apache/iceberg-go"
)

// The v3 manifest list assigns first_row_id to manifests that have
// none, starting at the snapshot's first-row-id, and reports the range
// it assigned as the snapshot's added-rows.
func TestWriteManifestListV3RowIDs(t *testing.T) {
	schema := newTestSchema()
	spec := *iceberg.UnpartitionedSpec
	snapID := int64(12)
	dataSeq := int64(3)

	entry := iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapID, &dataSeq, nil,
		mustDataFile(t, spec, "data/a.parquet", 4))
	var manifestBuf bytes.Buffer
	mf, err := iceberg.WriteManifest("metadata/m0.avro", &manifestBuf, 3, spec, schema, snapID,
		[]iceberg.ManifestEntry{entry})
	if err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	var listBuf bytes.Buffer
	parent := int64(10)
	seqNum := int64(3)
	addedRows, err := writeManifestList(3, &listBuf, snapID, &parent, &seqNum, 500, []iceberg.ManifestFile{mf})
	if err != nil {
		t.Fatalf("writeManifestList: %v", err)
	}
	if addedRows == nil || *addedRows != 4 {
		t.Fatalf("addedRows = %v, want 4", addedRows)
	}

	readBack, err := iceberg.ReadManifestList(&listBuf)
	if err != nil {
		t.Fatalf("read manifest list: %v", err)
	}
	if len(readBack) != 1 || readBack[0].FirstRowID() == nil || *readBack[0].FirstRowID() != 500 {
		t.Fatalf("manifest first_row_id = %v, want 500", readBack)
	}
}

func mustDataFile(t *testing.T, spec iceberg.PartitionSpec, path string, recordCount int64) iceberg.DataFile {
	t.Helper()
	dfb, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentData, path, iceberg.ParquetFile,
		map[int]any{}, nil, nil, recordCount, 1024)
	if err != nil {
		t.Fatalf("build data file: %v", err)
	}
	return dfb.Build()
}

// A plan that assigned row IDs from a next-row-id the table no longer
// has is stale, like a moved head; so is a format version that changed
// since planning.
func TestCheckRowLineagePlan(t *testing.T) {
	if err := checkRowLineagePlan(3, 2, 0, 0); err == nil {
		t.Error("version change must be stale")
	}
	if err := checkRowLineagePlan(3, 3, 100, 200); err == nil {
		t.Error("moved next-row-id must be stale")
	}
	if err := checkRowLineagePlan(2, 2, 0, 0); err != nil {
		t.Errorf("v2 plan is never row-lineage stale: %v", err)
	}
	if err := checkRowLineagePlan(3, 3, 100, 100); err != nil {
		t.Errorf("unchanged next-row-id must not be stale: %v", err)
	}
}

