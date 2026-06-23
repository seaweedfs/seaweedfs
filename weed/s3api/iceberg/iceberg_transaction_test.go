package iceberg

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

func TestDecodeCommitTransactionRequest(t *testing.T) {
	body := `{"table-changes":[
		{"identifier":{"namespace":["ns"],"name":"t1"},"requirements":[{"type":"assert-table-uuid","uuid":"00000000-0000-0000-0000-000000000001"}],"updates":[{"action":"set-properties","updates":{"k":"v"}}]},
		{"identifier":{"namespace":["ns","sub"],"name":"t2"},"requirements":[],"updates":[]}
	]}`

	var req CommitTransactionRequest
	if err := json.Unmarshal([]byte(body), &req); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if len(req.TableChanges) != 2 {
		t.Fatalf("table-changes = %d, want 2", len(req.TableChanges))
	}
	if req.TableChanges[0].Identifier.Name != "t1" {
		t.Fatalf("change[0] name = %q, want t1", req.TableChanges[0].Identifier.Name)
	}
	if got := []string(req.TableChanges[1].Identifier.Namespace); len(got) != 2 || got[1] != "sub" {
		t.Fatalf("change[1] namespace = %v, want [ns sub]", got)
	}
}

func TestParseTableChangeSeparatesStatistics(t *testing.T) {
	change := tableChangeRequest{
		Requirements: json.RawMessage(`[{"type":"assert-table-uuid","uuid":"00000000-0000-0000-0000-000000000001"}]`),
		Updates: []json.RawMessage{
			json.RawMessage(`{"action":"set-properties","updates":{"k":"v"}}`),
			json.RawMessage(`{"action":"set-statistics","snapshot-id":10,"statistics-path":"s3://bucket/table/metadata/stats.puffin","file-size-in-bytes":100,"file-footer-size-in-bytes":20,"blob-metadata":[]}`),
		},
	}

	requirements, updates, statistics, reqErr := parseTableChange(change)
	if reqErr != nil {
		t.Fatalf("parseTableChange() error = %+v", reqErr)
	}
	if len(requirements) != 1 {
		t.Fatalf("requirements = %d, want 1", len(requirements))
	}
	if len(updates) != 1 {
		t.Fatalf("updates = %d, want 1", len(updates))
	}
	if len(statistics) != 1 || statistics[0].set == nil || statistics[0].set.SnapshotID != 10 {
		t.Fatalf("unexpected statistics updates: %#v", statistics)
	}
}

func TestParseTableChangeRejectsInvalidRequirements(t *testing.T) {
	change := tableChangeRequest{Requirements: json.RawMessage(`{not json}`)}
	_, _, _, reqErr := parseTableChange(change)
	if reqErr == nil {
		t.Fatalf("parseTableChange() expected error for invalid requirements")
	}
	if reqErr.status != 400 {
		t.Fatalf("status = %d, want 400", reqErr.status)
	}
}

func TestCloneTableMetadataIsIndependent(t *testing.T) {
	orig := &s3tables.TableMetadata{
		Iceberg:      &s3tables.IcebergMetadata{TableUUID: "uuid-1"},
		FullMetadata: json.RawMessage(`{"v":1}`),
	}
	clone := cloneTableMetadata(orig)
	if clone == orig || clone.Iceberg == orig.Iceberg {
		t.Fatalf("cloneTableMetadata returned aliased pointers")
	}

	orig.Iceberg.TableUUID = "mutated"
	orig.FullMetadata[0] = 'X'
	if clone.Iceberg.TableUUID != "uuid-1" {
		t.Fatalf("clone Iceberg.TableUUID = %q, want uuid-1", clone.Iceberg.TableUUID)
	}
	if string(clone.FullMetadata) != `{"v":1}` {
		t.Fatalf("clone FullMetadata = %q, mutated with source", clone.FullMetadata)
	}

	if cloneTableMetadata(nil) != nil {
		t.Fatalf("cloneTableMetadata(nil) = non-nil")
	}
}

func TestBuildTableRestoreRequestCarriesPriorState(t *testing.T) {
	pc := &preparedTableCommit{
		namespace:           []string{"ns", "sub"},
		tableName:           "t1",
		prevMetadataLoc:     "s3://bucket/ns/t1/metadata/v3.metadata.json",
		prevMetadataVersion: 3,
		prevMetadata: &s3tables.TableMetadata{
			Iceberg:      &s3tables.IcebergMetadata{TableUUID: uuid.NewString()},
			FullMetadata: json.RawMessage(`{"format-version":2,"last-updated-ms":3}`),
		},
	}

	req := buildTableRestoreRequest("arn:bucket", pc)

	if req.TableBucketARN != "arn:bucket" || req.Name != "t1" {
		t.Fatalf("restore target = %s/%s, want arn:bucket/t1", req.TableBucketARN, req.Name)
	}
	if req.MetadataLocation != pc.prevMetadataLoc {
		t.Fatalf("restore MetadataLocation = %q, want %q", req.MetadataLocation, pc.prevMetadataLoc)
	}
	if req.MetadataVersion != pc.prevMetadataVersion {
		t.Fatalf("restore MetadataVersion = %d, want %d", req.MetadataVersion, pc.prevMetadataVersion)
	}
	if req.Metadata == nil || !bytes.Equal(req.Metadata.FullMetadata, pc.prevMetadata.FullMetadata) {
		t.Fatalf("restore FullMetadata = %q, want %q", req.Metadata.FullMetadata, pc.prevMetadata.FullMetadata)
	}
	if req.Metadata.Iceberg == nil || req.Metadata.Iceberg.TableUUID != pc.prevMetadata.Iceberg.TableUUID {
		t.Fatalf("restore TableUUID mismatch")
	}
}

// TestRollbackRevertsEveryFlippedField mirrors handleUpdateTable's partial-field
// update semantics to prove that after a flip + rollback the table is back to its
// pre-transaction state. With the old location-only rollback the table kept the
// new full metadata and version; the restore request must revert all of them.
func TestRollbackRevertsEveryFlippedField(t *testing.T) {
	// prior on-disk state of an already-committed table.
	prior := tableEntry{
		metadataLocation: "s3://bucket/ns/t1/metadata/v3.metadata.json",
		metadataVersion:  3,
		metadata: &s3tables.TableMetadata{
			Iceberg:      &s3tables.IcebergMetadata{TableUUID: "uuid-1"},
			FullMetadata: json.RawMessage(`{"format-version":2,"v":3}`),
		},
	}

	pc := &preparedTableCommit{
		namespace:           []string{"ns"},
		tableName:           "t1",
		tableUUID:           uuid.MustParse("00000000-0000-0000-0000-000000000001"),
		metadataBytes:       json.RawMessage(`{"format-version":2,"v":4}`),
		metadataVersion:     4,
		newMetadataLoc:      "s3://bucket/ns/t1/metadata/v4.metadata.json",
		prevMetadataLoc:     prior.metadataLocation,
		prevMetadataVersion: prior.metadataVersion,
		prevMetadata:        cloneTableMetadata(prior.metadata),
	}

	// flip: apply the new commit, then roll it back via the restore request.
	state := prior.clone()
	state.applyUpdate(&s3tables.UpdateTableRequest{
		Metadata:         &s3tables.TableMetadata{Iceberg: &s3tables.IcebergMetadata{TableUUID: pc.tableUUID.String()}, FullMetadata: pc.metadataBytes},
		MetadataVersion:  pc.metadataVersion,
		MetadataLocation: pc.newMetadataLoc,
	})
	state.applyUpdate(buildTableRestoreRequest("arn:bucket", pc))

	if state.metadataLocation != prior.metadataLocation {
		t.Fatalf("after rollback location = %q, want %q", state.metadataLocation, prior.metadataLocation)
	}
	if state.metadataVersion != prior.metadataVersion {
		t.Fatalf("after rollback version = %d, want %d", state.metadataVersion, prior.metadataVersion)
	}
	if !bytes.Equal(state.metadata.FullMetadata, prior.metadata.FullMetadata) {
		t.Fatalf("after rollback FullMetadata = %q, want %q", state.metadata.FullMetadata, prior.metadata.FullMetadata)
	}
	if state.metadata.Iceberg.TableUUID != prior.metadata.Iceberg.TableUUID {
		t.Fatalf("after rollback TableUUID = %q, want %q", state.metadata.Iceberg.TableUUID, prior.metadata.Iceberg.TableUUID)
	}
}

// tableEntry models the persisted table fields and the partial-update rules
// handleUpdateTable applies, so rollback can be exercised without a live filer.
type tableEntry struct {
	metadataLocation string
	metadataVersion  int
	metadata         *s3tables.TableMetadata
}

func (e tableEntry) clone() *tableEntry {
	return &tableEntry{
		metadataLocation: e.metadataLocation,
		metadataVersion:  e.metadataVersion,
		metadata:         cloneTableMetadata(e.metadata),
	}
}

func (e *tableEntry) applyUpdate(req *s3tables.UpdateTableRequest) {
	if req.Metadata != nil {
		if e.metadata == nil {
			e.metadata = &s3tables.TableMetadata{}
		}
		if req.Metadata.Iceberg != nil {
			if e.metadata.Iceberg == nil {
				e.metadata.Iceberg = &s3tables.IcebergMetadata{}
			}
			if req.Metadata.Iceberg.TableUUID != "" {
				e.metadata.Iceberg.TableUUID = req.Metadata.Iceberg.TableUUID
			}
		}
		if len(req.Metadata.FullMetadata) > 0 {
			e.metadata.FullMetadata = req.Metadata.FullMetadata
		}
	}
	if req.MetadataLocation != "" {
		e.metadataLocation = req.MetadataLocation
	}
	if req.MetadataVersion > 0 {
		e.metadataVersion = req.MetadataVersion
	}
}
