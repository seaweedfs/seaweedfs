package iceberg

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
)

// metadataWithSnapshots builds metadata whose main branch is on the last
// snapshot, optionally tagging one of the earlier ones.
func metadataWithSnapshots(t *testing.T, ids []int64, tagged int64) table.Metadata {
	t.Helper()

	base, err := newTableMetadata(uuid.New(), "s3://bkt/ns/t", nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("newTableMetadata() error = %v", err)
	}
	builder, err := table.MetadataBuilderFromBase(base, "")
	if err != nil {
		t.Fatalf("MetadataBuilderFromBase() error = %v", err)
	}

	now := time.Now().UnixMilli()
	for i, id := range ids {
		snapshot := table.Snapshot{
			SnapshotID:   id,
			TimestampMs:  now + int64(i),
			ManifestList: "metadata/snap-" + string(rune('0'+i)) + ".avro",
		}
		if err := builder.AddSnapshot(&snapshot); err != nil {
			t.Fatalf("AddSnapshot(%d) error = %v", id, err)
		}
	}
	if err := builder.SetSnapshotRef(table.MainBranch, ids[len(ids)-1], table.BranchRef); err != nil {
		t.Fatalf("SetSnapshotRef(main) error = %v", err)
	}
	if tagged != 0 {
		if err := builder.SetSnapshotRef("release", tagged, table.TagRef); err != nil {
			t.Fatalf("SetSnapshotRef(release) error = %v", err)
		}
	}

	metadata, err := builder.Build()
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	return metadata
}

func snapshotIDs(metadata table.Metadata) map[int64]bool {
	ids := map[int64]bool{}
	for _, snapshot := range metadata.Snapshots() {
		ids[snapshot.SnapshotID] = true
	}
	return ids
}

func TestApplySnapshotsParamRefsKeepsOnlyReferenced(t *testing.T) {
	metadata := metadataWithSnapshots(t, []int64{1, 2, 3}, 1)

	got, err := applySnapshotsParam(httptest.NewRequest("GET", "/v1/namespaces/ns/tables/t?snapshots=refs", nil), metadata)
	if err != nil {
		t.Fatalf("applySnapshotsParam() error = %v", err)
	}

	ids := snapshotIDs(got)
	if !ids[1] {
		t.Error("tagged snapshot 1 was dropped")
	}
	if !ids[3] {
		t.Error("current snapshot 3 was dropped")
	}
	if ids[2] {
		t.Error("unreferenced snapshot 2 was returned")
	}
}

func TestApplySnapshotsParamDefaultsToAll(t *testing.T) {
	metadata := metadataWithSnapshots(t, []int64{1, 2, 3}, 1)

	for _, target := range []string{"/v1/namespaces/ns/tables/t", "/v1/namespaces/ns/tables/t?snapshots=all"} {
		got, err := applySnapshotsParam(httptest.NewRequest("GET", target, nil), metadata)
		if err != nil {
			t.Fatalf("applySnapshotsParam(%s) error = %v", target, err)
		}
		if len(got.Snapshots()) != 3 {
			t.Errorf("applySnapshotsParam(%s) kept %d snapshots, want 3", target, len(got.Snapshots()))
		}
	}
}
