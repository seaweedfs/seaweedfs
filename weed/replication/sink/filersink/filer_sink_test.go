package filersink

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// entry builds a file entry with the given mtime and size for the update-action
// decision, which only looks at those two attributes.
func entry(mtime int64, size uint64) *filer_pb.Entry {
	return &filer_pb.Entry{Attributes: &filer_pb.FuseAttributes{Mtime: mtime, FileSize: size}}
}

// chooseUpdateAction decides skip vs repair vs normal. A destination left
// truncated by an earlier failed replication can carry a newer mtime (the
// source preserved an old mtime while the partial copy was written recently),
// so a pure mtime guard would strand the corruption. A shorter destination with
// a newer mtime must be repaired, not skipped.
func TestChooseUpdateAction(t *testing.T) {
	tests := []struct {
		name     string
		existing *filer_pb.Entry
		incoming *filer_pb.Entry
		want     updateAction
	}{
		{"nil existing entry", nil, entry(200, 100), updateNormal},
		{"destination older, catching up", entry(100, 50), entry(200, 100), updateNormal},
		{"same mtime", entry(200, 50), entry(200, 100), updateNormal},
		{"destination newer and complete", entry(300, 100), entry(200, 100), updateSkip},
		{"destination newer and larger", entry(300, 200), entry(200, 100), updateSkip},
		{"destination newer but truncated", entry(300, 90), entry(200, 100), updateRepair},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := chooseUpdateAction(tc.existing, tc.incoming); got != tc.want {
				t.Fatalf("chooseUpdateAction = %v, want %v", got, tc.want)
			}
		})
	}
}
