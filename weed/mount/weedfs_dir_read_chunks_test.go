package mount

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// TestListDirectoryEntriesOmitsChunks covers the wiring the readdir speedup
// rests on: that the context marker actually reaches the store's decode. Every
// other test exercises the decoder directly, so a refactor that stopped
// threading the context would revert the optimisation silently.
func TestListDirectoryEntriesOmitsChunks(t *testing.T) {
	dir := util.FullPath("/images")
	wfs := newBenchWFS(t, dir, 4)

	for _, tc := range []struct {
		name       string
		ctx        context.Context
		wantChunks bool
	}{
		{"plain listing keeps chunks", context.Background(), true},
		{"marked listing drops chunks", filer_pb.WithChunksOmitted(context.Background()), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var seen int
			_, err := wfs.metaCache.ListDirectoryEntries(tc.ctx, dir, "", false, 100, func(entry *filer.Entry) (bool, error) {
				seen++
				if got := len(entry.Chunks) > 0; got != tc.wantChunks {
					t.Errorf("%s: has chunks = %v, want %v", entry.Name(), got, tc.wantChunks)
				}
				// The size has to survive either way, since that is what the
				// readdir reports.
				if entry.FileSize != 4<<20 {
					t.Errorf("%s: FileSize = %d, want %d", entry.Name(), entry.FileSize, 4<<20)
				}
				return true, nil
			})
			if err != nil {
				t.Fatalf("list: %v", err)
			}
			if seen != 4 {
				t.Fatalf("listed %d entries, want 4", seen)
			}
		})
	}

	// readdirContext is what weedfs_dir_read.go actually passes.
	if !filer_pb.ChunksOmitted(readdirContext) {
		t.Error("readdirContext does not carry the chunks-omitted marker")
	}
}
