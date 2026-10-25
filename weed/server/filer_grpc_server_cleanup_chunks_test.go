package weed_server

import (
	"context"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// A CreateEntry whose context is cancelled before its chunks are compacted must
// fail the request and store nothing, rather than panic in CompactFileChunks.
func TestCreateEntryCancelledContextFailsRequest(t *testing.T) {
	store := newRenameTestStore()
	f := newRenameTestFiler(t, store)

	fs := &FilerServer{
		filer:          f,
		option:         &FilerOption{},
		entryLockTable: util.NewLockTable[util.FullPath](),
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := fs.CreateEntry(ctx, &filer_pb.CreateEntryRequest{
		Directory:                "/test",
		SkipCheckParentDirectory: true,
		Entry: &filer_pb.Entry{
			Name:       "obj",
			Attributes: &filer_pb.FuseAttributes{Mtime: 1700000000, FileMode: 0644, Inode: 1},
			Chunks: []*filer_pb.FileChunk{
				{FileId: "1,01", Offset: 0, Size: 100, ModifiedTsNs: 1},
			},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "CompactFileChunks: "+context.Canceled.Error()) {
		t.Fatalf("err = %v, want CompactFileChunks to fail with context canceled", err)
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	if _, found := store.entries["/test/obj"]; found {
		t.Fatal("entry was stored despite the failed request")
	}
}
