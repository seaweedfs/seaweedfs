package weed_server

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func chunkFid(fid string, size uint64) *filer_pb.FileChunk {
	return &filer_pb.FileChunk{FileId: fid, Offset: 0, Size: size}
}

func TestValidateExpectedChunks(t *testing.T) {
	withChunks := func(fids ...string) *filer.Entry {
		e := &filer.Entry{FullPath: "/test/obj"}
		for _, fid := range fids {
			e.Chunks = append(e.Chunks, chunkFid(fid, 100))
		}
		return e
	}

	cases := []struct {
		name     string
		entry    *filer.Entry
		expected []string
		wantErr  bool
	}{
		{"nil-disables", withChunks("3,01"), nil, false},
		{"empty-disables", withChunks("3,01"), []string{}, false},
		{"nil-entry-disables", nil, nil, false},
		{"nil-entry-with-expected", nil, []string{"3,01"}, true},
		{"exact-match", withChunks("3,01", "3,02"), []string{"3,01", "3,02"}, false},
		{"order-independent", withChunks("3,01", "3,02"), []string{"3,02", "3,01"}, false},
		{"matching-duplicates", withChunks("3,01", "3,01", "3,02"), []string{"3,01", "3,01", "3,02"}, false},
		{"matching-duplicates-reordered", withChunks("3,02", "3,01", "3,01"), []string{"3,01", "3,02", "3,01"}, false},
		// The strand: stored was diffed down to empty by a concurrent update,
		// but the stale writer still expects the old chunk present -> reject.
		{"stored-emptied", withChunks(), []string{"3,01"}, true},
		{"chunk-added", withChunks("3,01", "3,02"), []string{"3,01"}, true},
		{"chunk-removed", withChunks("3,01"), []string{"3,01", "3,02"}, true},
		{"chunk-replaced", withChunks("3,09"), []string{"3,01"}, true},
		{"duplicate-count-mismatch", withChunks("3,01"), []string{"3,01", "3,01"}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateExpectedChunks(tc.entry, tc.expected)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("want FailedPrecondition, got nil")
				}
				if status.Code(err) != codes.FailedPrecondition {
					t.Fatalf("want FailedPrecondition, got %v", status.Code(err))
				}
				return
			}
			if err != nil {
				t.Fatalf("want nil, got %v", err)
			}
		})
	}
}

// TestUpdateEntryChunkCASPreventsStrand drives the real UpdateEntry handler
// through the stranding interleaving from issue #10366:
//
//  1. entry has chunks=[F]; needle F is live.
//  2. R (a chunk-preserving read-modify-write) snapshots expected chunks=[F].
//  3. D (eviction) commits chunks=[] first; F is diffed away and queued for
//     deletion. We model D's commit by leaving the stored entry at chunks=[].
//  4. R commits its stale snapshot last.
//
// Without the precondition, R's stale write resurrects the reference to F
// (last-write-wins) even though F is already queued for deletion, stranding the
// entry on a dead needle. With expected_chunks set, R's write is rejected with a
// retryable FAILED_PRECONDITION and the stored entry stays chunks=[].
func TestUpdateEntryChunkCASPreventsStrand(t *testing.T) {
	const fidF = "3,01637037d6"

	newFiler := func(storedChunks []*filer_pb.FileChunk) (*FilerServer, *renameTestStore) {
		store := newRenameTestStore()
		store.entries["/test/obj"] = &filer.Entry{
			FullPath: "/test/obj",
			Attr:     filer.Attr{Inode: 1, Mtime: time.Unix(1700000000, 0), Mode: 0644},
			Chunks:   storedChunks,
		}
		f := newRenameTestFiler(store)
		fs := &FilerServer{filer: f, option: &FilerOption{}, entryLockTable: util.NewLockTable[util.FullPath]()}
		return fs, store
	}

	// R's stale request: it read chunks=[F] and re-asserts chunks=[F].
	staleReq := func(withCAS bool) *filer_pb.UpdateEntryRequest {
		req := &filer_pb.UpdateEntryRequest{
			Directory: "/test",
			Entry: &filer_pb.Entry{
				Name:       "obj",
				Attributes: &filer_pb.FuseAttributes{Mtime: 1700000005, FileMode: 0644, Inode: 1},
				Chunks:     []*filer_pb.FileChunk{chunkFid(fidF, 100)},
			},
		}
		if withCAS {
			req.ExpectedChunks = []string{fidF}
		}
		return req
	}

	// D already committed chunks=[]; R commits last WITHOUT the precondition.
	t.Run("without-cas-resurrects", func(t *testing.T) {
		fs, store := newFiler(nil)
		if _, err := fs.UpdateEntry(context.Background(), staleReq(false)); err != nil {
			t.Fatalf("UpdateEntry: %v", err)
		}
		if got := len(store.entries["/test/obj"].GetChunks()); got != 1 {
			t.Fatalf("expected the buggy resurrection (chunks=[F]); got %d chunks", got)
		}
	})

	// D already committed chunks=[]; R commits last WITH the precondition.
	t.Run("with-cas-rejected", func(t *testing.T) {
		fs, store := newFiler(nil)
		_, err := fs.UpdateEntry(context.Background(), staleReq(true))
		if status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("stale chunk-preserving write must be rejected with FailedPrecondition, got %v", err)
		}
		if got := len(store.entries["/test/obj"].GetChunks()); got != 0 {
			t.Fatalf("entry must stay chunks=[] (no resurrected reference); got %d chunks", got)
		}
	})

	// Fresh RMW: the precondition still matches the stored chunk set, so the
	// metadata-only update is applied and chunks=[F] is preserved.
	t.Run("with-cas-fresh-passes", func(t *testing.T) {
		fs, store := newFiler([]*filer_pb.FileChunk{chunkFid(fidF, 100)})
		if _, err := fs.UpdateEntry(context.Background(), staleReq(true)); err != nil {
			t.Fatalf("fresh RMW must succeed, got %v", err)
		}
		chunks := store.entries["/test/obj"].GetChunks()
		if len(chunks) != 1 || chunks[0].GetFileIdString() != fidF {
			t.Fatalf("fresh RMW must preserve chunks=[F]; got %v", chunks)
		}
	})
}
