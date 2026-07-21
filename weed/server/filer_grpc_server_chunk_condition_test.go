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

func chunkFid(fid string) *filer_pb.FileChunk {
	return &filer_pb.FileChunk{FileId: fid, Size: 100}
}

func ifChunksEqual(fids ...string) *filer_pb.WriteCondition {
	return one(&filer_pb.WriteCondition_Clause{Kind: filer_pb.WriteCondition_IF_CHUNKS_EQUAL, Fids: fids})
}

func TestChunkFidsEqualClause(t *testing.T) {
	withChunks := func(fids ...string) *filer.Entry {
		e := &filer.Entry{FullPath: "/test/obj"}
		for _, fid := range fids {
			e.Chunks = append(e.Chunks, chunkFid(fid))
		}
		return e
	}

	cases := []struct {
		name string
		cur  *filer.Entry
		fids []string
		want bool
	}{
		{"absent-no-chunks", nil, nil, true},
		{"absent-expected", nil, []string{"3,01"}, false},
		{"empty-expects-none", withChunks(), nil, true},
		{"exact", withChunks("3,01", "3,02"), []string{"3,01", "3,02"}, true},
		{"reordered", withChunks("3,01", "3,02"), []string{"3,02", "3,01"}, true},
		{"duplicates", withChunks("3,01", "3,01", "3,02"), []string{"3,02", "3,01", "3,01"}, true},
		// The strand: a concurrent update emptied the stored chunk list, so the
		// stale writer's expectation no longer holds.
		{"stored-emptied", withChunks(), []string{"3,01"}, false},
		// The reverse strand: the writer read no chunks, but a concurrent update
		// has since added one; an empty fids list still guards it.
		{"stored-filled", withChunks("3,01"), nil, false},
		{"chunk-added", withChunks("3,01", "3,02"), []string{"3,01"}, false},
		{"chunk-removed", withChunks("3,01"), []string{"3,01", "3,02"}, false},
		{"chunk-replaced", withChunks("3,09"), []string{"3,01"}, false},
		{"duplicate-count", withChunks("3,01"), []string{"3,01", "3,01"}, false},
	}
	for _, tc := range cases {
		if got := writeConditionSatisfied(ifChunksEqual(tc.fids...), tc.cur); got != tc.want {
			t.Errorf("%s: got %v want %v", tc.name, got, tc.want)
		}
	}
}

// TestUpdateEntryChunkConditionPreventsStrand drives the UpdateEntry handler
// through the interleaving that strands an entry on a dead needle:
//
//  1. entry has chunks=[F]; needle F is live.
//  2. R (a chunk-preserving read-modify-write) snapshots chunks=[F].
//  3. D (eviction) commits chunks=[] first; F is diffed away and queued for
//     deletion. The store is seeded at chunks=[] to model D's commit.
//  4. R commits its stale snapshot last.
//
// Without a condition R's write wins and resurrects the reference to F; with
// IF_CHUNKS_EQUAL it fails with FailedPrecondition and the entry stays empty.
func TestUpdateEntryChunkConditionPreventsStrand(t *testing.T) {
	const fidF = "3,01637037d6"

	newServer := func(storedChunks ...*filer_pb.FileChunk) (*FilerServer, *renameTestStore) {
		store := newRenameTestStore()
		store.entries["/test/obj"] = &filer.Entry{
			FullPath: "/test/obj",
			Attr:     filer.Attr{Inode: 1, Mtime: time.Unix(1700000000, 0), Mode: 0644},
			Chunks:   storedChunks,
		}
		f := newRenameTestFiler(store)
		return &FilerServer{filer: f, option: &FilerOption{}, entryLockTable: util.NewLockTable[util.FullPath]()}, store
	}

	// R's stale request: it read chunks=[F] and writes back chunks=[F].
	staleReq := func(cond *filer_pb.WriteCondition) *filer_pb.UpdateEntryRequest {
		return &filer_pb.UpdateEntryRequest{
			Directory: "/test",
			Entry: &filer_pb.Entry{
				Name:       "obj",
				Attributes: &filer_pb.FuseAttributes{Mtime: 1700000005, FileMode: 0644, Inode: 1},
				Chunks:     []*filer_pb.FileChunk{chunkFid(fidF)},
			},
			Condition: cond,
		}
	}

	t.Run("unconditional-resurrects", func(t *testing.T) {
		fs, store := newServer()
		if _, err := fs.UpdateEntry(context.Background(), staleReq(nil)); err != nil {
			t.Fatalf("UpdateEntry: %v", err)
		}
		if got := len(store.entries["/test/obj"].GetChunks()); got != 1 {
			t.Fatalf("last-write-wins should have resurrected chunks=[F], got %d chunks", got)
		}
	})

	t.Run("condition-rejects-stale", func(t *testing.T) {
		fs, store := newServer()
		_, err := fs.UpdateEntry(context.Background(), staleReq(ifChunksEqual(fidF)))
		if status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("stale write must fail with FailedPrecondition, got %v", err)
		}
		if got := len(store.entries["/test/obj"].GetChunks()); got != 0 {
			t.Fatalf("entry must stay chunks=[], got %d chunks", got)
		}
	})

	t.Run("condition-passes-fresh", func(t *testing.T) {
		fs, store := newServer(chunkFid(fidF))
		if _, err := fs.UpdateEntry(context.Background(), staleReq(ifChunksEqual(fidF))); err != nil {
			t.Fatalf("fresh read-modify-write must succeed: %v", err)
		}
		chunks := store.entries["/test/obj"].GetChunks()
		if len(chunks) != 1 || chunks[0].GetFileIdString() != fidF {
			t.Fatalf("chunks=[F] must be preserved, got %v", chunks)
		}
	})

	// The reverse strand: R read the entry before a concurrent writer added F,
	// so its stale write would wipe the chunk and queue F for deletion. An
	// IF_CHUNKS_EQUAL clause with no fids guards the emptiness it observed.
	t.Run("empty-expectation-rejects", func(t *testing.T) {
		fs, store := newServer(chunkFid(fidF))
		req := &filer_pb.UpdateEntryRequest{
			Directory: "/test",
			Entry: &filer_pb.Entry{
				Name:       "obj",
				Attributes: &filer_pb.FuseAttributes{Mtime: 1700000005, FileMode: 0644, Inode: 1},
			},
			Condition: ifChunksEqual(),
		}
		_, err := fs.UpdateEntry(context.Background(), req)
		if status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("stale wipe must fail with FailedPrecondition, got %v", err)
		}
		if got := len(store.entries["/test/obj"].GetChunks()); got != 1 {
			t.Fatalf("chunks=[F] must be preserved, got %d chunks", got)
		}
	})
}

// DeleteEntry queues chunk deletions, so it must serialize on the same path
// lock the conditional writers hold; otherwise it can interleave with a
// passed precondition and the stale write resurrects the queued fids.
func TestDeleteEntryWaitsForPathLock(t *testing.T) {
	store := newRenameTestStore()
	store.entries["/test/obj"] = &filer.Entry{
		FullPath: "/test/obj",
		Attr:     filer.Attr{Inode: 1, Mtime: time.Unix(1700000000, 0), Mode: 0644},
	}
	f := newRenameTestFiler(store)
	fs := &FilerServer{filer: f, option: &FilerOption{}, entryLockTable: util.NewLockTable[util.FullPath]()}

	lockPath := util.FullPath("/test/obj")
	hold := fs.entryLockTable.AcquireLock("test", lockPath, util.ExclusiveLock)

	done := make(chan struct{})
	go func() {
		fs.DeleteEntry(context.Background(), &filer_pb.DeleteEntryRequest{Directory: "/test", Name: "obj"})
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("DeleteEntry completed while the path lock was held")
	case <-time.After(100 * time.Millisecond):
	}

	fs.entryLockTable.ReleaseLock(lockPath, hold)
	<-done
	if _, ok := store.entries["/test/obj"]; ok {
		t.Fatal("entry not deleted after the lock was released")
	}
}
