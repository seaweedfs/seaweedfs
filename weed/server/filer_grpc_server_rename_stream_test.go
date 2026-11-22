package weed_server

import (
	"context"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
)

// renameTestStream collects what StreamRenameEntry sends.
type renameTestStream struct {
	grpc.ServerStream
	sent []*filer_pb.StreamRenameEntryResponse
}

func (s *renameTestStream) Context() context.Context { return context.Background() }
func (s *renameTestStream) Send(resp *filer_pb.StreamRenameEntryResponse) error {
	s.sent = append(s.sent, resp)
	return nil
}

func streamRename(t *testing.T, store filer.FilerStore, oldName, newName string) (*renameTestStream, error) {
	t.Helper()
	server := &FilerServer{filer: newRenameTestFiler(t, store), entryLockTable: util.NewLockTable[util.FullPath]()}
	stream := &renameTestStream{}
	err := server.StreamRenameEntry(&filer_pb.StreamRenameEntryRequest{
		OldDirectory: "/",
		OldName:      oldName,
		NewDirectory: "/",
		NewName:      newName,
	}, stream)
	return stream, err
}

// A directory renamed onto a non-empty directory is refused. The refusal has
// to end the rename's transaction: a SQL store otherwise keeps it open, and
// with it one of its pool's connections, for the life of the filer.
func TestStreamRenameOntoNonEmptyDirectoryEndsItsTransaction(t *testing.T) {
	store := newRenameTestStore()
	store.strictTx = true
	store.entries["/a"] = newDirectoryEntry("/a", 101)
	store.entries["/b"] = newDirectoryEntry("/b", 102)
	store.entries["/b/x.txt"] = newFileEntry("/b/x.txt", 103)

	stream, err := streamRename(t, store, "a", "b")
	if err == nil || !strings.Contains(err.Error(), "is not empty") {
		t.Fatalf("StreamRenameEntry error = %v, want \"is not empty\"", err)
	}
	if n := store.openTransactions(); n != 0 {
		t.Fatalf("%d transaction(s) left open after the refusal", n)
	}
	if len(stream.sent) != 0 {
		t.Fatalf("sent %d responses for a refused rename", len(stream.sent))
	}
	if _, err := store.FindEntry(context.Background(), "/a"); err != nil {
		t.Fatalf("source after the refusal: %v", err)
	}
}

// The other ways out of StreamRenameEntry end the transaction too.
func TestStreamRenameEndsItsTransaction(t *testing.T) {
	t.Run("onto a file", func(t *testing.T) {
		store := newRenameTestStore()
		store.strictTx = true
		store.entries["/a"] = newDirectoryEntry("/a", 101)
		store.entries["/c.txt"] = newFileEntry("/c.txt", 102)
		if _, err := streamRename(t, store, "a", "c.txt"); err == nil || !strings.Contains(err.Error(), "is not directory") {
			t.Fatalf("StreamRenameEntry error = %v, want \"is not directory\"", err)
		}
		if n := store.openTransactions(); n != 0 {
			t.Fatalf("%d transaction(s) left open", n)
		}
	})
	t.Run("onto an empty directory", func(t *testing.T) {
		store := newRenameTestStore()
		store.strictTx = true
		store.entries["/a"] = newDirectoryEntry("/a", 101)
		store.entries["/a/y.txt"] = newFileEntry("/a/y.txt", 102)
		store.entries["/e"] = newDirectoryEntry("/e", 103)
		if _, err := streamRename(t, store, "a", "e"); err != nil {
			t.Fatalf("StreamRenameEntry: %v", err)
		}
		if n := store.openTransactions(); n != 0 {
			t.Fatalf("%d transaction(s) left open", n)
		}
		if _, err := store.FindEntry(context.Background(), "/e/y.txt"); err != nil {
			t.Fatalf("moved child: %v", err)
		}
	})
}
