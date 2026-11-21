package weed_server

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// A delete as a client sees it: the store refuses the delete of /dir/dst.txt,
// one of the two links hardLinkChunkServer sets up, the client sends it again,
// and /dir/other.txt keeps the file's chunk until it is deleted too.

func deleteLink(t *testing.T, server *FilerServer, name string) string {
	t.Helper()
	resp, err := server.DeleteEntry(context.Background(), &filer_pb.DeleteEntryRequest{
		Directory: "/dir", Name: name, IsDeleteData: true,
	})
	if err != nil {
		t.Fatalf("DeleteEntry %s: %v", name, err)
	}
	return resp.GetError()
}

func TestDeleteEntryRefusedThenSentAgainKeepsOtherLink(t *testing.T) {
	server, f, store, id := hardLinkChunkServer(t, 2)

	// the store refuses the delete, as TiKV does on a write conflict
	store.deleteErr = errors.New("write conflict { start_ts:463, conflict_start_ts:462, conflict_commit_ts:464 }")
	if msg := deleteLink(t, server, "dst.txt"); !strings.Contains(msg, "write conflict") {
		t.Fatalf("refused delete answered %q, want the store's refusal", msg)
	}
	store.deleteErr = nil
	if msg := deleteLink(t, server, "dst.txt"); msg != "" {
		t.Fatalf("delete sent again: %s", msg)
	}
	checkDeletions(t, f)
	checkOtherLink(t, f, replacedChunk, 1)

	if msg := deleteLink(t, server, "other.txt"); msg != "" {
		t.Fatalf("delete other.txt: %s", msg)
	}
	checkDeletions(t, f, replacedChunk)
	if _, found := store.kv[string(id)]; found {
		t.Fatal("hard link record kept after its last link was deleted")
	}
}
