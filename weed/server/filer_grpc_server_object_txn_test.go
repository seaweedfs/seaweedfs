package weed_server

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func newTxnTestServer(seed map[string]*filer.Entry) (*FilerServer, *renameTestStore) {
	store := newRenameTestStore()
	for path, entry := range seed {
		entry.FullPath = util.FullPath(path)
		store.entries[path] = entry
	}
	f := newRenameTestFiler(store)
	f.DirBucketsPath = "/buckets"
	fs := &FilerServer{filer: f, option: &FilerOption{}, entryLockTable: util.NewLockTable[util.FullPath]()}
	return fs, store
}

// A versioned delete is a multi-entry object operation: drop the null version,
// write a delete marker, and flip the latest pointer. ObjectTransaction applies
// all three atomically under one lock keyed on the object path.
func TestObjectTransactionMultiEntry(t *testing.T) {
	now := time.Unix(1700000000, 0)
	fs, store := newTxnTestServer(map[string]*filer.Entry{
		"/buckets/b/obj": {
			Attr:     filer.Attr{Inode: 1, Mtime: now, Crtime: now, Mode: 0644},
			Extended: map[string][]byte{s3_constants.ExtETagKey: []byte("abc")},
		},
		"/buckets/b/obj/.versions": {
			Attr:     filer.Attr{Inode: 2, Mtime: now, Crtime: now, Mode: 0755 | (1 << 31)},
			Extended: map[string][]byte{"latest": []byte("v1")},
		},
	})

	req := &filer_pb.ObjectTransactionRequest{
		LockKey: "/buckets/b/obj",
		Mutations: []*filer_pb.ObjectMutation{
			{Type: filer_pb.ObjectMutation_DELETE, Directory: "/buckets/b", Name: "obj"},
			{Type: filer_pb.ObjectMutation_PUT, Directory: "/buckets/b/obj/.versions", Entry: &filer_pb.Entry{
				Name:       "marker",
				Attributes: &filer_pb.FuseAttributes{Mtime: now.Unix(), FileMode: 0644, Inode: 3},
				Extended:   map[string][]byte{"isDeleteMarker": []byte("true")},
			}},
			{Type: filer_pb.ObjectMutation_PATCH_EXTENDED, Directory: "/buckets/b/obj", Name: ".versions",
				SetExtended:    map[string][]byte{"latest": []byte("marker")},
				DeleteExtended: nil},
		},
	}

	resp, err := fs.ObjectTransaction(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if resp.Error != "" {
		t.Fatalf("unexpected response error: %q", resp.Error)
	}

	if _, ok := store.entries["/buckets/b/obj"]; ok {
		t.Errorf("null version should be deleted")
	}
	if _, ok := store.entries["/buckets/b/obj/.versions/marker"]; !ok {
		t.Errorf("delete marker should be created")
	}
	if got := string(store.entries["/buckets/b/obj/.versions"].Extended["latest"]); got != "marker" {
		t.Errorf("latest pointer = %q, want marker", got)
	}
}

// A PATCH_EXTENDED mutation emits a metadata event (so the change replicates and
// subscribers see it), carrying both the prior and updated state in the diff.
func TestObjectTransactionPatchNotifies(t *testing.T) {
	queue := &captureQueue{}
	swapNotificationQueue(t, queue)

	now := time.Unix(1700000000, 0)
	fs, _ := newTxnTestServer(map[string]*filer.Entry{
		"/buckets/b/obj/.versions": {
			Attr:     filer.Attr{Inode: 2, Mtime: now, Crtime: now, Mode: 0755 | (1 << 31)},
			Extended: map[string][]byte{"latest": []byte("v1")},
		},
	})

	resp, err := fs.ObjectTransaction(context.Background(), &filer_pb.ObjectTransactionRequest{
		LockKey: "/buckets/b/obj",
		Mutations: []*filer_pb.ObjectMutation{
			{Type: filer_pb.ObjectMutation_PATCH_EXTENDED, Directory: "/buckets/b/obj", Name: ".versions",
				SetExtended: map[string][]byte{"latest": []byte("v2")}},
		},
	})
	if err != nil || resp.Error != "" {
		t.Fatalf("txn failed: err=%v resp=%q", err, resp.Error)
	}

	events := queue.snapshot()
	if len(events) != 1 {
		t.Fatalf("expected 1 metadata event from PATCH_EXTENDED, got %d", len(events))
	}
	ev := events[0].notification
	if ev.NewEntry == nil || string(ev.NewEntry.Extended["latest"]) != "v2" {
		t.Fatalf("event new entry latest = %q, want v2", ev.GetNewEntry().GetExtended()["latest"])
	}
	if ev.OldEntry == nil || string(ev.OldEntry.Extended["latest"]) != "v1" {
		t.Fatalf("event old entry latest = %q, want v1 (clone must preserve prior state)", ev.GetOldEntry().GetExtended()["latest"])
	}
}

// A failing precondition aborts before any mutation is applied.
func TestObjectTransactionPreconditionAborts(t *testing.T) {
	now := time.Unix(1700000000, 0)
	fs, store := newTxnTestServer(map[string]*filer.Entry{
		"/buckets/b/obj": {
			Attr:     filer.Attr{Inode: 1, Mtime: now, Crtime: now, Mode: 0644},
			Extended: map[string][]byte{s3_constants.ExtETagKey: []byte("abc")},
		},
	})

	req := &filer_pb.ObjectTransactionRequest{
		LockKey: "/buckets/b/obj",
		Condition: &filer_pb.WriteCondition{Clauses: []*filer_pb.WriteCondition_Clause{
			{Kind: filer_pb.WriteCondition_IF_ETAG_MATCH, Etags: []string{`"zzz"`}},
		}},
		Mutations: []*filer_pb.ObjectMutation{
			{Type: filer_pb.ObjectMutation_DELETE, Directory: "/buckets/b", Name: "obj"},
		},
	}

	resp, err := fs.ObjectTransaction(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if resp.ErrorCode != filer_pb.FilerError_PRECONDITION_FAILED {
		t.Fatalf("want PRECONDITION_FAILED, got %v (%q)", resp.ErrorCode, resp.Error)
	}
	if _, ok := store.entries["/buckets/b/obj"]; !ok {
		t.Errorf("object must survive a failed precondition")
	}
}

// DELETE and PATCH of an absent entry are no-ops, so a replayed transaction
// does not error.
func TestObjectTransactionIdempotentNoops(t *testing.T) {
	fs, _ := newTxnTestServer(nil)

	req := &filer_pb.ObjectTransactionRequest{
		LockKey: "/buckets/b/obj",
		Mutations: []*filer_pb.ObjectMutation{
			{Type: filer_pb.ObjectMutation_DELETE, Directory: "/buckets/b", Name: "obj"},
			{Type: filer_pb.ObjectMutation_PATCH_EXTENDED, Directory: "/buckets/b/obj", Name: ".versions",
				SetExtended: map[string][]byte{"latest": []byte("x")}},
		},
	}

	resp, err := fs.ObjectTransaction(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if resp.Error != "" {
		t.Fatalf("no-op mutations should not error: %q", resp.Error)
	}
}
