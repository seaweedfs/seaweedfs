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

// PATCH_EXTENDED with set_content replaces Entry.Content while merging Extended
// and preserving the rest; without set_content, Content is left untouched.
func TestObjectTransactionPatchContent(t *testing.T) {
	now := time.Unix(1700000000, 0)
	fs, store := newTxnTestServer(map[string]*filer.Entry{
		"/buckets/b": {
			Attr:     filer.Attr{Inode: 1, Mtime: now, Crtime: now, Mode: 0755 | (1 << 31)},
			Extended: map[string][]byte{"versioning": []byte("Enabled")},
			Content:  []byte("old-content"),
		},
	})

	// set_content replaces Content and merges an Extended key, preserving the
	// existing versioning key.
	resp, err := fs.ObjectTransaction(context.Background(), &filer_pb.ObjectTransactionRequest{
		LockKey: "/buckets/b",
		Mutations: []*filer_pb.ObjectMutation{
			{Type: filer_pb.ObjectMutation_PATCH_EXTENDED, Directory: "/buckets", Name: "b",
				SetContent: true, Content: []byte("encryption-blob"),
				SetExtended: map[string][]byte{"cors": []byte("yes")}},
		},
	})
	if err != nil || resp.Error != "" {
		t.Fatalf("patch set_content failed: err=%v resp=%q", err, resp.Error)
	}
	e := store.entries["/buckets/b"]
	if string(e.Content) != "encryption-blob" {
		t.Fatalf("content = %q, want encryption-blob", e.Content)
	}
	if string(e.Extended["versioning"]) != "Enabled" || string(e.Extended["cors"]) != "yes" {
		t.Fatalf("extended not merged: %v", e.Extended)
	}

	// A PATCH without set_content must not disturb Content.
	resp, err = fs.ObjectTransaction(context.Background(), &filer_pb.ObjectTransactionRequest{
		LockKey: "/buckets/b",
		Mutations: []*filer_pb.ObjectMutation{
			{Type: filer_pb.ObjectMutation_PATCH_EXTENDED, Directory: "/buckets", Name: "b",
				SetExtended: map[string][]byte{"versioning": []byte("Suspended")}},
		},
	})
	if err != nil || resp.Error != "" {
		t.Fatalf("patch extended-only failed: err=%v resp=%q", err, resp.Error)
	}
	e = store.entries["/buckets/b"]
	if string(e.Content) != "encryption-blob" {
		t.Fatalf("content clobbered by extended-only patch: %q", e.Content)
	}
	if string(e.Extended["versioning"]) != "Suspended" {
		t.Fatalf("versioning = %q, want Suspended", e.Extended["versioning"])
	}
	if e.FileSize != 0 {
		t.Fatalf("directory FileSize must stay 0, got %d", e.FileSize)
	}

	// For a file, set_content syncs FileSize to the new content length, even when
	// the content shrinks.
	store.entries["/file"] = &filer.Entry{
		FullPath: "/file",
		Attr:     filer.Attr{Inode: 9, Mtime: now, Crtime: now, Mode: 0644, FileSize: 100},
		Content:  []byte("xxxxxxxxxxxxxxx"),
	}
	resp, err = fs.ObjectTransaction(context.Background(), &filer_pb.ObjectTransactionRequest{
		LockKey: "/file",
		Mutations: []*filer_pb.ObjectMutation{
			{Type: filer_pb.ObjectMutation_PATCH_EXTENDED, Directory: "/", Name: "file",
				SetContent: true, Content: []byte("short")},
		},
	})
	if err != nil || resp.Error != "" {
		t.Fatalf("file patch failed: err=%v resp=%q", err, resp.Error)
	}
	if f := store.entries["/file"]; string(f.Content) != "short" || f.FileSize != uint64(len("short")) {
		t.Fatalf("file content=%q FileSize=%d, want short/5", f.Content, f.FileSize)
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

// Deleting the latest version and recomputing re-points the pointer at the new
// highest-named remaining version; the scan runs under the transaction lock.
func TestObjectTransactionRecomputeLatest(t *testing.T) {
	now := time.Unix(1700000000, 0)
	ver := func(id string) *filer.Entry {
		return &filer.Entry{
			Attr:     filer.Attr{Inode: 10, Mtime: now, Crtime: now, Mode: 0644},
			Extended: map[string][]byte{"vid": []byte(id), "etag": []byte("etag-" + id)},
		}
	}
	fs, store := newTxnTestServer(map[string]*filer.Entry{
		"/buckets/b/obj/.versions": {
			Attr: filer.Attr{Inode: 2, Mtime: now, Crtime: now, Mode: 0755 | (1 << 31)},
			Extended: map[string][]byte{
				"latestVid": []byte("v3"), "latestEtag": []byte("etag-v3"), "latestName": []byte("v3.ver"),
			},
		},
		"/buckets/b/obj/.versions/v1.ver": ver("v1"),
		"/buckets/b/obj/.versions/v2.ver": ver("v2"),
		"/buckets/b/obj/.versions/v3.ver": ver("v3"),
	})

	recompute := func() *filer_pb.ObjectMutation {
		return &filer_pb.ObjectMutation{
			Type: filer_pb.ObjectMutation_RECOMPUTE_LATEST, Directory: "/buckets/b/obj", Name: ".versions",
			Recompute: &filer_pb.Recompute{
				ScanDir:      "/buckets/b/obj/.versions",
				Descending:   true,
				CopyExtended: map[string]string{"latestVid": "vid", "latestEtag": "etag"},
				NameToKey:    "latestName",
			},
		}
	}

	// Delete the latest (v3); recompute should pick v2.
	resp, err := fs.ObjectTransaction(context.Background(), &filer_pb.ObjectTransactionRequest{
		LockKey: "/buckets/b/obj",
		Mutations: []*filer_pb.ObjectMutation{
			{Type: filer_pb.ObjectMutation_DELETE, Directory: "/buckets/b/obj/.versions", Name: "v3.ver"},
			recompute(),
		},
	})
	if err != nil || resp.Error != "" {
		t.Fatalf("txn failed: err=%v resp=%q", err, resp.Error)
	}
	ptr := store.entries["/buckets/b/obj/.versions"].Extended
	if string(ptr["latestVid"]) != "v2" || string(ptr["latestEtag"]) != "etag-v2" || string(ptr["latestName"]) != "v2.ver" {
		t.Fatalf("after deleting v3, pointer = vid:%s etag:%s name:%s; want v2",
			ptr["latestVid"], ptr["latestEtag"], ptr["latestName"])
	}

	// Delete the remaining versions; recompute on an empty dir clears the pointer.
	resp, err = fs.ObjectTransaction(context.Background(), &filer_pb.ObjectTransactionRequest{
		LockKey: "/buckets/b/obj",
		Mutations: []*filer_pb.ObjectMutation{
			{Type: filer_pb.ObjectMutation_DELETE, Directory: "/buckets/b/obj/.versions", Name: "v2.ver"},
			{Type: filer_pb.ObjectMutation_DELETE, Directory: "/buckets/b/obj/.versions", Name: "v1.ver"},
			recompute(),
		},
	})
	if err != nil || resp.Error != "" {
		t.Fatalf("txn failed: err=%v resp=%q", err, resp.Error)
	}
	ptr = store.entries["/buckets/b/obj/.versions"].Extended
	for _, k := range []string{"latestVid", "latestEtag", "latestName"} {
		if _, ok := ptr[k]; ok {
			t.Errorf("pointer key %q should be cleared when no versions remain", k)
		}
	}
}

// With descending=false the lowest-named child is chosen (the listing is capped
// at one entry).
func TestObjectTransactionRecomputeAscending(t *testing.T) {
	now := time.Unix(1700000000, 0)
	ver := func(id string) *filer.Entry {
		return &filer.Entry{Attr: filer.Attr{Inode: 10, Mtime: now, Crtime: now, Mode: 0644}, Extended: map[string][]byte{"vid": []byte(id)}}
	}
	fs, store := newTxnTestServer(map[string]*filer.Entry{
		"/buckets/b/obj/.versions":        {Attr: filer.Attr{Inode: 2, Mtime: now, Crtime: now, Mode: 0755 | (1 << 31)}, Extended: map[string][]byte{}},
		"/buckets/b/obj/.versions/v1.ver": ver("v1"),
		"/buckets/b/obj/.versions/v2.ver": ver("v2"),
	})

	resp, err := fs.ObjectTransaction(context.Background(), &filer_pb.ObjectTransactionRequest{
		LockKey: "/buckets/b/obj",
		Mutations: []*filer_pb.ObjectMutation{
			{Type: filer_pb.ObjectMutation_RECOMPUTE_LATEST, Directory: "/buckets/b/obj", Name: ".versions",
				Recompute: &filer_pb.Recompute{
					ScanDir:      "/buckets/b/obj/.versions",
					Descending:   false,
					CopyExtended: map[string]string{"latestVid": "vid"},
				}},
		},
	})
	if err != nil || resp.Error != "" {
		t.Fatalf("txn failed: err=%v resp=%q", err, resp.Error)
	}
	if got := string(store.entries["/buckets/b/obj/.versions"].Extended["latestVid"]); got != "v1" {
		t.Fatalf("ascending recompute latestVid = %q, want v1 (lowest)", got)
	}
}

// A batch applies each transaction independently: one failed precondition does
// not abort the others, matching S3 multi-object delete semantics.
func TestObjectTransactionBatchIndependent(t *testing.T) {
	now := time.Unix(1700000000, 0)
	obj := func(inode uint64) *filer.Entry {
		return &filer.Entry{
			Attr:     filer.Attr{Inode: inode, Mtime: now, Crtime: now, Mode: 0644},
			Extended: map[string][]byte{s3_constants.ExtETagKey: []byte("abc")},
		}
	}
	fs, store := newTxnTestServer(map[string]*filer.Entry{
		"/buckets/b/a": obj(1),
		"/buckets/b/c": obj(3),
	})

	del := func(name string, cond *filer_pb.WriteCondition) *filer_pb.ObjectTransactionRequest {
		return &filer_pb.ObjectTransactionRequest{
			LockKey:   "/buckets/b/" + name,
			Condition: cond,
			Mutations: []*filer_pb.ObjectMutation{
				{Type: filer_pb.ObjectMutation_DELETE, Directory: "/buckets/b", Name: name},
			},
		}
	}

	resp, err := fs.ObjectTransactionBatch(context.Background(), &filer_pb.ObjectTransactionBatchRequest{
		Transactions: []*filer_pb.ObjectTransactionRequest{
			del("a", nil),
			del("c", &filer_pb.WriteCondition{Clauses: []*filer_pb.WriteCondition_Clause{
				{Kind: filer_pb.WriteCondition_IF_ETAG_MATCH, Etags: []string{`"zzz"`}},
			}}),
		},
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(resp.Responses) != 2 {
		t.Fatalf("want 2 responses, got %d", len(resp.Responses))
	}
	if resp.Responses[0].Error != "" {
		t.Errorf("delete a should succeed: %q", resp.Responses[0].Error)
	}
	if resp.Responses[1].ErrorCode != filer_pb.FilerError_PRECONDITION_FAILED {
		t.Errorf("delete c should fail precondition, got %v", resp.Responses[1].ErrorCode)
	}
	if _, ok := store.entries["/buckets/b/a"]; ok {
		t.Errorf("a should be deleted")
	}
	if _, ok := store.entries["/buckets/b/c"]; !ok {
		t.Errorf("c should survive its failed precondition")
	}
}

// A nil transaction in a batch yields an error response in its slot rather than
// panicking, keeping responses parallel to the requests.
func TestObjectTransactionBatchNilTransaction(t *testing.T) {
	now := time.Unix(1700000000, 0)
	fs, store := newTxnTestServer(map[string]*filer.Entry{
		"/buckets/b/a": {Attr: filer.Attr{Inode: 1, Mtime: now, Crtime: now, Mode: 0644}},
	})

	resp, err := fs.ObjectTransactionBatch(context.Background(), &filer_pb.ObjectTransactionBatchRequest{
		Transactions: []*filer_pb.ObjectTransactionRequest{
			nil,
			{LockKey: "/buckets/b/a", Mutations: []*filer_pb.ObjectMutation{
				{Type: filer_pb.ObjectMutation_DELETE, Directory: "/buckets/b", Name: "a"},
			}},
		},
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(resp.Responses) != 2 {
		t.Fatalf("want 2 responses (parallel to requests), got %d", len(resp.Responses))
	}
	if resp.Responses[0].Error == "" {
		t.Errorf("nil transaction should produce an error response")
	}
	if resp.Responses[1].Error != "" {
		t.Errorf("valid transaction should succeed: %q", resp.Responses[1].Error)
	}
	if _, ok := store.entries["/buckets/b/a"]; ok {
		t.Errorf("a should be deleted by the valid transaction")
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
