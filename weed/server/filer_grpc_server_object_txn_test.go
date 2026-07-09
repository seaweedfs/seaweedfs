package weed_server

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/cluster/lock_manager"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

// RECOMPUTE_LATEST copies the chosen child's size/mtime to the pointer and
// stamps the demote key on the prior latest when the pointer moves.
func TestObjectTransactionRecomputeDemoteAndAttrs(t *testing.T) {
	t0 := time.Unix(1700000000, 0)
	t1 := time.Unix(1700000100, 0)
	mk := func(inode uint64, mt time.Time, size uint64, id string) *filer.Entry {
		return &filer.Entry{
			Attr:     filer.Attr{Inode: inode, Mtime: mt, Crtime: mt, Mode: 0644, FileSize: size},
			Extended: map[string][]byte{"vid": []byte(id)},
		}
	}
	fs, store := newTxnTestServer(map[string]*filer.Entry{
		"/buckets/b/obj/.versions": {
			Attr:     filer.Attr{Inode: 2, Mtime: t0, Crtime: t0, Mode: 0755 | (1 << 31)},
			Extended: map[string][]byte{"latestName": []byte("v1.ver"), "latestVid": []byte("v1")},
		},
		"/buckets/b/obj/.versions/v1.ver": mk(10, t0, 100, "v1"),
		"/buckets/b/obj/.versions/v2.ver": mk(11, t1, 250, "v2"),
	})

	resp, err := fs.ObjectTransaction(context.Background(), &filer_pb.ObjectTransactionRequest{
		LockKey: "/buckets/b/obj",
		Mutations: []*filer_pb.ObjectMutation{{
			Type: filer_pb.ObjectMutation_RECOMPUTE_LATEST, Directory: "/buckets/b/obj", Name: ".versions",
			Recompute: &filer_pb.Recompute{
				ScanDir:      "/buckets/b/obj/.versions",
				Descending:   true,
				CopyExtended: map[string]string{"latestVid": "vid"},
				NameToKey:    "latestName",
				SizeToKey:    "latestSize",
				MtimeToKey:   "latestMtime",
				DemoteKey:    "noncurrentSince",
				DemoteValue:  []byte("999"),
			},
		}},
	})
	if err != nil || resp.Error != "" {
		t.Fatalf("txn failed: err=%v resp=%q", err, resp.Error)
	}

	ptr := store.entries["/buckets/b/obj/.versions"].Extended
	if string(ptr["latestName"]) != "v2.ver" || string(ptr["latestVid"]) != "v2" {
		t.Fatalf("pointer not moved to v2: name=%s vid=%s", ptr["latestName"], ptr["latestVid"])
	}
	if string(ptr["latestSize"]) != "250" {
		t.Errorf("latestSize = %s, want 250", ptr["latestSize"])
	}
	if want := strconv.FormatInt(t1.Unix(), 10); string(ptr["latestMtime"]) != want {
		t.Errorf("latestMtime = %s, want %s", ptr["latestMtime"], want)
	}
	if got := store.entries["/buckets/b/obj/.versions/v1.ver"].Extended["noncurrentSince"]; string(got) != "999" {
		t.Errorf("prior latest v1.ver noncurrentSince = %q, want 999", got)
	}
	if _, ok := store.entries["/buckets/b/obj/.versions/v2.ver"].Extended["noncurrentSince"]; ok {
		t.Errorf("new latest v2.ver should not be demoted")
	}
}

// A versioned add is one transaction: the PUT writes the new version file and
// the RECOMPUTE_LATEST that follows, under the same lock, scans the directory,
// sees it, flips the .versions pointer to it, and demotes the prior latest. This
// is what lets putVersionedObject commit the version and its pointer atomically.
func TestObjectTransactionPutThenRecomputeLatest(t *testing.T) {
	t0 := time.Unix(1700000000, 0)
	t1 := time.Unix(1700000100, 0)
	fs, store := newTxnTestServer(map[string]*filer.Entry{
		"/buckets/b/obj/.versions": {
			Attr:     filer.Attr{Inode: 2, Mtime: t0, Crtime: t0, Mode: 0755 | (1 << 31)},
			Extended: map[string][]byte{"latestName": []byte("v1.ver"), "latestVid": []byte("v1")},
		},
		"/buckets/b/obj/.versions/v1.ver": {
			Attr:     filer.Attr{Inode: 10, Mtime: t0, Crtime: t0, Mode: 0644, FileSize: 100},
			Extended: map[string][]byte{"vid": []byte("v1")},
		},
	})

	resp, err := fs.ObjectTransaction(context.Background(), &filer_pb.ObjectTransactionRequest{
		LockKey: "/buckets/b/obj",
		Mutations: []*filer_pb.ObjectMutation{
			{Type: filer_pb.ObjectMutation_PUT, Directory: "/buckets/b/obj/.versions", Entry: &filer_pb.Entry{
				Name:       "v2.ver",
				Attributes: &filer_pb.FuseAttributes{Mtime: t1.Unix(), FileMode: 0644, Inode: 11, FileSize: 250},
				Extended:   map[string][]byte{"vid": []byte("v2")},
			}},
			{Type: filer_pb.ObjectMutation_RECOMPUTE_LATEST, Directory: "/buckets/b/obj", Name: ".versions",
				Recompute: &filer_pb.Recompute{
					ScanDir:      "/buckets/b/obj/.versions",
					Descending:   true,
					CopyExtended: map[string]string{"latestVid": "vid"},
					NameToKey:    "latestName",
					DemoteKey:    "noncurrentSince",
					DemoteValue:  []byte("999"),
				}},
		},
	})
	if err != nil || resp.Error != "" {
		t.Fatalf("txn failed: err=%v resp=%q", err, resp.Error)
	}

	if _, ok := store.entries["/buckets/b/obj/.versions/v2.ver"]; !ok {
		t.Fatalf("the PUT should have created the new version")
	}
	ptr := store.entries["/buckets/b/obj/.versions"].Extended
	if string(ptr["latestName"]) != "v2.ver" || string(ptr["latestVid"]) != "v2" {
		t.Fatalf("pointer should flip to the just-PUT version, got name=%s vid=%s", ptr["latestName"], ptr["latestVid"])
	}
	if got := store.entries["/buckets/b/obj/.versions/v1.ver"].Extended["noncurrentSince"]; string(got) != "999" {
		t.Errorf("prior latest v1.ver should be demoted, noncurrentSince=%q want 999", got)
	}
}

// A version-specific delete locks the object (condition_key checks WORM on the
// version), recomputes the pointer excluding the version (repoint-before-delete),
// then deletes it. A legal-hold guard blocks the delete and preserves the entry.
func TestObjectTransactionVersionDeleteWithWorm(t *testing.T) {
	now := time.Unix(1700000000, 0)
	ver := func(inode uint64, ext map[string][]byte) *filer.Entry {
		return &filer.Entry{Attr: filer.Attr{Inode: inode, Mtime: now, Crtime: now, Mode: 0644}, Extended: ext}
	}
	seed := func(latestLocked bool) map[string]*filer.Entry {
		vcExt := map[string][]byte{"vid": []byte("v3")}
		if latestLocked {
			vcExt["legalhold"] = []byte("ON")
		}
		return map[string]*filer.Entry{
			"/buckets/b/obj/.versions": {
				Attr:     filer.Attr{Inode: 2, Mtime: now, Crtime: now, Mode: 0755 | (1 << 31)},
				Extended: map[string][]byte{"latestName": []byte("v_c"), "latestVid": []byte("v3")},
			},
			"/buckets/b/obj/.versions/v_a": ver(10, map[string][]byte{"vid": []byte("v1")}),
			"/buckets/b/obj/.versions/v_b": ver(11, map[string][]byte{"vid": []byte("v2")}),
			"/buckets/b/obj/.versions/v_c": ver(12, vcExt),
		}
	}
	mkReq := func() *filer_pb.ObjectTransactionRequest {
		return &filer_pb.ObjectTransactionRequest{
			LockKey:      "/buckets/b/obj",
			ConditionKey: "/buckets/b/obj/.versions/v_c",
			Condition: &filer_pb.WriteCondition{Clauses: []*filer_pb.WriteCondition_Clause{
				{Kind: filer_pb.WriteCondition_IF_EXTENDED_NOT_EQUAL, ExtKey: "legalhold", ExtValue: "ON"},
			}},
			Mutations: []*filer_pb.ObjectMutation{
				{Type: filer_pb.ObjectMutation_RECOMPUTE_LATEST, Directory: "/buckets/b/obj", Name: ".versions",
					Recompute: &filer_pb.Recompute{ScanDir: "/buckets/b/obj/.versions", Descending: true, ExcludeName: "v_c",
						NameToKey: "latestName", CopyExtended: map[string]string{"latestVid": "vid"}}},
				{Type: filer_pb.ObjectMutation_DELETE, Directory: "/buckets/b/obj/.versions", Name: "v_c"},
			},
		}
	}

	// Legal hold ON: the WORM guard blocks; version and pointer untouched.
	fs, store := newTxnTestServer(seed(true))
	resp, err := fs.ObjectTransaction(context.Background(), mkReq())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if resp.ErrorCode != filer_pb.FilerError_PRECONDITION_FAILED {
		t.Fatalf("locked version delete should fail precondition, got code=%v err=%q", resp.ErrorCode, resp.Error)
	}
	if _, ok := store.entries["/buckets/b/obj/.versions/v_c"]; !ok {
		t.Errorf("locked version must not be deleted")
	}
	if got := string(store.entries["/buckets/b/obj/.versions"].Extended["latestName"]); got != "v_c" {
		t.Errorf("pointer must be unchanged when delete is blocked, got %s", got)
	}

	// No legal hold: pointer recomputes to v_b (excluding v_c), then v_c is deleted.
	fs, store = newTxnTestServer(seed(false))
	resp, err = fs.ObjectTransaction(context.Background(), mkReq())
	if err != nil || resp.Error != "" {
		t.Fatalf("unlocked delete failed: err=%v resp=%q", err, resp.Error)
	}
	if _, ok := store.entries["/buckets/b/obj/.versions/v_c"]; ok {
		t.Errorf("unlocked version should be deleted")
	}
	ptr := store.entries["/buckets/b/obj/.versions"].Extended
	if string(ptr["latestName"]) != "v_b" || string(ptr["latestVid"]) != "v2" {
		t.Errorf("pointer should recompute to v_b/v2, got name=%s vid=%s", ptr["latestName"], ptr["latestVid"])
	}
}

// Deleting the LAST version tears down the emptied .versions/ directory in the
// same transaction (remove_empty_parent on the DELETE), so a fully-drained key
// leaves no residue to re-trigger the read path's self-heal on every GET. A
// remaining child keeps the directory; a replay after the child is already
// gone still tidies it.
func TestObjectTransactionDeleteRemovesEmptyParent(t *testing.T) {
	now := time.Unix(1700000000, 0)
	seed := func(withSibling bool) map[string]*filer.Entry {
		entries := map[string]*filer.Entry{
			"/buckets/b/obj/.versions": {
				Attr:     filer.Attr{Inode: 2, Mtime: now, Crtime: now, Mode: 0755 | (1 << 31)},
				Extended: map[string][]byte{"latestName": []byte("v_a"), "latestVid": []byte("v1")},
			},
			"/buckets/b/obj/.versions/v_a": {
				Attr:     filer.Attr{Inode: 10, Mtime: now, Crtime: now, Mode: 0644},
				Extended: map[string][]byte{"vid": []byte("v1")},
			},
		}
		if withSibling {
			entries["/buckets/b/obj/.versions/orphan"] = &filer.Entry{
				Attr: filer.Attr{Inode: 11, Mtime: now, Crtime: now, Mode: 0644},
			}
		}
		return entries
	}
	// Mirrors routedDeleteSpecificVersion: repoint excluding the dying version,
	// then delete it and tidy the parent.
	mkReq := func() *filer_pb.ObjectTransactionRequest {
		return &filer_pb.ObjectTransactionRequest{
			LockKey: "/buckets/b/obj",
			Mutations: []*filer_pb.ObjectMutation{
				{Type: filer_pb.ObjectMutation_RECOMPUTE_LATEST, Directory: "/buckets/b/obj", Name: ".versions",
					Recompute: &filer_pb.Recompute{ScanDir: "/buckets/b/obj/.versions", Descending: true, ExcludeName: "v_a",
						NameToKey: "latestName", CopyExtended: map[string]string{"latestVid": "vid"}}},
				{Type: filer_pb.ObjectMutation_DELETE, Directory: "/buckets/b/obj/.versions", Name: "v_a", RemoveEmptyParent: true},
			},
		}
	}

	// Last version: the emptied directory goes with it.
	fs, store := newTxnTestServer(seed(false))
	resp, err := fs.ObjectTransaction(context.Background(), mkReq())
	if err != nil || resp.Error != "" {
		t.Fatalf("txn failed: err=%v resp=%q", err, resp.Error)
	}
	if _, ok := store.entries["/buckets/b/obj/.versions/v_a"]; ok {
		t.Errorf("version should be deleted")
	}
	if _, ok := store.entries["/buckets/b/obj/.versions"]; ok {
		t.Errorf(".versions directory emptied by the delete should be removed")
	}

	// A remaining child keeps the directory (non-recursive teardown declines).
	fs, store = newTxnTestServer(seed(true))
	resp, err = fs.ObjectTransaction(context.Background(), mkReq())
	if err != nil || resp.Error != "" {
		t.Fatalf("txn failed: err=%v resp=%q", err, resp.Error)
	}
	if _, ok := store.entries["/buckets/b/obj/.versions/orphan"]; !ok {
		t.Errorf("sibling must survive the teardown attempt")
	}
	if _, ok := store.entries["/buckets/b/obj/.versions"]; !ok {
		t.Errorf(".versions directory with a remaining child must not be removed")
	}

	// Replay: the child is already gone, the empty parent is still tidied.
	fs, store = newTxnTestServer(map[string]*filer.Entry{
		"/buckets/b/obj/.versions": {
			Attr: filer.Attr{Inode: 2, Mtime: now, Crtime: now, Mode: 0755 | (1 << 31)},
		},
	})
	resp, err = fs.ObjectTransaction(context.Background(), mkReq())
	if err != nil || resp.Error != "" {
		t.Fatalf("replay txn failed: err=%v resp=%q", err, resp.Error)
	}
	if _, ok := store.entries["/buckets/b/obj/.versions"]; ok {
		t.Errorf("replayed delete should still remove the empty directory")
	}
}

// PATCH_EXTENDED with touch_mtime bumps the entry's Mtime (a metadata-replace
// copy) while merging Extended.
func TestObjectTransactionPatchTouchMtime(t *testing.T) {
	old := time.Unix(1600000000, 0)
	fs, store := newTxnTestServer(map[string]*filer.Entry{
		"/buckets/b/obj": {
			FullPath: "/buckets/b/obj",
			Attr:     filer.Attr{Inode: 1, Mtime: old, Crtime: old, Mode: 0644},
			Extended: map[string][]byte{"X-Amz-Meta-old": []byte("1")},
		},
	})
	resp, err := fs.ObjectTransaction(context.Background(), &filer_pb.ObjectTransactionRequest{
		LockKey: "/buckets/b/obj",
		Mutations: []*filer_pb.ObjectMutation{{
			Type: filer_pb.ObjectMutation_PATCH_EXTENDED, Directory: "/buckets/b", Name: "obj",
			SetExtended:    map[string][]byte{"X-Amz-Meta-new": []byte("2")},
			DeleteExtended: []string{"X-Amz-Meta-old"},
			TouchMtime:     true,
		}},
	})
	if err != nil || resp.Error != "" {
		t.Fatalf("patch failed: err=%v resp=%q", err, resp.Error)
	}
	e := store.entries["/buckets/b/obj"]
	if !e.Attr.Mtime.After(old) {
		t.Errorf("touch_mtime should bump Mtime past %v, got %v", old, e.Attr.Mtime)
	}
	if _, ok := e.Extended["X-Amz-Meta-old"]; ok {
		t.Errorf("old meta should be deleted")
	}
	if string(e.Extended["X-Amz-Meta-new"]) != "2" {
		t.Errorf("new meta not set: %v", e.Extended)
	}
}

// withRing attaches a Dlm whose ring contains exactly the given servers and sets
// the filer's own host, so route_key resolution in ObjectTransaction is decided
// by who owns the single-server ring.
func withRing(fs *FilerServer, self pb.ServerAddress, servers ...pb.ServerAddress) {
	dlm := lock_manager.NewDistributedLockManager(self)
	dlm.LockRing.SetSnapshot(servers, 1)
	fs.filer.Dlm = dlm
	fs.option.Host = self
}

// When this filer owns route_key, the transaction applies locally rather than
// forwarding to itself.
func TestObjectTransactionRouteKeyOwnerAppliesLocally(t *testing.T) {
	self := pb.ServerAddress("localhost:1")
	fs, store := newTxnTestServer(map[string]*filer.Entry{
		"/buckets/b/obj": {FullPath: "/buckets/b/obj", Attr: filer.Attr{Inode: 1, Mode: 0644}},
	})
	withRing(fs, self, self)

	resp, err := fs.ObjectTransaction(context.Background(), &filer_pb.ObjectTransactionRequest{
		LockKey:  "/buckets/b/obj",
		RouteKey: "s3.object.write:/buckets/b/obj",
		Mutations: []*filer_pb.ObjectMutation{{
			Type: filer_pb.ObjectMutation_PATCH_EXTENDED, Directory: "/buckets/b", Name: "obj",
			SetExtended: map[string][]byte{"X-Amz-Meta-k": []byte("v")},
		}},
	})
	if err != nil || resp.Error != "" {
		t.Fatalf("txn failed: err=%v resp=%q", err, resp.Error)
	}
	if string(store.entries["/buckets/b/obj"].Extended["X-Amz-Meta-k"]) != "v" {
		t.Errorf("mutation should have applied locally: %v", store.entries["/buckets/b/obj"].Extended)
	}
}

// A forwarded transaction (is_moved) applies locally even when the ring names a
// different owner: is_moved bounds forwarding to a single hop, so two filers that
// disagree on the owner during a ring change cannot loop. If is_moved were
// ignored, this would attempt to dial the bogus owner instead of applying.
func TestObjectTransactionIsMovedSkipsForward(t *testing.T) {
	self := pb.ServerAddress("localhost:1")
	other := pb.ServerAddress("localhost:2")
	fs, store := newTxnTestServer(map[string]*filer.Entry{
		"/buckets/b/obj": {FullPath: "/buckets/b/obj", Attr: filer.Attr{Inode: 1, Mode: 0644}},
	})
	withRing(fs, self, other) // ring owner is "other", not self

	resp, err := fs.ObjectTransaction(context.Background(), &filer_pb.ObjectTransactionRequest{
		LockKey:  "/buckets/b/obj",
		RouteKey: "s3.object.write:/buckets/b/obj",
		IsMoved:  true,
		Mutations: []*filer_pb.ObjectMutation{{
			Type: filer_pb.ObjectMutation_PATCH_EXTENDED, Directory: "/buckets/b", Name: "obj",
			SetExtended: map[string][]byte{"X-Amz-Meta-k": []byte("v")},
		}},
	})
	if err != nil || resp.Error != "" {
		t.Fatalf("txn failed: err=%v resp=%q", err, resp.Error)
	}
	if string(store.entries["/buckets/b/obj"].Extended["X-Amz-Meta-k"]) != "v" {
		t.Errorf("forwarded txn should apply locally: %v", store.entries["/buckets/b/obj"].Extended)
	}
}

// End-to-end forward hop: a non-owner filer dials the ring owner and the owner
// applies the transaction. The owner's own ring points back at the (bogus)
// sender, so it would re-forward and fail to dial unless is_moved is set on the
// forwarded request — making this also assert that one-hop bound over the wire.
func TestObjectTransactionForwardsToOwner(t *testing.T) {
	owner, ownerStore := newTxnTestServer(map[string]*filer.Entry{
		"/buckets/b/obj": {FullPath: "/buckets/b/obj", Attr: filer.Attr{Inode: 1, Mode: 0644}},
	})

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	// Pin the grpc port to the real listener (ToGrpcAddress otherwise adds the
	// +10000 convention, which dials nothing).
	port := lis.Addr().(*net.TCPAddr).Port
	ownerAddr := pb.NewServerAddressWithGrpcPort(lis.Addr().String(), port)
	sender := pb.ServerAddress("127.0.0.1:1") // bogus: nothing listens here

	// owner's ring points back at the sender; only is_moved keeps it from
	// re-forwarding to (and failing to dial) that bogus address.
	withRing(owner, ownerAddr, sender)
	owner.grpcDialOption = grpc.WithTransportCredentials(insecure.NewCredentials())

	srv := grpc.NewServer()
	filer_pb.RegisterSeaweedFilerServer(srv, owner)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)

	self, selfStore := newTxnTestServer(nil)
	withRing(self, sender, ownerAddr) // ring owner is the real owner; self forwards
	self.grpcDialOption = grpc.WithTransportCredentials(insecure.NewCredentials())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := self.ObjectTransaction(ctx, &filer_pb.ObjectTransactionRequest{
		LockKey:  "/buckets/b/obj",
		RouteKey: "s3.object.write:/buckets/b/obj",
		Mutations: []*filer_pb.ObjectMutation{{
			Type: filer_pb.ObjectMutation_PATCH_EXTENDED, Directory: "/buckets/b", Name: "obj",
			SetExtended: map[string][]byte{"X-Amz-Meta-k": []byte("v")},
		}},
	})
	if err != nil || resp.Error != "" {
		t.Fatalf("forwarded txn failed: err=%v resp=%q", err, resp.Error)
	}
	if string(ownerStore.entries["/buckets/b/obj"].Extended["X-Amz-Meta-k"]) != "v" {
		t.Errorf("owner should have applied the forwarded mutation: %v", ownerStore.entries["/buckets/b/obj"].Extended)
	}
	if _, ok := selfStore.entries["/buckets/b/obj"]; ok {
		t.Errorf("non-owner must forward, not apply locally")
	}
}
