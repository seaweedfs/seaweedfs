package weed_server

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// RECOMPUTE_LATEST must emit a metadata notification for the pointer it writes.
// Without it the recomputed latest-version pointer lives only in the filer that
// processed the mutation, so peer filers never learn the current version and
// their ListObjects undercount versioned buckets.
func TestRecomputeLatestEmitsPointerUpdateEvent(t *testing.T) {
	store := newRenameTestStore()
	store.entries["/buckets/b/obj.versions"] = newDirectoryEntry("/buckets/b/obj.versions", 10)
	version := newFileEntry("/buckets/b/obj.versions/v_123", 11)
	version.Extended = map[string][]byte{"vid": []byte("123")}
	store.entries["/buckets/b/obj.versions/v_123"] = version

	queue := &captureQueue{}
	swapNotificationQueue(t, queue)

	server := &FilerServer{filer: newRenameTestFiler(store)}
	m := &filer_pb.ObjectMutation{
		Type:      filer_pb.ObjectMutation_RECOMPUTE_LATEST,
		Directory: "/buckets/b",
		Name:      "obj.versions",
		Recompute: &filer_pb.Recompute{
			ScanDir:      "/buckets/b/obj.versions",
			Descending:   true,
			NameToKey:    "latest-file",
			CopyExtended: map[string]string{"latest-vid": "vid"},
		},
	}
	if err := server.applyObjectMutation(context.Background(), m, false, nil); err != nil {
		t.Fatalf("applyObjectMutation: %v", err)
	}

	// Pointer is persisted locally.
	ptr, err := store.FindEntry(context.Background(), "/buckets/b/obj.versions")
	if err != nil {
		t.Fatalf("find pointer: %v", err)
	}
	if got := string(ptr.Extended["latest-vid"]); got != "123" {
		t.Fatalf("pointer latest-vid = %q, want 123", got)
	}

	// ...and it is announced so peers replicate it.
	events := queue.snapshot()
	if len(events) != 1 {
		t.Fatalf("event count = %d, want 1", len(events))
	}
	e := events[0]
	if e.notification.NewEntry == nil || e.notification.NewEntry.Name != "obj.versions" {
		t.Fatalf("event new entry = %+v, want obj.versions", e.notification.NewEntry)
	}
	if got := string(e.notification.NewEntry.Extended["latest-vid"]); got != "123" {
		t.Fatalf("event latest-vid = %q, want 123", got)
	}
	if e.notification.OldEntry != nil && len(e.notification.OldEntry.Extended["latest-vid"]) != 0 {
		t.Fatalf("event old entry already had pointer: %+v", e.notification.OldEntry)
	}
}

// A pointer flip that demotes the prior latest must announce both the pointer
// update and the demote stamp, so a peer's view of both entries stays correct.
func TestRecomputeLatestDemoteEmitsEvent(t *testing.T) {
	store := newRenameTestStore()
	pointer := newDirectoryEntry("/buckets/b/obj.versions", 10)
	pointer.Extended = map[string][]byte{"latest-file": []byte("v_100"), "latest-vid": []byte("100")}
	store.entries["/buckets/b/obj.versions"] = pointer

	prior := newFileEntry("/buckets/b/obj.versions/v_100", 11)
	prior.Extended = map[string][]byte{"vid": []byte("100")}
	store.entries["/buckets/b/obj.versions/v_100"] = prior

	latest := newFileEntry("/buckets/b/obj.versions/v_200", 12)
	latest.Extended = map[string][]byte{"vid": []byte("200")}
	store.entries["/buckets/b/obj.versions/v_200"] = latest

	queue := &captureQueue{}
	swapNotificationQueue(t, queue)

	server := &FilerServer{filer: newRenameTestFiler(store)}
	m := &filer_pb.ObjectMutation{
		Type:      filer_pb.ObjectMutation_RECOMPUTE_LATEST,
		Directory: "/buckets/b",
		Name:      "obj.versions",
		Recompute: &filer_pb.Recompute{
			ScanDir:      "/buckets/b/obj.versions",
			Descending:   true,
			NameToKey:    "latest-file",
			CopyExtended: map[string]string{"latest-vid": "vid"},
			DemoteKey:    "noncurrent-since",
			DemoteValue:  []byte("999"),
		},
	}
	if err := server.applyObjectMutation(context.Background(), m, false, nil); err != nil {
		t.Fatalf("applyObjectMutation: %v", err)
	}

	gotPtr, err := store.FindEntry(context.Background(), "/buckets/b/obj.versions")
	if err != nil {
		t.Fatalf("find pointer: %v", err)
	}
	if got := string(gotPtr.Extended["latest-file"]); got != "v_200" {
		t.Fatalf("pointer latest-file = %q, want v_200", got)
	}
	gotPrior, err := store.FindEntry(context.Background(), "/buckets/b/obj.versions/v_100")
	if err != nil {
		t.Fatalf("find prior: %v", err)
	}
	if got := string(gotPrior.Extended["noncurrent-since"]); got != "999" {
		t.Fatalf("prior noncurrent-since = %q, want 999", got)
	}

	var sawPointer, sawDemote bool
	for _, e := range queue.snapshot() {
		if e.notification.NewEntry == nil {
			continue
		}
		switch e.notification.NewEntry.Name {
		case "obj.versions":
			if string(e.notification.NewEntry.Extended["latest-file"]) == "v_200" {
				sawPointer = true
			}
		case "v_100":
			if string(e.notification.NewEntry.Extended["noncurrent-since"]) == "999" {
				sawDemote = true
			}
		}
	}
	if !sawPointer {
		t.Fatalf("missing pointer-flip event: %+v", queue.snapshot())
	}
	if !sawDemote {
		t.Fatalf("missing demote event: %+v", queue.snapshot())
	}
}
