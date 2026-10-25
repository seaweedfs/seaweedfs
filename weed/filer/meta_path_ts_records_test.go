package filer

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// pathCommitRecordCount counts the per-path commit records in the stub store.
func pathCommitRecordCount(store *stubFilerStore) int {
	store.mu.Lock()
	defer store.mu.Unlock()
	n := 0
	for k := range store.kv {
		if strings.HasPrefix(k, metaPathTsPrefix) || strings.HasPrefix(k, metaPathDelPrefix) {
			n++
		}
	}
	return n
}

func storeHasKey(store *stubFilerStore, key string) bool {
	store.mu.Lock()
	defer store.mu.Unlock()
	_, found := store.kv[key]
	return found
}

// TestPathCommitRecordsWrittenByDefault: with the option unset, a delete
// records both the change and the delete.
func TestPathCommitRecordsWrittenByDefault(t *testing.T) {
	store := newStubFilerStore()
	f := newReplayTestFiler(store)

	f.noteCommittedEvent(deleteEvent("/dir", "file", time.Now().UnixNano()))

	if n := pathCommitRecordCount(store); n != 2 {
		t.Fatalf("records = %d, want 2", n)
	}
}

// TestPathCommitRecordsOff: with the option off the store is marked before
// anything is served, nothing is recorded, and records written before are not
// read.
func TestPathCommitRecordsOff(t *testing.T) {
	store := newStubFilerStore()
	f := newReplayTestFiler(store)
	now := time.Now()
	path := util.NewFullPath("/dir", "file")
	// records written while the option was still on
	f.notePathChange(path, now.UnixNano(), true)

	f.pathCommitRecordsOff = true
	if err := f.markPathCommitRecordsOff(context.Background()); err != nil {
		t.Fatalf("markPathCommitRecordsOff: %v", err)
	}
	if !storeHasKey(store, metaPathOffKey) {
		t.Fatal("no off marker in the store")
	}
	f.noteCommittedEvent(overwriteEvent("/dir", "other", now.UnixNano(), "x"))
	f.noteCommittedEvent(deleteEvent("/dir", "gone", now.UnixNano()))
	if n := pathCommitRecordCount(store); n != 2 {
		t.Fatalf("records = %d, want only the 2 written before the option went off", n)
	}

	// the old records no longer count: the path orders by its entry, as an
	// entry written before records existed does
	mtime := now.Add(-time.Hour)
	store.entries[string(path)] = &Entry{FullPath: path, Attr: Attr{Mtime: mtime}}
	tsNs, err := f.localCommitTsNs(context.Background(), path)
	if err != nil {
		t.Fatalf("localCommitTsNs: %v", err)
	}
	if tsNs != mtime.UnixNano() {
		t.Fatalf("localCommitTsNs = %d, want the entry's mtime %d", tsNs, mtime.UnixNano())
	}
}

// TestPathCommitRecordsBackOn: once the option is back on, a record older than
// a change made while it was off must not let an older replayed peer event
// overwrite that change, and records written right after count at once.
func TestPathCommitRecordsBackOn(t *testing.T) {
	store := newStubFilerStore()
	ctx := context.Background()
	now := time.Now()
	path := util.NewFullPath("/dir", "file")

	// records on: the path's change an hour ago is recorded
	on := newReplayTestFiler(store)
	on.notePathChange(path, now.Add(-time.Hour).UnixNano(), false)

	// records off: the path changes again, unrecorded
	off := newReplayTestFiler(store)
	off.pathCommitRecordsOff = true
	if err := off.markPathCommitRecordsOff(ctx); err != nil {
		t.Fatalf("markPathCommitRecordsOff: %v", err)
	}
	store.entries[string(path)] = &Entry{FullPath: path, Attr: Attr{Mtime: now}, Content: []byte("changed while off")}
	off.noteCommittedEvent(overwriteEvent("/dir", "file", now.UnixNano(), "changed while off"))

	// records back on, as a filer starting with them on
	backOn := newReplayTestFiler(store)
	if err := backOn.syncPathCommitGen(ctx); err != nil {
		t.Fatalf("syncPathCommitGen: %v", err)
	}
	if backOn.pathCommitGen.Load() != 1 || storeHasKey(store, metaPathOffKey) {
		t.Fatalf("generation = %d, off marker kept = %v; want 1 and the marker cleared",
			backOn.pathCommitGen.Load(), storeHasKey(store, metaPathOffKey))
	}
	if err := backOn.Replay(overwriteEvent("/dir", "file", now.Add(-30*time.Minute).UnixNano(), "older peer write")); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	entry, err := store.FindEntry(ctx, path)
	if err != nil {
		t.Fatalf("FindEntry: %v", err)
	}
	if string(entry.Content) != "changed while off" {
		t.Fatalf("content = %q, want the change made while the records were off", entry.Content)
	}

	// a delete right after the records came back counts at once: an older
	// replayed create of the path does not bring it back
	gone := util.NewFullPath("/dir", "gone")
	backOn.noteCommittedEvent(deleteEvent("/dir", "gone", now.Add(time.Second).UnixNano()))
	createEvent := &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		EventNotification: &filer_pb.EventNotification{
			NewEntry: &filer_pb.Entry{Name: "gone", Content: []byte("stale resurrect")},
		},
		TsNs: now.UnixNano(),
	}
	if err := backOn.Replay(createEvent); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if _, err := store.FindEntry(ctx, gone); err == nil {
		t.Fatal("an older replayed create resurrected a path deleted after the records came back")
	}
}

// TestPathCommitRecordsMarkerFailures: the off marker must be stored before
// the filer serves, and a failed bump keeps the marker for the next try.
func TestPathCommitRecordsMarkerFailures(t *testing.T) {
	store := newStubFilerStore()
	ctx := context.Background()

	off := newReplayTestFiler(store)
	off.pathCommitRecordsOff = true
	store.setKvPutErr(errors.New("store down"))
	if err := off.markPathCommitRecordsOff(ctx); err == nil {
		t.Fatal("markPathCommitRecordsOff succeeded with the store down")
	}
	store.setKvPutErr(nil)
	if err := off.markPathCommitRecordsOff(ctx); err != nil {
		t.Fatalf("markPathCommitRecordsOff: %v", err)
	}

	backOn := newReplayTestFiler(store)
	store.setKvPutErr(errors.New("store down"))
	if err := backOn.syncPathCommitGen(ctx); err == nil {
		t.Fatal("syncPathCommitGen succeeded without storing the bump")
	}
	if !storeHasKey(store, metaPathOffKey) {
		t.Fatal("off marker cleared although the bump was not stored")
	}
	store.setKvPutErr(nil)
	if err := backOn.syncPathCommitGen(ctx); err != nil {
		t.Fatalf("syncPathCommitGen: %v", err)
	}
	if backOn.pathCommitGen.Load() != 1 || storeHasKey(store, metaPathOffKey) {
		t.Fatalf("generation = %d, off marker kept = %v; want 1 and the marker cleared",
			backOn.pathCommitGen.Load(), storeHasKey(store, metaPathOffKey))
	}
}
