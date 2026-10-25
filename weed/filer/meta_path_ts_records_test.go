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

func storeGetUint64(store *stubFilerStore, key string) uint64 {
	store.mu.Lock()
	defer store.mu.Unlock()
	if v := store.kv[key]; len(v) >= 8 {
		return util.BytesToUint64(v)
	}
	return 0
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
	offTs := storeGetUint64(store, metaPathOffKey)

	// records back on, as a filer starting with them on
	backOn := newReplayTestFiler(store)
	if err := backOn.syncPathCommitGen(ctx); err != nil {
		t.Fatalf("syncPathCommitGen: %v", err)
	}
	if backOn.pathCommitGen.Load() != offTs || storeHasKey(store, metaPathOffKey) {
		t.Fatalf("generation = %d, off marker kept = %v; want %d and the marker cleared",
			backOn.pathCommitGen.Load(), storeHasKey(store, metaPathOffKey), offTs)
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
	offTs := storeGetUint64(store, metaPathOffKey)
	store.setKvPutErr(nil)
	if err := backOn.syncPathCommitGen(ctx); err != nil {
		t.Fatalf("syncPathCommitGen: %v", err)
	}
	if backOn.pathCommitGen.Load() != offTs || storeHasKey(store, metaPathOffKey) {
		t.Fatalf("generation = %d, off marker kept = %v; want %d and the marker cleared",
			backOn.pathCommitGen.Load(), storeHasKey(store, metaPathOffKey), offTs)
	}
}

// TestPathCommitGenConsumedTogether: filers racing on the same off marker
// land on one generation — the marker's own timestamp — a marker older than
// the stored generation is cleaned rather than consumed again, and a bump
// landing mid-check is adopted rather than counted past.
func TestPathCommitGenConsumedTogether(t *testing.T) {
	store := newStubFilerStore()
	ctx := context.Background()

	off := newReplayTestFiler(store)
	off.pathCommitRecordsOff = true
	if err := off.markPathCommitRecordsOff(ctx); err != nil {
		t.Fatalf("markPathCommitRecordsOff: %v", err)
	}
	offTs := storeGetUint64(store, metaPathOffKey)
	if offTs == 0 {
		t.Fatal("empty off marker")
	}

	// two filers consume the marker back to back: both take its timestamp
	for i, name := range []string{"first", "second"} {
		filer := newReplayTestFiler(store)
		if err := filer.syncPathCommitGen(ctx); err != nil {
			t.Fatalf("%s syncPathCommitGen: %v", name, err)
		}
		if got := filer.pathCommitGen.Load(); got != offTs {
			t.Fatalf("%s generation = %d, want the consumed marker %d (iteration %d)", name, got, offTs, i)
		}
	}

	// a marker older than the stored generation is dropped without a bump
	stale := make([]byte, 8)
	util.Uint64toBytes(stale, offTs-1)
	if err := store.KvPut(ctx, []byte(metaPathOffKey), stale); err != nil {
		t.Fatalf("KvPut: %v", err)
	}
	third := newReplayTestFiler(store)
	if err := third.syncPathCommitGen(ctx); err != nil {
		t.Fatalf("syncPathCommitGen: %v", err)
	}
	if third.pathCommitGen.Load() != offTs || storeHasKey(store, metaPathOffKey) {
		t.Fatalf("generation = %d, off marker kept = %v; want %d and the stale marker dropped",
			third.pathCommitGen.Load(), storeHasKey(store, metaPathOffKey), offTs)
	}

	// a fresh marker consumed while this sync is mid-check: the stored value
	// is adopted, not counted past — the hook lands the concurrent bump on
	// the second generation read
	newTs := offTs + 100
	fresh := make([]byte, 8)
	util.Uint64toBytes(fresh, newTs)
	if err := store.KvPut(ctx, []byte(metaPathOffKey), fresh); err != nil {
		t.Fatalf("KvPut: %v", err)
	}
	reads := 0
	store.kvGetHook = func(key []byte) {
		if string(key) == metaPathGenKey {
			reads++
			if reads == 2 {
				won := make([]byte, 8)
				util.Uint64toBytes(won, newTs+1)
				store.kv[metaPathGenKey] = won
			}
		}
	}
	fourth := newReplayTestFiler(store)
	if err := fourth.syncPathCommitGen(ctx); err != nil {
		t.Fatalf("syncPathCommitGen: %v", err)
	}
	store.kvGetHook = nil
	if got := fourth.pathCommitGen.Load(); got != newTs+1 {
		t.Fatalf("generation = %d, want the concurrently stored %d", got, newTs+1)
	}
}
