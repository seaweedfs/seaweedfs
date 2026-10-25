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

func TestPathCommitRecordsWrittenByDefault(t *testing.T) {
	store := newStubFilerStore()
	f := newReplayTestFiler(store)

	f.noteCommittedEvent(deleteEvent("/dir", "file", time.Now().UnixNano()))

	if n := pathCommitRecordCount(store); n != 2 {
		t.Fatalf("records = %d, want 2", n)
	}
}

// Until a peer with its own store is observed, only deletes are recorded
// (a deleted path has no mtime to fall back to), the store is marked off,
// and records written before are not read.
func TestPathCommitRecordsOffUntilPeerSeen(t *testing.T) {
	store := newStubFilerStore()
	f := newReplayTestFiler(store)
	now := time.Now()
	path := util.NewFullPath("/dir", "file")
	f.notePathChange(path, now.UnixNano(), true)

	f.pathCommitRecordsOff.Store(true)
	if err := f.markPathCommitRecordsOff(context.Background()); err != nil {
		t.Fatalf("markPathCommitRecordsOff: %v", err)
	}
	if !storeHasKey(store, metaPathOffKey) {
		t.Fatal("no off marker in the store")
	}
	f.noteCommittedEvent(overwriteEvent("/dir", "other", now.UnixNano(), "x"))
	f.noteCommittedEvent(deleteEvent("/dir", "gone", now.UnixNano()))
	if n := pathCommitRecordCount(store); n != 3 {
		t.Fatalf("records = %d, want the 2 written before plus the delete tombstone", n)
	}
	if _, err := store.KvGet(context.Background(), metaPathKey(metaPathDelPrefix, util.FullPath("/dir/gone"))); err != nil {
		t.Fatalf("delete while off left no tombstone: %v", err)
	}

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

// A path deleted while the records were off stays fenced by its tombstone
// once they come on: an older replayed create must not resurrect it.
func TestPathCommitRecordsOffDeleteStillFences(t *testing.T) {
	store := newStubFilerStore()
	ctx := context.Background()
	now := time.Now()
	gone := util.NewFullPath("/dir", "gone")

	off := newReplayTestFiler(store)
	off.pathCommitRecordsOff.Store(true)
	if err := off.markPathCommitRecordsOff(ctx); err != nil {
		t.Fatalf("markPathCommitRecordsOff: %v", err)
	}
	off.noteCommittedEvent(deleteEvent("/dir", "gone", now.UnixNano()))

	on := newReplayTestFiler(store)
	on.pathCommitRecordsOff.Store(true)
	if err := on.enablePathCommitRecords(ctx); err != nil {
		t.Fatalf("enablePathCommitRecords: %v", err)
	}
	createEvent := &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		EventNotification: &filer_pb.EventNotification{
			NewEntry: &filer_pb.Entry{Name: "gone", Content: []byte("stale resurrect")},
		},
		TsNs: now.Add(-time.Minute).UnixNano(),
	}
	if err := on.Replay(createEvent); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if _, err := store.FindEntry(ctx, gone); err == nil {
		t.Fatal("a replayed create resurrected a path deleted while the records were off")
	}
}

// Once a peer with its own store is observed the records come on: a record
// older than a change made while off must not let an older replayed peer
// event overwrite that change, and records written right after count at once.
func TestPathCommitRecordsOnAfterPeerSeen(t *testing.T) {
	store := newStubFilerStore()
	ctx := context.Background()
	now := time.Now()
	path := util.NewFullPath("/dir", "file")

	on := newReplayTestFiler(store)
	on.notePathChange(path, now.Add(-time.Hour).UnixNano(), false)

	off := newReplayTestFiler(store)
	off.pathCommitRecordsOff.Store(true)
	if err := off.markPathCommitRecordsOff(ctx); err != nil {
		t.Fatalf("markPathCommitRecordsOff: %v", err)
	}
	store.entries[string(path)] = &Entry{FullPath: path, Attr: Attr{Mtime: now}, Content: []byte("changed while off")}
	off.noteCommittedEvent(overwriteEvent("/dir", "file", now.UnixNano(), "changed while off"))
	offTs := storeGetUint64(store, metaPathOffKey)

	backOn := newReplayTestFiler(store)
	backOn.pathCommitRecordsOff.Store(true)
	if err := backOn.enablePathCommitRecords(ctx); err != nil {
		t.Fatalf("enablePathCommitRecords: %v", err)
	}
	if backOn.pathCommitGen.Load() != offTs || storeHasKey(store, metaPathOffKey) {
		t.Fatalf("generation = %d, off marker kept = %v; want %d and the marker cleared",
			backOn.pathCommitGen.Load(), storeHasKey(store, metaPathOffKey), offTs)
	}
	if !storeHasKey(store, metaPathReplKey) {
		t.Fatal("no replication marker in the store")
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

// The latch is stored, so a restarted filer keeps writing records under the
// same generation without waiting for the peer's signature again.
func TestPathCommitRecordsLatchedAcrossRestart(t *testing.T) {
	store := newStubFilerStore()
	ctx := context.Background()

	first := newReplayTestFiler(store)
	first.pathCommitRecordsOff.Store(true)
	if err := first.markPathCommitRecordsOff(ctx); err != nil {
		t.Fatalf("markPathCommitRecordsOff: %v", err)
	}
	if err := first.enablePathCommitRecords(ctx); err != nil {
		t.Fatalf("enablePathCommitRecords: %v", err)
	}
	first.notePathChange(util.FullPath("/dir/file"), 100, false)

	restarted := newReplayTestFiler(store)
	if _, err := store.KvGet(ctx, []byte(metaPathReplKey)); err != nil {
		t.Fatalf("replication marker missing: %v", err)
	}
	// records already on (flag never set off): sync adopts the stored
	// generation, no off marker means no bump
	restarted.syncPathCommitGen(ctx)
	if restarted.pathCommitGen.Load() != first.pathCommitGen.Load() {
		t.Fatalf("generation = %d, want %d", restarted.pathCommitGen.Load(), first.pathCommitGen.Load())
	}
	restarted.notePathChange(util.FullPath("/dir/other"), 200, false)
	if n := pathCommitRecordCount(store); n != 2 {
		t.Fatalf("records = %d, want 2", n)
	}
	tsNs, err := restarted.localCommitTsNs(ctx, util.FullPath("/dir/file"))
	if err != nil || tsNs != 100 {
		t.Fatalf("localCommitTsNs = %d, %v; want 100", tsNs, err)
	}
}

// A filer sharing a store latches on the peer's replication marker even
// before it discovers the peer itself.
func TestPathCommitRecordsConvergeSharedStore(t *testing.T) {
	store := newStubFilerStore()
	ctx := context.Background()

	a := newReplayTestFiler(store)
	a.pathCommitRecordsOff.Store(true)
	if err := a.checkPathCommitRecords(ctx); err != nil {
		t.Fatalf("checkPathCommitRecords: %v", err)
	}
	if !storeHasKey(store, metaPathOffKey) {
		t.Fatal("no off marker in the store")
	}
	if err := a.enablePathCommitRecords(ctx); err != nil {
		t.Fatalf("enablePathCommitRecords: %v", err)
	}
	if a.pathCommitGen.Load() == 0 {
		t.Fatal("records on without a generation")
	}

	b := newReplayTestFiler(store)
	b.pathCommitRecordsOff.Store(true)
	if err := b.checkPathCommitRecords(ctx); err != nil {
		t.Fatalf("checkPathCommitRecords: %v", err)
	}
	if b.pathCommitRecordsOff.Load() {
		t.Fatal("sharer did not latch onto the replication marker")
	}
	b.notePathChange(util.FullPath("/dir/file"), 100, false)
	if n := pathCommitRecordCount(store); n != 1 {
		t.Fatalf("records = %d, want 1", n)
	}
}

// The off marker must be stored before the filer serves, and a failed bump
// keeps the marker and the records off for the next try.
func TestPathCommitRecordsMarkerFailures(t *testing.T) {
	store := newStubFilerStore()
	ctx := context.Background()

	off := newReplayTestFiler(store)
	off.pathCommitRecordsOff.Store(true)
	store.setKvPutErr(errors.New("store down"))
	if err := off.markPathCommitRecordsOff(ctx); err == nil {
		t.Fatal("markPathCommitRecordsOff succeeded with the store down")
	}
	store.setKvPutErr(nil)
	if err := off.markPathCommitRecordsOff(ctx); err != nil {
		t.Fatalf("markPathCommitRecordsOff: %v", err)
	}
	offTs := storeGetUint64(store, metaPathOffKey)

	backOn := newReplayTestFiler(store)
	backOn.pathCommitRecordsOff.Store(true)
	store.setKvPutErr(errors.New("store down"))
	if err := backOn.enablePathCommitRecords(ctx); err == nil {
		t.Fatal("enablePathCommitRecords succeeded without storing the bump")
	}
	if !backOn.pathCommitRecordsOff.Load() || !storeHasKey(store, metaPathOffKey) {
		t.Fatal("records enabled or off marker cleared although the bump was not stored")
	}
	store.setKvPutErr(nil)
	if err := backOn.enablePathCommitRecords(ctx); err != nil {
		t.Fatalf("enablePathCommitRecords: %v", err)
	}
	if backOn.pathCommitGen.Load() != offTs || storeHasKey(store, metaPathOffKey) {
		t.Fatalf("generation = %d, off marker kept = %v; want %d and the marker cleared",
			backOn.pathCommitGen.Load(), storeHasKey(store, metaPathOffKey), offTs)
	}
}

// Filers racing on the same off marker land on one generation — the marker's
// own timestamp — a marker older than the stored generation is cleaned rather
// than consumed again, and a bump landing mid-check is adopted rather than
// counted past.
func TestPathCommitGenConsumedTogether(t *testing.T) {
	store := newStubFilerStore()
	ctx := context.Background()

	off := newReplayTestFiler(store)
	off.pathCommitRecordsOff.Store(true)
	if err := off.markPathCommitRecordsOff(ctx); err != nil {
		t.Fatalf("markPathCommitRecordsOff: %v", err)
	}
	offTs := storeGetUint64(store, metaPathOffKey)
	if offTs == 0 {
		t.Fatal("empty off marker")
	}

	// two filers consume the marker back to back: one takes its timestamp,
	// the other adopts it once the marker is gone
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

	// an off-state filer enabling after the marker was consumed still fences
	// its own off window: the current time stands in for the missing marker
	fifth := newReplayTestFiler(store)
	fifth.pathCommitRecordsOff.Store(true)
	if err := fifth.enablePathCommitRecords(ctx); err != nil {
		t.Fatalf("enablePathCommitRecords: %v", err)
	}
	if got := fifth.pathCommitGen.Load(); got <= newTs+1 {
		t.Fatalf("generation = %d, want a fence past the stored %d", got, newTs+1)
	}
}
