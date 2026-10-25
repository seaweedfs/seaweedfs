package filer

import (
	"context"
	"errors"
	"strings"
	"sync"
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
	if backOn.pathCommitGen.Load() <= offTs || storeHasKey(store, metaPathOffKey) {
		t.Fatalf("generation = %d, off marker kept = %v; want a fence past %d and the marker cleared",
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
	if backOn.pathCommitGen.Load() <= offTs || storeHasKey(store, metaPathOffKey) {
		t.Fatalf("generation = %d, off marker kept = %v; want a fence past %d and the marker cleared",
			backOn.pathCommitGen.Load(), storeHasKey(store, metaPathOffKey), offTs)
	}
}

// Filers racing on the same off marker land on one generation — the marker's
// own timestamp. The marker the generation was bumped for reappearing is
// dropped, but a different marker is consumed however old it reads: a new off
// period can run behind the stored generation when a clock lags. A bump for
// the same marker landing mid-check is adopted rather than counted past.
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

	// the marker the stored generation was bumped for reappearing — left by
	// a delete that failed on an earlier pass — is dropped without a bump
	leftover := make([]byte, 8)
	util.Uint64toBytes(leftover, offTs)
	if err := store.KvPut(ctx, []byte(metaPathOffKey), leftover); err != nil {
		t.Fatalf("KvPut: %v", err)
	}
	third := newReplayTestFiler(store)
	if err := third.syncPathCommitGen(ctx); err != nil {
		t.Fatalf("syncPathCommitGen: %v", err)
	}
	if third.pathCommitGen.Load() != offTs || storeHasKey(store, metaPathOffKey) {
		t.Fatalf("generation = %d, off marker kept = %v; want %d and the leftover marker dropped",
			third.pathCommitGen.Load(), storeHasKey(store, metaPathOffKey), offTs)
	}

	// a new marker reading older than the stored generation — an off period
	// written on a host whose clock lags — is still unrecorded time: it is
	// consumed past the stored generation, not dropped
	stale := make([]byte, 8)
	util.Uint64toBytes(stale, offTs-1)
	if err := store.KvPut(ctx, []byte(metaPathOffKey), stale); err != nil {
		t.Fatalf("KvPut: %v", err)
	}
	fourth := newReplayTestFiler(store)
	if err := fourth.syncPathCommitGen(ctx); err != nil {
		t.Fatalf("syncPathCommitGen: %v", err)
	}
	if fourth.pathCommitGen.Load() != offTs+1 || storeHasKey(store, metaPathOffKey) {
		t.Fatalf("generation = %d, off marker kept = %v; want %d and the stale marker consumed",
			fourth.pathCommitGen.Load(), storeHasKey(store, metaPathOffKey), offTs+1)
	}

	// a filer syncing with the marker gone adopts the stored generation —
	// the value's trailing field — not the consumed marker it also names
	restarted := newReplayTestFiler(store)
	if err := restarted.syncPathCommitGen(ctx); err != nil {
		t.Fatalf("syncPathCommitGen: %v", err)
	}
	if restarted.pathCommitGen.Load() != offTs+1 {
		t.Fatalf("generation = %d, want the stored %d", restarted.pathCommitGen.Load(), offTs+1)
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
				won := make([]byte, 16)
				util.Uint64toBytes(won, newTs)
				util.Uint64toBytes(won[8:], newTs+1)
				store.kv[metaPathGenKey] = won
			}
		}
	}
	fifth := newReplayTestFiler(store)
	if err := fifth.syncPathCommitGen(ctx); err != nil {
		t.Fatalf("syncPathCommitGen: %v", err)
	}
	store.kvGetHook = nil
	if got := fifth.pathCommitGen.Load(); got != newTs+1 {
		t.Fatalf("generation = %d, want the concurrently stored %d", got, newTs+1)
	}

	// an off-state filer enabling after the marker was consumed still fences
	// its own off window: the current time names the fence
	sixth := newReplayTestFiler(store)
	sixth.pathCommitRecordsOff.Store(true)
	if err := sixth.enablePathCommitRecords(ctx); err != nil {
		t.Fatalf("enablePathCommitRecords: %v", err)
	}
	if got := sixth.pathCommitGen.Load(); got <= newTs+1 {
		t.Fatalf("generation = %d, want a fence past the stored %d", got, newTs+1)
	}
}

// A sync whose marker read ran behind a later off period's whole consume
// cycle must not bump for its stale read: the stored generation already
// covers a different marker, and the marker live by the re-read is the one
// consumed instead.
func TestPathCommitGenStaleMarkerRead(t *testing.T) {
	store := newStubFilerStore()
	ctx := context.Background()

	offMarker := func(ts uint64) []byte {
		marker := make([]byte, 8)
		util.Uint64toBytes(marker, ts)
		return marker
	}
	t1, t3 := uint64(1000), uint64(3000)

	// two off periods consumed back to back: the stored generation covers
	// the second one's marker
	for i, name := range []string{"first", "second"} {
		filer := newReplayTestFiler(store)
		if err := store.KvPut(ctx, []byte(metaPathOffKey), offMarker(t1+uint64(i)*1000)); err != nil {
			t.Fatalf("KvPut: %v", err)
		}
		if err := filer.syncPathCommitGen(ctx); err != nil {
			t.Fatalf("%s syncPathCommitGen: %v", name, err)
		}
		if got := filer.pathCommitGen.Load(); got != t1+uint64(i)*1000 {
			t.Fatalf("%s generation = %d, want %d", name, got, t1+uint64(i)*1000)
		}
	}

	// a third off period is now live, but the racy sync's first read still
	// shows the consumed first marker; the re-read must see the live one
	third := offMarker(t3)
	offReads := 0
	store.kvGetHook = func(key []byte) {
		if string(key) == metaPathOffKey {
			offReads++
			if offReads == 2 {
				store.kv[metaPathOffKey] = third
			}
		}
	}
	if err := store.KvPut(ctx, []byte(metaPathOffKey), offMarker(t1)); err != nil {
		t.Fatalf("KvPut: %v", err)
	}
	stale := newReplayTestFiler(store)
	if err := stale.syncPathCommitGen(ctx); err != nil {
		t.Fatalf("syncPathCommitGen: %v", err)
	}
	store.kvGetHook = nil
	if got := stale.pathCommitGen.Load(); got != t3 {
		t.Fatalf("generation = %d, want the live marker %d — a stale read must not bump", got, t3)
	}
	if storeHasKey(store, metaPathOffKey) {
		t.Fatal("the live marker was left standing after it was consumed")
	}
}

// Filers consuming one off marker in parallel settle on a single generation:
// every bumper either stores the marker's own timestamp or adopts the value
// a racing bumper already stored for it, and a filer whose marker read runs
// behind a winner's delete adopts the stored generation instead.
func TestPathCommitGenConsumedConcurrently(t *testing.T) {
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

	const racers = 4
	filers := make([]*Filer, racers)
	errs := make([]error, racers)
	var wg sync.WaitGroup
	for i := range filers {
		filers[i] = newReplayTestFiler(store)
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = filers[i].syncPathCommitGen(ctx)
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("syncPathCommitGen %d: %v", i, err)
		}
		if got := filers[i].pathCommitGen.Load(); got != offTs {
			t.Fatalf("filer %d generation = %d, want the consumed marker %d", i, got, offTs)
		}
	}
	if storeHasKey(store, metaPathOffKey) {
		t.Fatal("off marker left after the racing bumps")
	}
}
