package filer

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// newReplayTestFiler builds the minimum Filer Replay needs over a stub store.
func newReplayTestFiler(store FilerStore) *Filer {
	return &Filer{
		Store:          NewFilerStoreWrapper(store),
		EntryLockTable: util.NewLockTable[util.FullPath](),
	}
}

// flakyReplayStore fails InsertEntry for its first failUntil calls, then
// delegates to the embedded stubFilerStore.
type flakyReplayStore struct {
	*stubFilerStore
	countMu   sync.Mutex
	failUntil int
	err       error
	attempts  int
}

func (s *flakyReplayStore) InsertEntry(ctx context.Context, entry *Entry) error {
	s.countMu.Lock()
	s.attempts++
	fail := s.attempts <= s.failUntil
	s.countMu.Unlock()
	if fail {
		return s.err
	}
	return s.stubFilerStore.InsertEntry(ctx, entry)
}

func (s *flakyReplayStore) attemptCount() int {
	s.countMu.Lock()
	defer s.countMu.Unlock()
	return s.attempts
}

func replayFailureCount(t *testing.T, peer pb.ServerAddress) float64 {
	t.Helper()
	var m dto.Metric
	if err := stats.FilerMetaAggregatorReplayFailures.WithLabelValues(string(peer)).Write(&m); err != nil {
		t.Fatalf("read counter: %v", err)
	}
	return m.GetCounter().GetValue()
}

func quotaChangeEvent(bucket string, quota int64) *filer_pb.SubscribeMetadataResponse {
	return &filer_pb.SubscribeMetadataResponse{
		Directory: "/buckets",
		EventNotification: &filer_pb.EventNotification{
			NewEntry: &filer_pb.Entry{Name: bucket, IsDirectory: true, Quota: quota},
		},
		TsNs: time.Now().UnixNano(),
	}
}

// The old body logged a Replay error and returned, so one transient failure
// permanently diverged the entry from the peer.
func TestReplicateMetadataChangeRetriesTransientFailure(t *testing.T) {
	// int64: untyped, the value defaults to int and overflows a 32-bit build
	const wantQuota int64 = 131072 << 20

	store := &flakyReplayStore{
		stubFilerStore: newStubFilerStore(),
		failUntil:      1,
		err:            errors.New("i/o timeout talking to store"),
	}
	peer := pb.ServerAddress("peer-transient:1")
	f := newReplayTestFiler(store)

	if err := replicateMetadataChange(f, peer, quotaChangeEvent("my-bucket", wantQuota), nil); err != nil {
		t.Fatalf("replicateMetadataChange: %v", err)
	}

	inserted, err := store.FindEntry(context.Background(), util.NewFullPath("/buckets", "my-bucket"))
	if err != nil {
		t.Fatal("expected the entry to be inserted once the transient failure is retried past")
	}
	if inserted.Quota != wantQuota {
		t.Fatalf("quota = %d, want %d", inserted.Quota, wantQuota)
	}
	if got := store.attemptCount(); got < 2 {
		t.Fatalf("attempts = %d, want at least 2", got)
	}
}

// An event that can never replay must fail fast instead of blocking the
// subscribe stream, and must be counted so the divergence is not silent.
func TestReplicateMetadataChangeGivesUpLoudlyOnPermanentFailure(t *testing.T) {
	store := &flakyReplayStore{
		stubFilerStore: newStubFilerStore(),
		failUntil:      1 << 30,
		err:            errors.New("entry checksum mismatch"),
	}
	peer := pb.ServerAddress("peer-permanent:1")
	f := newReplayTestFiler(store)

	before := replayFailureCount(t, peer)

	err := replicateMetadataChange(f, peer, quotaChangeEvent("poison-bucket", 65536<<20), nil)
	if err == nil {
		t.Fatal("expected the exhausted replay error so the caller can pin the peer offset at the failed event")
	}

	if got := store.attemptCount(); got != 1 {
		t.Fatalf("attempts = %d, want exactly 1: a non-transient error must fail fast, not retry", got)
	}
	if got := replayFailureCount(t, peer); got != before+1 {
		t.Fatalf("FilerMetaAggregatorReplayFailures[%s] = %v, want %v", peer, got, before+1)
	}
}

func overwriteEvent(dir, name string, tsNs int64, content string) *filer_pb.SubscribeMetadataResponse {
	return &filer_pb.SubscribeMetadataResponse{
		Directory: dir,
		EventNotification: &filer_pb.EventNotification{
			OldEntry: &filer_pb.Entry{Name: name},
			NewEntry: &filer_pb.Entry{Name: name, Content: []byte(content)},
		},
		TsNs: tsNs,
	}
}

func deleteEvent(dir, name string, tsNs int64) *filer_pb.SubscribeMetadataResponse {
	return &filer_pb.SubscribeMetadataResponse{
		Directory: dir,
		EventNotification: &filer_pb.EventNotification{
			OldEntry: &filer_pb.Entry{Name: name},
		},
		TsNs: tsNs,
	}
}

func TestReplaySkipsEventOlderThanLocalEntry(t *testing.T) {
	store := newStubFilerStore()
	f := newReplayTestFiler(store)
	now := time.Now()
	path := util.NewFullPath("/dir", "file")
	store.entries[string(path)] = &Entry{
		FullPath: path,
		Attr:     Attr{Mtime: now},
		Content:  []byte("newer local write"),
	}

	if err := f.Replay(overwriteEvent("/dir", "file", now.Add(-time.Minute).UnixNano(), "old replayed version")); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	entry, err := store.FindEntry(context.Background(), path)
	if err != nil {
		t.Fatalf("FindEntry: %v", err)
	}
	if string(entry.Content) != "newer local write" {
		t.Fatalf("content = %q, want local write kept", entry.Content)
	}
}

func TestReplayAppliesEventNewerThanLocalEntry(t *testing.T) {
	store := newStubFilerStore()
	f := newReplayTestFiler(store)
	now := time.Now()
	path := util.NewFullPath("/dir", "file")
	store.entries[string(path)] = &Entry{
		FullPath: path,
		Attr:     Attr{Mtime: now.Add(-time.Minute)},
		Content:  []byte("old local write"),
	}

	if err := f.Replay(overwriteEvent("/dir", "file", now.UnixNano(), "newer peer write")); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	entry, err := store.FindEntry(context.Background(), path)
	if err != nil {
		t.Fatalf("FindEntry: %v", err)
	}
	if string(entry.Content) != "newer peer write" {
		t.Fatalf("content = %q, want peer write applied", entry.Content)
	}
}

func TestReplaySkipsDeleteOlderThanLocalEntry(t *testing.T) {
	store := newStubFilerStore()
	f := newReplayTestFiler(store)
	now := time.Now()
	path := util.NewFullPath("/dir", "file")
	store.entries[string(path)] = &Entry{FullPath: path, Attr: Attr{Mtime: now}}

	if err := f.Replay(deleteEvent("/dir", "file", now.Add(-time.Minute).UnixNano())); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if _, err := store.FindEntry(context.Background(), path); err != nil {
		t.Fatalf("newer local entry must survive a stale delete: %v", err)
	}
}

func TestReplayAppliesNewerDelete(t *testing.T) {
	store := newStubFilerStore()
	f := newReplayTestFiler(store)
	now := time.Now()
	path := util.NewFullPath("/dir", "file")
	store.entries[string(path)] = &Entry{FullPath: path, Attr: Attr{Mtime: now.Add(-time.Minute)}}

	if err := f.Replay(deleteEvent("/dir", "file", now.UnixNano())); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if _, err := store.FindEntry(context.Background(), path); err == nil {
		t.Fatal("newer delete must remove the local entry")
	}
}

// A move event touches two paths; the local check applies to each side
// independently, so a newer destination is kept while the source is removed.
func TestReplayMoveChecksEachPathSeparately(t *testing.T) {
	store := newStubFilerStore()
	f := newReplayTestFiler(store)
	now := time.Now()
	oldPath := util.NewFullPath("/a", "file")
	newPath := util.NewFullPath("/b", "file")
	store.entries[string(oldPath)] = &Entry{FullPath: oldPath, Attr: Attr{Mtime: now.Add(-time.Minute)}}
	store.entries[string(newPath)] = &Entry{FullPath: newPath, Attr: Attr{Mtime: now}, Content: []byte("kept")}

	event := &filer_pb.SubscribeMetadataResponse{
		Directory: "/a",
		EventNotification: &filer_pb.EventNotification{
			OldEntry:      &filer_pb.Entry{Name: "file"},
			NewEntry:      &filer_pb.Entry{Name: "file"},
			NewParentPath: "/b",
		},
		TsNs: now.Add(-time.Minute).UnixNano(),
	}
	if err := f.Replay(event); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if _, err := store.FindEntry(context.Background(), oldPath); err == nil {
		t.Fatal("old source entry should be deleted")
	}
	entry, err := store.FindEntry(context.Background(), newPath)
	if err != nil {
		t.Fatalf("FindEntry: %v", err)
	}
	if string(entry.Content) != "kept" {
		t.Fatalf("content = %q, want newer destination kept", entry.Content)
	}
}

// A local delete leaves a recorded tombstone, so a replayed create older than
// the delete must not resurrect the path even though no entry remains to
// compare against.
func TestReplaySkipsCreateOlderThanLocalDelete(t *testing.T) {
	store := newStubFilerStore()
	f := newReplayTestFiler(store)
	now := time.Now()
	path := util.NewFullPath("/dir", "file")

	// The delete removed the entry; its commit record survives.
	f.notePathChange(path, now.UnixNano(), true)

	createEvent := &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		EventNotification: &filer_pb.EventNotification{
			NewEntry: &filer_pb.Entry{Name: "file", Content: []byte("stale resurrect")},
		},
		TsNs: now.Add(-time.Minute).UnixNano(),
	}
	if err := f.Replay(createEvent); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if _, err := store.FindEntry(context.Background(), path); err == nil {
		t.Fatal("stale replayed create resurrected a locally deleted entry")
	}
}

// A deleted directory covers its children: a bulk drop emits no per-child
// events, so the tombstone on the ancestor is what a replayed child create
// must check.
func TestReplaySkipsCreateUnderDeletedAncestor(t *testing.T) {
	store := newStubFilerStore()
	f := newReplayTestFiler(store)
	now := time.Now()

	f.notePathChange("/dir", now.UnixNano(), true)

	createEvent := &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		EventNotification: &filer_pb.EventNotification{
			NewEntry: &filer_pb.Entry{Name: "child", Content: []byte("stale")},
		},
		TsNs: now.Add(-time.Minute).UnixNano(),
	}
	if err := f.Replay(createEvent); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if _, err := store.FindEntry(context.Background(), "/dir/child"); err == nil {
		t.Fatal("stale replayed create resurrected a child of a deleted directory")
	}
}

// A recorded commit timestamp orders replay, not the entry's mtime: a rename
// keeps the source's mtime on the destination, and a mounted client can set
// mtime arbitrarily, so an entry whose mtime postdates the event must still
// be overwritten when its commit record is older.
func TestReplayAppliesWhenCommitRecordIsOlderThanFutureMtime(t *testing.T) {
	store := newStubFilerStore()
	f := newReplayTestFiler(store)
	now := time.Now()
	path := util.NewFullPath("/b", "x")
	store.entries[string(path)] = &Entry{
		FullPath: path,
		Attr:     Attr{Mtime: now.Add(time.Hour)}, // future mtime, e.g. client-set or kept from a rename source
		Content:  []byte("old destination"),
	}
	// The last committed change predates the event.
	f.notePathChange(path, now.Add(-time.Minute).UnixNano(), false)

	event := &filer_pb.SubscribeMetadataResponse{
		Directory: "/a",
		EventNotification: &filer_pb.EventNotification{
			OldEntry:      &filer_pb.Entry{Name: "x"},
			NewEntry:      &filer_pb.Entry{Name: "x", Content: []byte("renamed in")},
			NewParentPath: "/b",
		},
		TsNs: now.UnixNano(),
	}
	if err := f.Replay(event); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	entry, err := store.FindEntry(context.Background(), path)
	if err != nil {
		t.Fatalf("FindEntry: %v", err)
	}
	if string(entry.Content) != "renamed in" {
		t.Fatalf("content = %q, want the rename destination applied", entry.Content)
	}
}

// A store read failure is not a not-found: the replayed change must not be
// applied on the guess that no newer local entry exists.
func TestReplayPropagatesLookupError(t *testing.T) {
	store := newStubFilerStore()
	store.findErr = errors.New("store read failed")
	f := newReplayTestFiler(store)

	err := f.Replay(overwriteEvent("/dir", "file", time.Now().UnixNano(), "v"))
	if err == nil {
		t.Fatal("expected Replay to propagate the FindEntry failure")
	}
}

// The check and the mutation share the per-path entry lock, so a local write
// holding that lock cannot be split by a replayed change.
func TestReplaySerializesWithEntryLock(t *testing.T) {
	store := newStubFilerStore()
	f := newReplayTestFiler(store)
	now := time.Now()
	path := util.NewFullPath("/dir", "file")
	store.entries[string(path)] = &Entry{FullPath: path, Attr: Attr{Mtime: now}}

	// A local mutation holding the path lock; a replay started behind it
	// sees the recorded commit and skips.
	held := f.EntryLockTable.AcquireLock("test local write", path, util.ExclusiveLock)
	f.notePathChange(path, now.UnixNano(), false)

	done := make(chan error, 1)
	go func() {
		done <- f.Replay(overwriteEvent("/dir", "file", now.Add(-time.Minute).UnixNano(), "stale"))
	}()
	select {
	case <-done:
		t.Fatal("replay finished while the path lock was still held")
	case <-time.After(50 * time.Millisecond):
	}
	f.EntryLockTable.ReleaseLock(path, held)
	if err := <-done; err != nil {
		t.Fatalf("Replay: %v", err)
	}
	entry, err := store.FindEntry(context.Background(), path)
	if err != nil {
		t.Fatalf("FindEntry: %v", err)
	}
	if entry.Content != nil {
		t.Fatalf("content = %q, want local entry kept", entry.Content)
	}
}

// A closed stop channel must abort the retry loop with
// errPeerSubscriptionStopped so the subscription tears down and the offset
// save keeps the last completed event instead of skipping this one.
func TestReplicateMetadataChangeAbortsOnStop(t *testing.T) {
	store := &flakyReplayStore{
		stubFilerStore: newStubFilerStore(),
		failUntil:      1 << 30,
		err:            errors.New("i/o timeout talking to store"),
	}
	peer := pb.ServerAddress("peer-stopped:1")
	f := newReplayTestFiler(store)

	stopChan := make(chan struct{})
	close(stopChan)
	err := replicateMetadataChange(f, peer, quotaChangeEvent("stuck-bucket", 1<<20), stopChan)
	if !errors.Is(err, errPeerSubscriptionStopped) {
		t.Fatalf("err = %v, want errPeerSubscriptionStopped", err)
	}
	if got := store.attemptCount(); got != 0 {
		t.Fatalf("attempts = %d, want 0: a stopped subscription must not replay", got)
	}
}

// Commit-record keys must stay small: SQL KV stores keep the key suffix in a
// VARCHAR(766) column, so a raw path over ~578 bytes silently drops the
// record (the insert fails and the fallback update touches no row).
func TestMetaPathKeyFixedSizeForLongPaths(t *testing.T) {
	path := util.NewFullPath("/", string(make([]rune, 600)))
	for _, p := range []util.FullPath{"/a", path} {
		if got := len(metaPathKey(metaPathTsPrefix, p)); got != len(metaPathTsPrefix)+16 {
			t.Fatalf("metaPathKey(%d-byte path) = %d bytes, want %d", len(p), got, len(metaPathTsPrefix)+16)
		}
	}
}

// Exact-path locking alone lets a replayed child create slip between a local
// ancestor delete's tombstone check and the sweep: replay locks every
// ancestor too, so it waits for the delete's lock and then sees the
// tombstone.
func TestReplaySerializesWithAncestorLock(t *testing.T) {
	store := newStubFilerStore()
	f := newReplayTestFiler(store)

	now := time.Now()
	child := util.NewFullPath("/dir", "file")
	held := f.EntryLockTable.AcquireLock("test local delete", "/dir", util.ExclusiveLock)
	f.notePathChange("/dir", now.UnixNano(), true)

	createEvent := &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		EventNotification: &filer_pb.EventNotification{
			NewEntry: &filer_pb.Entry{Name: "file", Content: []byte("stale resurrect")},
		},
		TsNs: now.Add(-time.Minute).UnixNano(),
	}
	done := make(chan error, 1)
	go func() {
		done <- f.Replay(createEvent)
	}()
	select {
	case <-done:
		t.Fatal("replay finished while the ancestor lock was still held")
	case <-time.After(50 * time.Millisecond):
	}
	f.EntryLockTable.ReleaseLock("/dir", held)
	if err := <-done; err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if _, err := store.FindEntry(context.Background(), child); err == nil {
		t.Fatal("stale replayed create resurrected a child of a locally deleted directory")
	}
}

// Bootstrap ordering uses the sender's commit timestamp: a local commit
// newer than the traversed entry keeps its state even when the peer entry's
// mtime is far in the future.
func TestBootstrapSkipsPeerEntryBehindLocalDelete(t *testing.T) {
	store := newStubFilerStore()
	f := newReplayTestFiler(store)
	now := time.Now()
	path := util.NewFullPath("/dir", "file")

	f.notePathChange(path, now.UnixNano(), true)

	entry := &Entry{
		FullPath: path,
		Attr:     Attr{Mtime: now.Add(time.Hour)}, // client-settable, must not win
		Content:  []byte("stale peer entry"),
	}
	if err := f.applyTraversedEntry(context.Background(), entry, now.Add(-time.Minute).UnixNano()); err != nil {
		t.Fatalf("applyTraversedEntry: %v", err)
	}
	if _, err := store.FindEntry(context.Background(), path); err == nil {
		t.Fatal("bootstrap resurrected a path newer local delete covered")
	}
}

// The bug this guards: a peer entry committed recently but stamped with an
// old client mtime must still apply over an absent-or-older local commit.
func TestBootstrapAppliesPeerCommitNewerThanLocal(t *testing.T) {
	store := newStubFilerStore()
	f := newReplayTestFiler(store)
	now := time.Now()
	path := util.NewFullPath("/dir", "file")

	entry := &Entry{
		FullPath: path,
		Attr:     Attr{Mtime: now.Add(-time.Hour)}, // old client-set mtime
		Content:  []byte("fresh peer write"),
	}
	if err := f.applyTraversedEntry(context.Background(), entry, now.UnixNano()); err != nil {
		t.Fatalf("applyTraversedEntry: %v", err)
	}
	got, err := store.FindEntry(context.Background(), path)
	if err != nil {
		t.Fatalf("peer entry with newer commit must bootstrap: %v", err)
	}
	if string(got.Content) != "fresh peer write" {
		t.Fatalf("content = %q, want peer entry applied", got.Content)
	}
}

// A bootstrap-applied entry records the sender's commit timestamp, so a
// replayed event older than it — but newer than the entry's mtime — is
// still skipped.
func TestBootstrapAppliedCommitBlocksStaleReplay(t *testing.T) {
	store := newStubFilerStore()
	f := newReplayTestFiler(store)
	now := time.Now()
	path := util.NewFullPath("/dir", "file")

	entry := &Entry{
		FullPath: path,
		Attr:     Attr{Mtime: now.Add(-time.Hour)},
		Content:  []byte("fresh peer write"),
	}
	if err := f.applyTraversedEntry(context.Background(), entry, now.UnixNano()); err != nil {
		t.Fatalf("applyTraversedEntry: %v", err)
	}
	if err := f.Replay(overwriteEvent("/dir", "file", now.Add(-time.Minute).UnixNano(), "stale replay")); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	got, _ := store.FindEntry(context.Background(), path)
	if string(got.Content) != "fresh peer write" {
		t.Fatalf("content = %q, stale replay must not overwrite the bootstrap entry", got.Content)
	}
}

// An existing local entry is replaced only when the peer commit is newer —
// not when its mtime happens to be.
func TestBootstrapUpdatesExistingByCommitTime(t *testing.T) {
	store := newStubFilerStore()
	f := newReplayTestFiler(store)
	now := time.Now()
	path := util.NewFullPath("/dir", "file")

	store.entries[string(path)] = &Entry{
		FullPath: path,
		Attr:     Attr{Mtime: now.Add(time.Hour)}, // future mtime, e.g. client-set
		Content:  []byte("old local write"),
	}
	f.notePathChange(path, now.Add(-time.Minute).UnixNano(), false)

	entry := &Entry{
		FullPath: path,
		Attr:     Attr{Mtime: now.Add(-time.Hour)},
		Content:  []byte("newer peer commit"),
	}
	if err := f.applyTraversedEntry(context.Background(), entry, now.UnixNano()); err != nil {
		t.Fatalf("applyTraversedEntry: %v", err)
	}
	got, _ := store.FindEntry(context.Background(), path)
	if string(got.Content) != "newer peer commit" {
		t.Fatalf("content = %q, newer peer commit must update despite the future local mtime", got.Content)
	}
}

// A delete whose commit record failed to persist still blocks a stale
// replayed create: the pending record stands in until the retry lands it.
func TestPendingDeleteRecordBlocksStaleReplay(t *testing.T) {
	store := newStubFilerStore()
	store.setKvPutErr(errors.New("kv store unreachable"))
	f := newReplayTestFiler(store)
	f.flushCtx, f.flushCancel = context.WithCancel(context.Background())
	defer f.flushCancel()
	now := time.Now()
	path := util.NewFullPath("/dir", "file")

	// The delete already committed; only its ordering record failed.
	f.notePathChange(path, now.UnixNano(), true)

	createEvent := &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		EventNotification: &filer_pb.EventNotification{
			NewEntry: &filer_pb.Entry{Name: "file", Content: []byte("stale resurrect")},
		},
		TsNs: now.Add(-time.Minute).UnixNano(),
	}
	if err := f.Replay(createEvent); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if _, err := store.FindEntry(context.Background(), path); err == nil {
		t.Fatal("stale create resurrected a path whose delete record is still pending")
	}
}

// The failed record write retries until it lands, then leaves the pending set.
func TestPendingCommitRecordRepairs(t *testing.T) {
	store := newStubFilerStore()
	f := newReplayTestFiler(store)
	f.flushCtx, f.flushCancel = context.WithCancel(context.Background())
	defer f.flushCancel()
	path := util.NewFullPath("/dir", "file")

	store.setKvPutErr(errors.New("kv store unreachable"))
	f.notePathChange(path, 100, true)
	if _, loaded := f.metaTsPending.Load(string(path)); !loaded {
		t.Fatal("failed record write must stay pending")
	}
	store.setKvPutErr(nil)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, loaded := f.metaTsPending.Load(string(path)); !loaded {
			ts, found, err := f.kvReadTsNs(context.Background(), metaPathKey(metaPathDelPrefix, path))
			if err != nil || !found || ts != 100 {
				t.Fatalf("delete record after repair: ts=%v found=%v err=%v", ts, found, err)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("pending record was not repaired")
}

// CommitTsNs reports the recorded commit time over the entry mtime, and a
// still-pending record over both.
func TestCommitTsNsPrefersRecordAndPending(t *testing.T) {
	store := newStubFilerStore()
	f := newReplayTestFiler(store)
	path := util.NewFullPath("/dir", "file")

	if got, err := f.CommitTsNs(context.Background(), path, 7); err != nil || got != 7 {
		t.Fatalf("no record: CommitTsNs = %d, %v; want fallback 7", got, err)
	}
	f.notePathChange(path, 50, false)
	if got, err := f.CommitTsNs(context.Background(), path, 7); err != nil || got != 50 {
		t.Fatalf("recorded: CommitTsNs = %d, %v; want 50", got, err)
	}
	pending := &metaTsPendingRec{}
	pending.changeTsNs.Store(80)
	f.metaTsPending.Store(string(path), pending)
	if got, err := f.CommitTsNs(context.Background(), path, 7); err != nil || got != 80 {
		t.Fatalf("pending: CommitTsNs = %d, %v; want 80", got, err)
	}
}

// A successful write discharges what the pending record owed before it, so a
// repair pass can never put an older timestamp back over it.
func TestPendingCommitRecordDischargedByNewerWrite(t *testing.T) {
	store := newStubFilerStore()
	f := newReplayTestFiler(store)
	f.flushCtx, f.flushCancel = context.WithCancel(context.Background())
	defer f.flushCancel()
	path := util.NewFullPath("/dir", "file")

	store.setKvPutErr(errors.New("kv store unreachable"))
	f.notePathChange(path, 100, false)
	store.setKvPutErr(nil)
	f.notePathChange(path, 200, false)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, loaded := f.metaTsPending.Load(string(path)); !loaded {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	ts, found, err := f.kvReadTsNs(context.Background(), metaPathKey(metaPathTsPrefix, path))
	if err != nil || !found || ts != 200 {
		t.Fatalf("MpTs after repair = %v, %v, %v; want 200", ts, found, err)
	}
}

// A failed delete record keeps its own time even when a later failed change
// merges into the pending record: the repair must not stamp the delete at
// the newer change's time.
func TestPendingDeleteRecordKeepsItsTime(t *testing.T) {
	store := newStubFilerStore()
	store.setKvPutErr(errors.New("kv store unreachable"))
	f := newReplayTestFiler(store)
	f.flushCtx, f.flushCancel = context.WithCancel(context.Background())
	defer f.flushCancel()
	path := util.NewFullPath("/dir", "file")

	f.notePathChange(path, 100, true)
	f.notePathChange(path, 200, false)
	store.setKvPutErr(nil)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, loaded := f.metaTsPending.Load(string(path)); !loaded {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if ts, found, err := f.kvReadTsNs(context.Background(), metaPathKey(metaPathTsPrefix, path)); err != nil || !found || ts != 200 {
		t.Fatalf("MpTs = %v, %v, %v; want 200", ts, found, err)
	}
	if ts, found, err := f.kvReadTsNs(context.Background(), metaPathKey(metaPathDelPrefix, path)); err != nil || !found || ts != 100 {
		t.Fatalf("MpDl = %v, %v, %v; want 100", ts, found, err)
	}
}
