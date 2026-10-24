package filer

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
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

// A missing local entry has no commit time to order against: the peer copy
// must apply even when both sides are timestamp-less — an epoch-dated entry
// from a peer that predates EntryTsNs is still bootstrapped, while an
// existing zero-mtime local entry keeps its place.
func TestBootstrapCopiesMissingEntryWithoutCommitTime(t *testing.T) {
	store := newStubFilerStore()
	f := newReplayTestFiler(store)
	missing := util.NewFullPath("/dir", "missing")
	existing := util.NewFullPath("/dir", "existing")

	store.entries[string(existing)] = &Entry{
		FullPath: existing,
		Attr:     Attr{Mtime: time.Unix(0, 0)},
		Content:  []byte("zero-mtime local entry"),
	}
	for _, path := range []util.FullPath{missing, existing} {
		entry := &Entry{
			FullPath: path,
			Attr:     Attr{Mtime: time.Unix(0, 0)},
			Content:  []byte("epoch-dated peer entry"),
		}
		if err := f.applyTraversedEntry(context.Background(), entry, 0); err != nil {
			t.Fatalf("applyTraversedEntry %s: %v", path, err)
		}
	}
	got, err := store.FindEntry(context.Background(), missing)
	if err != nil {
		t.Fatalf("missing local entry must take the peer copy: %v", err)
	}
	if string(got.Content) != "epoch-dated peer entry" {
		t.Fatalf("content = %q, want peer entry applied", got.Content)
	}
	if got, _ := store.FindEntry(context.Background(), existing); string(got.Content) != "zero-mtime local entry" {
		t.Fatalf("content = %q, existing local entry must be kept", got.Content)
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

// A replayed directory delete must remove the local subtree too: the origin
// only emits the directory's own key, and children the peer holds that the
// origin never listed would orphan under the missing parent.
func TestReplayDirectoryDeleteSweepsLocalChildren(t *testing.T) {
	store := newStubFilerStore()
	f := newReplayTestFiler(store)
	ctx := context.Background()

	put := func(path string, isDir bool) {
		e := &Entry{FullPath: util.FullPath(path)}
		if isDir {
			e.Attr.Mode = os.ModeDir
		}
		if err := store.InsertEntry(ctx, e); err != nil {
			t.Fatalf("insert %s: %v", path, err)
		}
	}
	put("/d", true)
	put("/d/a.txt", false)
	put("/d/sub", true)
	put("/d/sub/nested.txt", false)

	event := &filer_pb.SubscribeMetadataResponse{
		Directory: "/",
		EventNotification: &filer_pb.EventNotification{
			OldEntry:     &filer_pb.Entry{Name: "d", IsDirectory: true},
			DeleteChunks: false,
		},
		TsNs: time.Now().UnixNano(),
	}
	if err := f.Replay(event); err != nil {
		t.Fatalf("replay: %v", err)
	}

	for _, path := range []string{"/d", "/d/a.txt", "/d/sub", "/d/sub/nested.txt"} {
		if _, err := store.FindEntry(ctx, util.FullPath(path)); !errors.Is(err, filer_pb.ErrNotFound) {
			t.Fatalf("expected %s gone after replayed directory delete, got %v", path, err)
		}
	}
}

// The sweep locks each child under locks Replay already holds, so it cannot
// wait on a child a local write holds — a rename could hold the child while
// waiting on one of the replayed paths. Contention fails the replay instead,
// and a retry after the local write sweeps normally.
func TestReplaySweepFailsOnChildLockContention(t *testing.T) {
	store := newStubFilerStore()
	f := newReplayTestFiler(store)
	ctx := context.Background()

	if err := store.InsertEntry(ctx, &Entry{FullPath: "/d", Attr: Attr{Mode: os.ModeDir}}); err != nil {
		t.Fatal(err)
	}
	if err := store.InsertEntry(ctx, &Entry{FullPath: "/d/a.txt"}); err != nil {
		t.Fatal(err)
	}
	event := &filer_pb.SubscribeMetadataResponse{
		Directory: "/",
		EventNotification: &filer_pb.EventNotification{
			OldEntry: &filer_pb.Entry{Name: "d", IsDirectory: true},
		},
		TsNs: time.Now().UnixNano(),
	}

	held := f.EntryLockTable.AcquireLock("test local write", "/d/a.txt", util.ExclusiveLock)
	if err := f.Replay(event); err == nil {
		f.EntryLockTable.ReleaseLock("/d/a.txt", held)
		t.Fatal("replay succeeded while a child lock was held by a local write")
	}
	f.EntryLockTable.ReleaseLock("/d/a.txt", held)

	if err := f.Replay(event); err != nil {
		t.Fatalf("replay after the local write: %v", err)
	}
	for _, path := range []string{"/d", "/d/a.txt"} {
		if _, err := store.FindEntry(ctx, util.FullPath(path)); !errors.Is(err, filer_pb.ErrNotFound) {
			t.Fatalf("expected %s gone after the retried delete, got %v", path, err)
		}
	}
}

func TestReplayFileDeleteKeepsSiblingEntries(t *testing.T) {
	store := newStubFilerStore()
	f := newReplayTestFiler(store)
	ctx := context.Background()

	dir := &Entry{FullPath: "/d", Attr: Attr{Mode: os.ModeDir}}
	if err := store.InsertEntry(ctx, dir); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"gone.txt", "kept.txt"} {
		if err := store.InsertEntry(ctx, &Entry{FullPath: util.NewFullPath("/d", name)}); err != nil {
			t.Fatal(err)
		}
	}

	event := &filer_pb.SubscribeMetadataResponse{
		Directory: "/d",
		EventNotification: &filer_pb.EventNotification{
			OldEntry: &filer_pb.Entry{Name: "gone.txt"},
		},
		TsNs: time.Now().UnixNano(),
	}
	if err := f.Replay(event); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if _, err := store.FindEntry(ctx, "/d/gone.txt"); !errors.Is(err, filer_pb.ErrNotFound) {
		t.Fatalf("expected deleted file gone, got %v", err)
	}
	if _, err := store.FindEntry(ctx, "/d/kept.txt"); err != nil {
		t.Fatalf("expected sibling kept, got %v", err)
	}
}

func TestReplayDirectoryDeleteKeepsNewerChildren(t *testing.T) {
	store := newStubFilerStore()
	f := newReplayTestFiler(store)
	ctx := context.Background()

	eventTs := time.Now().UnixNano()
	put := func(path string, isDir bool, mtime time.Time) {
		e := &Entry{FullPath: util.FullPath(path), Attr: Attr{Mtime: mtime}}
		if isDir {
			e.Attr.Mode = os.ModeDir
		}
		if err := store.InsertEntry(ctx, e); err != nil {
			t.Fatalf("insert %s: %v", path, err)
		}
	}
	old := time.Unix(1600000000, 0)
	newer := time.Unix(0, eventTs).Add(time.Second)
	put("/d", true, old)
	put("/d/old.txt", false, old)
	put("/d/sub", true, old)
	put("/d/sub/newer.txt", false, newer)

	event := &filer_pb.SubscribeMetadataResponse{
		Directory: "/",
		EventNotification: &filer_pb.EventNotification{
			OldEntry:     &filer_pb.Entry{Name: "d", IsDirectory: true},
			DeleteChunks: false,
		},
		TsNs: eventTs,
	}
	if err := f.Replay(event); err != nil {
		t.Fatalf("replay: %v", err)
	}

	if _, err := store.FindEntry(ctx, "/d/old.txt"); !errors.Is(err, filer_pb.ErrNotFound) {
		t.Fatalf("older child should be swept, got %v", err)
	}
	for _, path := range []string{"/d", "/d/sub", "/d/sub/newer.txt"} {
		if _, err := store.FindEntry(ctx, util.FullPath(path)); err != nil {
			t.Fatalf("newer child and its ancestors should survive: %s: %v", path, err)
		}
	}
}

// A directory kept for newer descendants still records the delete: a stale
// create replayed later for another child must be fenced by it.
func TestReplayKeptDirectoryDeleteFencesStaleCreates(t *testing.T) {
	store := newStubFilerStore()
	f := newReplayTestFiler(store)
	ctx := context.Background()

	eventTs := time.Now().UnixNano()
	old := time.Unix(1600000000, 0)
	newer := time.Unix(0, eventTs).Add(time.Second)
	put := func(path string, isDir bool, mtime time.Time) {
		e := &Entry{FullPath: util.FullPath(path), Attr: Attr{Mtime: mtime}}
		if isDir {
			e.Attr.Mode = os.ModeDir
		}
		if err := store.InsertEntry(ctx, e); err != nil {
			t.Fatalf("insert %s: %v", path, err)
		}
	}
	put("/d", true, old)
	put("/d/newer.txt", false, newer)

	event := &filer_pb.SubscribeMetadataResponse{
		Directory: "/",
		EventNotification: &filer_pb.EventNotification{
			OldEntry: &filer_pb.Entry{Name: "d", IsDirectory: true},
		},
		TsNs: eventTs,
	}
	if err := f.Replay(event); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if _, err := store.FindEntry(ctx, "/d"); err != nil {
		t.Fatalf("newer child should keep the directory: %v", err)
	}

	stale := &filer_pb.SubscribeMetadataResponse{
		Directory: "/d",
		EventNotification: &filer_pb.EventNotification{
			NewEntry: &filer_pb.Entry{Name: "late.txt"},
		},
		TsNs: eventTs - int64(time.Minute),
	}
	if err := f.Replay(stale); err != nil {
		t.Fatalf("replay stale create: %v", err)
	}
	if _, err := store.FindEntry(ctx, "/d/late.txt"); !errors.Is(err, filer_pb.ErrNotFound) {
		t.Fatal("stale create resurrected a child under a directory the origin deleted")
	}
}

// A swept hard link drops the shared inode record with the entry. When the
// record is gone the sweep must free the chunks no sibling link can reach.
func TestReplayDirectoryDeleteFreesOrphanedHardLinkChunks(t *testing.T) {
	store := newStubFilerStore()
	f := newReplayTestFiler(store)
	f.FilerConf = NewFilerConf()
	f.FileIdDeletionQueue = util.NewUnboundedQueue()
	ctx := context.Background()

	hardLinkId := NewHardLinkId()
	inode := &Entry{
		FullPath:        "/d/a.txt",
		Attr:            Attr{Mtime: time.Now()},
		HardLinkId:      hardLinkId,
		HardLinkCounter: 1,
		Chunks:          []*filer_pb.FileChunk{{FileId: "3,01637037d6"}},
	}
	blob, err := inode.EncodeAttributesAndChunks()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.KvPut(ctx, hardLinkId, blob); err != nil {
		t.Fatal(err)
	}
	put := func(e *Entry) {
		if err := store.InsertEntry(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	put(&Entry{FullPath: "/d", Attr: Attr{Mode: os.ModeDir}})
	put(&Entry{FullPath: "/d/a.txt", Attr: Attr{Mtime: time.Now()}, HardLinkId: hardLinkId, HardLinkCounter: 1})

	event := &filer_pb.SubscribeMetadataResponse{
		Directory: "/",
		EventNotification: &filer_pb.EventNotification{
			OldEntry:     &filer_pb.Entry{Name: "d", IsDirectory: true},
			DeleteChunks: true,
		},
		TsNs: time.Now().UnixNano(),
	}
	if err := f.Replay(event); err != nil {
		t.Fatalf("replay: %v", err)
	}

	if _, err := store.KvGet(ctx, hardLinkId); !errors.Is(err, ErrKvNotFound) {
		t.Fatalf("hard link record should be gone, got %v", err)
	}
	if got := f.pendingDeletionCount(); got != 1 {
		t.Fatalf("pending deletions = %d, want the orphaned shared chunks queued", got)
	}
}

// A sibling link outside the swept tree keeps the inode record, so the shared
// chunks stay owned and must not be deleted.
func TestReplayDirectoryDeleteKeepsSharedHardLinkChunks(t *testing.T) {
	store := newStubFilerStore()
	f := newReplayTestFiler(store)
	f.FilerConf = NewFilerConf()
	f.FileIdDeletionQueue = util.NewUnboundedQueue()
	ctx := context.Background()

	hardLinkId := NewHardLinkId()
	inode := &Entry{
		FullPath:        "/d/a.txt",
		Attr:            Attr{Mtime: time.Now()},
		HardLinkId:      hardLinkId,
		HardLinkCounter: 2,
		Chunks:          []*filer_pb.FileChunk{{FileId: "3,01637037d6"}},
	}
	blob, err := inode.EncodeAttributesAndChunks()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.KvPut(ctx, hardLinkId, blob); err != nil {
		t.Fatal(err)
	}
	put := func(e *Entry) {
		if err := store.InsertEntry(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	put(&Entry{FullPath: "/d", Attr: Attr{Mode: os.ModeDir}})
	put(&Entry{FullPath: "/d/a.txt", Attr: Attr{Mtime: time.Now()}, HardLinkId: hardLinkId, HardLinkCounter: 2})
	put(&Entry{FullPath: "/keep", Attr: Attr{Mode: os.ModeDir}})
	put(&Entry{FullPath: "/keep/b.txt", Attr: Attr{Mtime: time.Now()}, HardLinkId: hardLinkId, HardLinkCounter: 2})

	event := &filer_pb.SubscribeMetadataResponse{
		Directory: "/",
		EventNotification: &filer_pb.EventNotification{
			OldEntry:     &filer_pb.Entry{Name: "d", IsDirectory: true},
			DeleteChunks: true,
		},
		TsNs: time.Now().UnixNano(),
	}
	if err := f.Replay(event); err != nil {
		t.Fatalf("replay: %v", err)
	}

	if _, err := store.KvGet(ctx, hardLinkId); err != nil {
		t.Fatalf("surviving link must keep the inode record: %v", err)
	}
	if got := f.pendingDeletionCount(); got != 0 {
		t.Fatalf("pending deletions = %d, want 0 while a sibling link owns the chunks", got)
	}
}

// A replayed directory delete must mirror the origin's remote cleanup: entries
// the lagging peer alone still holds lose their remote objects too, and record
// tombstones so lazy remote fetches cannot resurrect them.
func TestReplayDirectoryDeleteSweepsRemoteObjects(t *testing.T) {
	const storageType = "stub_replay_remote"
	stub := &stubRemoteClient{
		statResult: &filer_pb.RemoteEntry{RemoteMtime: 1700000000, RemoteSize: 10},
	}
	defer registerStubMaker(t, storageType, stub)()

	conf := &remote_pb.RemoteConf{Name: "replaystore", Type: storageType}
	rs := NewFilerRemoteStorage()
	rs.storageNameToConf[conf.Name] = conf
	rs.mapDirectoryToRemoteStorage("/buckets/mybucket", &remote_pb.RemoteStorageLocation{
		Name:   "replaystore",
		Bucket: "mybucket",
		Path:   "/",
	})

	store := newStubFilerStore()
	f := newTestFiler(t, store, rs)
	ctx := context.Background()

	put := func(e *Entry) {
		if err := store.InsertEntry(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	for _, dir := range []string{"/buckets", "/buckets/mybucket", "/buckets/mybucket/d"} {
		put(&Entry{FullPath: util.FullPath(dir), Attr: Attr{Mode: os.ModeDir}})
	}
	put(&Entry{
		FullPath: "/buckets/mybucket/d/f.txt",
		Attr:     Attr{Mtime: time.Now(), Mode: 0644},
		Remote:   &filer_pb.RemoteEntry{RemoteMtime: 1700000000, RemoteSize: 10},
	})

	event := &filer_pb.SubscribeMetadataResponse{
		Directory: "/buckets/mybucket",
		EventNotification: &filer_pb.EventNotification{
			OldEntry:     &filer_pb.Entry{Name: "d", IsDirectory: true},
			DeleteChunks: false,
		},
		TsNs: time.Now().UnixNano(),
	}
	if err := f.Replay(event); err != nil {
		t.Fatalf("replay: %v", err)
	}
	f.onMetadataChangeEvent(event)

	if _, err := store.FindEntry(ctx, "/buckets/mybucket/d/f.txt"); !errors.Is(err, filer_pb.ErrNotFound) {
		t.Fatalf("swept child should be gone, got %v", err)
	}
	if len(stub.deleteCalls) != 1 || stub.deleteCalls[0].Path != "/d/f.txt" {
		t.Fatalf("expected one remote delete of /d/f.txt, got %v", stub.deleteCalls)
	}
	if got := f.remoteTombstones.dirs["/buckets/mybucket/d"]; got != event.TsNs {
		t.Fatalf("dir tombstone stamped %d, want the event ts %d", got, event.TsNs)
	}
}

// A metadata-only delete keeps remote objects: the event's marker means the
// remote tree stays authoritative, so the sweep must not touch it.
func TestReplayDirectoryDeleteKeepRemoteObjects(t *testing.T) {
	const storageType = "stub_replay_remote_keep"
	stub := &stubRemoteClient{}
	defer registerStubMaker(t, storageType, stub)()

	conf := &remote_pb.RemoteConf{Name: "keepstore", Type: storageType}
	rs := NewFilerRemoteStorage()
	rs.storageNameToConf[conf.Name] = conf
	rs.mapDirectoryToRemoteStorage("/buckets/mybucket", &remote_pb.RemoteStorageLocation{
		Name:   "keepstore",
		Bucket: "mybucket",
		Path:   "/",
	})

	store := newStubFilerStore()
	f := newTestFiler(t, store, rs)
	ctx := context.Background()

	put := func(e *Entry) {
		if err := store.InsertEntry(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	for _, dir := range []string{"/buckets", "/buckets/mybucket", "/buckets/mybucket/d"} {
		put(&Entry{FullPath: util.FullPath(dir), Attr: Attr{Mode: os.ModeDir}})
	}
	put(&Entry{
		FullPath: "/buckets/mybucket/d/f.txt",
		Attr:     Attr{Mtime: time.Now(), Mode: 0644},
		Remote:   &filer_pb.RemoteEntry{RemoteMtime: 1700000000, RemoteSize: 10},
	})

	event := &filer_pb.SubscribeMetadataResponse{
		Directory: "/buckets/mybucket",
		EventNotification: &filer_pb.EventNotification{
			OldEntry: &filer_pb.Entry{
				Name:        "d",
				IsDirectory: true,
				Extended:    map[string][]byte{ExtKeepRemoteObjectKey: []byte("true")},
			},
		},
		TsNs: time.Now().UnixNano(),
	}
	if err := f.Replay(event); err != nil {
		t.Fatalf("replay: %v", err)
	}
	f.onMetadataChangeEvent(event)

	if len(stub.deleteCalls) != 0 {
		t.Fatalf("metadata-only delete must not touch remote objects, got %v", stub.deleteCalls)
	}
	if len(f.remoteTombstones.files)+len(f.remoteTombstones.dirs) != 0 {
		t.Fatalf("metadata-only delete must not record tombstones, got %v", f.remoteTombstones.dirs)
	}
}

// A delayed sweep must not erase an object another writer replaced since the
// entry's stamp was recorded — the same guard the write-back daemon applies.
func TestReplayDirectoryDeleteKeepsReplacedRemoteObject(t *testing.T) {
	const storageType = "stub_replay_remote_replaced"
	stub := &stubRemoteClient{
		statResult: &filer_pb.RemoteEntry{RemoteMtime: 1700000099, RemoteSize: 10},
	}
	defer registerStubMaker(t, storageType, stub)()

	conf := &remote_pb.RemoteConf{Name: "replacestore", Type: storageType}
	rs := NewFilerRemoteStorage()
	rs.storageNameToConf[conf.Name] = conf
	rs.mapDirectoryToRemoteStorage("/buckets/mybucket", &remote_pb.RemoteStorageLocation{
		Name:   "replacestore",
		Bucket: "mybucket",
		Path:   "/",
	})

	store := newStubFilerStore()
	f := newTestFiler(t, store, rs)
	ctx := context.Background()

	put := func(e *Entry) {
		if err := store.InsertEntry(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	for _, dir := range []string{"/buckets", "/buckets/mybucket", "/buckets/mybucket/d"} {
		put(&Entry{FullPath: util.FullPath(dir), Attr: Attr{Mode: os.ModeDir}})
	}
	put(&Entry{
		FullPath: "/buckets/mybucket/d/f.txt",
		Attr:     Attr{Mtime: time.Now(), Mode: 0644},
		Remote:   &filer_pb.RemoteEntry{RemoteMtime: 1700000000, RemoteSize: 10},
	})

	event := &filer_pb.SubscribeMetadataResponse{
		Directory: "/buckets/mybucket",
		EventNotification: &filer_pb.EventNotification{
			OldEntry:     &filer_pb.Entry{Name: "d", IsDirectory: true},
			DeleteChunks: false,
		},
		TsNs: time.Now().UnixNano(),
	}
	if err := f.Replay(event); err != nil {
		t.Fatalf("replay: %v", err)
	}

	if len(stub.deleteCalls) != 0 {
		t.Fatalf("a replaced remote object must be kept, got %v", stub.deleteCalls)
	}
}

// A backend offering a conditional delete must serve the sweep: the check and
// the delete bind into one preconditioned request, so a replacement landing
// between them cannot be deleted.
func TestReplayDirectoryDeleteUsesConditionalRemoteDelete(t *testing.T) {
	const storageType = "stub_replay_remote_cond"
	stub := &stubConditionalRemoteClient{conditionalResult: true}
	defer registerStubMaker(t, storageType, stub)()

	conf := &remote_pb.RemoteConf{Name: "condstore", Type: storageType}
	rs := NewFilerRemoteStorage()
	rs.storageNameToConf[conf.Name] = conf
	rs.mapDirectoryToRemoteStorage("/buckets/mybucket", &remote_pb.RemoteStorageLocation{
		Name:   "condstore",
		Bucket: "mybucket",
		Path:   "/",
	})

	store := newStubFilerStore()
	f := newTestFiler(t, store, rs)
	ctx := context.Background()

	put := func(e *Entry) {
		if err := store.InsertEntry(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	for _, dir := range []string{"/buckets", "/buckets/mybucket", "/buckets/mybucket/d"} {
		put(&Entry{FullPath: util.FullPath(dir), Attr: Attr{Mode: os.ModeDir}})
	}
	put(&Entry{
		FullPath: "/buckets/mybucket/d/f.txt",
		Attr:     Attr{Mtime: time.Now(), Mode: 0644},
		Remote:   &filer_pb.RemoteEntry{RemoteMtime: 1700000000, RemoteSize: 10},
	})

	event := &filer_pb.SubscribeMetadataResponse{
		Directory: "/buckets/mybucket",
		EventNotification: &filer_pb.EventNotification{
			OldEntry:     &filer_pb.Entry{Name: "d", IsDirectory: true},
			DeleteChunks: false,
		},
		TsNs: time.Now().UnixNano(),
	}
	if err := f.Replay(event); err != nil {
		t.Fatalf("replay: %v", err)
	}

	if len(stub.conditionalCalls) != 1 || stub.conditionalCalls[0].Path != "/d/f.txt" {
		t.Fatalf("expected one conditional delete of /d/f.txt, got %v", stub.conditionalCalls)
	}
	if len(stub.deleteCalls) != 0 {
		t.Fatalf("an unconditional delete must not run beside the conditional one, got %v", stub.deleteCalls)
	}
}

// A conditional delete reporting the object changed keeps the remote object
// but still sweeps the local metadata: the entry the peer held is gone either
// way.
func TestReplayDirectoryDeleteKeepsChangedRemoteObject(t *testing.T) {
	const storageType = "stub_replay_remote_cond_keep"
	stub := &stubConditionalRemoteClient{conditionalResult: false}
	defer registerStubMaker(t, storageType, stub)()

	conf := &remote_pb.RemoteConf{Name: "condkeepstore", Type: storageType}
	rs := NewFilerRemoteStorage()
	rs.storageNameToConf[conf.Name] = conf
	rs.mapDirectoryToRemoteStorage("/buckets/mybucket", &remote_pb.RemoteStorageLocation{
		Name:   "condkeepstore",
		Bucket: "mybucket",
		Path:   "/",
	})

	store := newStubFilerStore()
	f := newTestFiler(t, store, rs)
	ctx := context.Background()

	put := func(e *Entry) {
		if err := store.InsertEntry(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	for _, dir := range []string{"/buckets", "/buckets/mybucket", "/buckets/mybucket/d"} {
		put(&Entry{FullPath: util.FullPath(dir), Attr: Attr{Mode: os.ModeDir}})
	}
	put(&Entry{
		FullPath: "/buckets/mybucket/d/f.txt",
		Attr:     Attr{Mtime: time.Now(), Mode: 0644},
		Remote:   &filer_pb.RemoteEntry{RemoteMtime: 1700000000, RemoteSize: 10},
	})

	event := &filer_pb.SubscribeMetadataResponse{
		Directory: "/buckets/mybucket",
		EventNotification: &filer_pb.EventNotification{
			OldEntry:     &filer_pb.Entry{Name: "d", IsDirectory: true},
			DeleteChunks: false,
		},
		TsNs: time.Now().UnixNano(),
	}
	if err := f.Replay(event); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if _, err := store.FindEntry(ctx, "/buckets/mybucket/d/f.txt"); !errors.Is(err, filer_pb.ErrNotFound) {
		t.Fatalf("swept child should be gone, got %v", err)
	}
	if len(stub.deleteCalls) != 0 {
		t.Fatalf("a changed remote object must be kept, got %v", stub.deleteCalls)
	}
}

// A failed remote delete aborts the sweep so the replication stream retries
// instead of orphaning the object no other event will ever name.
func TestReplayDirectoryDeleteFailsOnRemoteDeleteError(t *testing.T) {
	const storageType = "stub_replay_remote_err"
	stub := &stubRemoteClient{
		statResult: &filer_pb.RemoteEntry{RemoteMtime: 1700000000, RemoteSize: 10},
		deleteErr:  errors.New("remote unavailable"),
	}
	defer registerStubMaker(t, storageType, stub)()

	conf := &remote_pb.RemoteConf{Name: "errstore", Type: storageType}
	rs := NewFilerRemoteStorage()
	rs.storageNameToConf[conf.Name] = conf
	rs.mapDirectoryToRemoteStorage("/buckets/mybucket", &remote_pb.RemoteStorageLocation{
		Name:   "errstore",
		Bucket: "mybucket",
		Path:   "/",
	})

	store := newStubFilerStore()
	f := newTestFiler(t, store, rs)
	ctx := context.Background()

	put := func(e *Entry) {
		if err := store.InsertEntry(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	for _, dir := range []string{"/buckets", "/buckets/mybucket", "/buckets/mybucket/d"} {
		put(&Entry{FullPath: util.FullPath(dir), Attr: Attr{Mode: os.ModeDir}})
	}
	put(&Entry{
		FullPath: "/buckets/mybucket/d/f.txt",
		Attr:     Attr{Mtime: time.Now(), Mode: 0644},
		Remote:   &filer_pb.RemoteEntry{RemoteMtime: 1700000000, RemoteSize: 10},
	})

	event := &filer_pb.SubscribeMetadataResponse{
		Directory: "/buckets/mybucket",
		EventNotification: &filer_pb.EventNotification{
			OldEntry:     &filer_pb.Entry{Name: "d", IsDirectory: true},
			DeleteChunks: false,
		},
		TsNs: time.Now().UnixNano(),
	}
	if err := f.Replay(event); err == nil {
		t.Fatal("a failed remote delete must fail the replay for retry")
	}
	if _, err := store.FindEntry(ctx, "/buckets/mybucket/d/f.txt"); err != nil {
		t.Fatalf("the child must survive for the retry, got %v", err)
	}
}

// A swept entry deleted while its rename copy was still pending reads its
// content from the source the marker names: removing the entry must release
// that object too, or it is orphaned — the destination key may hold nothing.
func TestReplayDirectoryDeleteReleasesPendingRenameSource(t *testing.T) {
	const storageType = "stub_replay_remote_rename_src"
	stub := &pathStatRemoteClient{objects: map[string]*filer_pb.RemoteEntry{
		"/d/src.txt": {RemoteMtime: 1700000000, RemoteSize: 10},
	}}
	defer registerStubMaker(t, storageType, stub)()

	conf := &remote_pb.RemoteConf{Name: "srcstore", Type: storageType}
	rs := NewFilerRemoteStorage()
	rs.storageNameToConf[conf.Name] = conf
	rs.mapDirectoryToRemoteStorage("/buckets/mybucket", &remote_pb.RemoteStorageLocation{
		Name:   "srcstore",
		Bucket: "mybucket",
		Path:   "/",
	})

	store := newStubFilerStore()
	f := newTestFiler(t, store, rs)
	ctx := context.Background()

	put := func(e *Entry) {
		if err := store.InsertEntry(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	for _, dir := range []string{"/buckets", "/buckets/mybucket", "/buckets/mybucket/d"} {
		put(&Entry{FullPath: util.FullPath(dir), Attr: Attr{Mode: os.ModeDir}})
	}
	put(&Entry{
		FullPath: "/buckets/mybucket/d/dst.txt",
		Attr:     Attr{Mtime: time.Now(), Mode: 0644},
		Remote:   &filer_pb.RemoteEntry{RemoteMtime: 1700000000, RemoteSize: 10},
		Extended: map[string][]byte{ExtRemoteRenameSourceKey: []byte("/buckets/mybucket/d/src.txt")},
	})

	event := &filer_pb.SubscribeMetadataResponse{
		Directory: "/buckets/mybucket",
		EventNotification: &filer_pb.EventNotification{
			OldEntry:     &filer_pb.Entry{Name: "d", IsDirectory: true},
			DeleteChunks: false,
		},
		TsNs: time.Now().UnixNano(),
	}
	if err := f.Replay(event); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if _, err := store.FindEntry(ctx, "/buckets/mybucket/d/dst.txt"); !errors.Is(err, filer_pb.ErrNotFound) {
		t.Fatalf("swept child should be gone, got %v", err)
	}
	if len(stub.deleteCalls) != 1 || stub.deleteCalls[0].Path != "/d/src.txt" {
		t.Fatalf("expected one remote delete of the pending rename's source /d/src.txt, got %v", stub.deleteCalls)
	}
}

// The source survives while a remote-only entry holds its path: that entry's
// content is the same object. The swept destination entry's own object still
// goes while its stamp describes it.
func TestReplayDirectoryDeleteKeepsPendingRenameSourceOwnedByRemoteOnly(t *testing.T) {
	const storageType = "stub_replay_remote_rename_owned"
	stub := &pathStatRemoteClient{objects: map[string]*filer_pb.RemoteEntry{
		"/d/dst.txt":     {RemoteMtime: 1700000000, RemoteSize: 10},
		"/other/src.txt": {RemoteMtime: 1700000000, RemoteSize: 10},
	}}
	defer registerStubMaker(t, storageType, stub)()

	conf := &remote_pb.RemoteConf{Name: "ownstore", Type: storageType}
	rs := NewFilerRemoteStorage()
	rs.storageNameToConf[conf.Name] = conf
	rs.mapDirectoryToRemoteStorage("/buckets/mybucket", &remote_pb.RemoteStorageLocation{
		Name:   "ownstore",
		Bucket: "mybucket",
		Path:   "/",
	})

	store := newStubFilerStore()
	f := newTestFiler(t, store, rs)
	ctx := context.Background()

	put := func(e *Entry) {
		if err := store.InsertEntry(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	for _, dir := range []string{"/buckets", "/buckets/mybucket", "/buckets/mybucket/d", "/buckets/mybucket/other"} {
		put(&Entry{FullPath: util.FullPath(dir), Attr: Attr{Mode: os.ModeDir}})
	}
	put(&Entry{
		FullPath: "/buckets/mybucket/d/dst.txt",
		Attr:     Attr{Mtime: time.Now(), Mode: 0644},
		Remote:   &filer_pb.RemoteEntry{RemoteMtime: 1700000000, RemoteSize: 10},
		Extended: map[string][]byte{ExtRemoteRenameSourceKey: []byte("/buckets/mybucket/other/src.txt")},
	})
	put(&Entry{
		FullPath: "/buckets/mybucket/other/src.txt",
		Attr:     Attr{Mtime: time.Now(), Mode: 0644},
		Remote:   &filer_pb.RemoteEntry{RemoteMtime: 1700000000, RemoteSize: 10},
	})

	event := &filer_pb.SubscribeMetadataResponse{
		Directory: "/buckets/mybucket",
		EventNotification: &filer_pb.EventNotification{
			OldEntry:     &filer_pb.Entry{Name: "d", IsDirectory: true},
			DeleteChunks: false,
		},
		TsNs: time.Now().UnixNano(),
	}
	if err := f.Replay(event); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(stub.deleteCalls) != 1 || stub.deleteCalls[0].Path != "/d/dst.txt" {
		t.Fatalf("expected one remote delete of /d/dst.txt, got %v", stub.deleteCalls)
	}
	if _, err := store.FindEntry(ctx, "/buckets/mybucket/other/src.txt"); err != nil {
		t.Fatalf("the remote-only source holder must survive, got %v", err)
	}
}

// A remote-only entry that took the path since the listing reads the object
// at the key as its only copy — the sweep must keep both.
func TestReplaySweepKeepsRemoteOnlyOwnerOfPath(t *testing.T) {
	const storageType = "stub_replay_remote_owner"
	stub := &pathStatRemoteClient{objects: map[string]*filer_pb.RemoteEntry{
		"/d/f.txt": {RemoteMtime: 1700000099, RemoteSize: 20},
	}}
	defer registerStubMaker(t, storageType, stub)()

	conf := &remote_pb.RemoteConf{Name: "ownerstore", Type: storageType}
	rs := NewFilerRemoteStorage()
	rs.storageNameToConf[conf.Name] = conf
	rs.mapDirectoryToRemoteStorage("/buckets/mybucket", &remote_pb.RemoteStorageLocation{
		Name:   "ownerstore",
		Bucket: "mybucket",
		Path:   "/",
	})

	store := newStubFilerStore()
	f := newTestFiler(t, store, rs)
	ctx := context.Background()

	owner := &Entry{
		FullPath: "/buckets/mybucket/d/f.txt",
		Attr:     Attr{Mtime: time.Now(), Mode: 0644},
		Remote:   &filer_pb.RemoteEntry{RemoteMtime: 1700000099, RemoteSize: 20},
	}
	if err := store.InsertEntry(ctx, owner); err != nil {
		t.Fatal(err)
	}

	stale := &Entry{
		FullPath: "/buckets/mybucket/d/f.txt",
		Attr:     Attr{Mtime: time.Now(), Mode: 0644},
		Remote:   &filer_pb.RemoteEntry{RemoteMtime: 1700000000, RemoteSize: 10},
	}
	sweep := &folderSweep{chunksByPath: make(map[util.FullPath][]*filer_pb.FileChunk)}
	message := &filer_pb.EventNotification{OldEntry: &filer_pb.Entry{Name: "d", IsDirectory: true}}
	kept, err := f.deleteSweptChild(ctx, stale, message, 0, sweep, false, true)
	if err != nil {
		t.Fatalf("deleteSweptChild: %v", err)
	}
	if !kept {
		t.Fatal("a remote-only entry that took the path must be kept")
	}
	if _, err := store.FindEntry(ctx, "/buckets/mybucket/d/f.txt"); err != nil {
		t.Fatalf("the remote-only owner must survive, got %v", err)
	}
	if len(stub.deleteCalls) != 0 {
		t.Fatalf("the owned remote object must be kept, got %v", stub.deleteCalls)
	}
}
