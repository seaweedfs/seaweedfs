package filer

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/log_buffer"
)

// Committed entries carry their own commit timestamp; deletes write
// nothing — no per-path record and no tombstone.
func TestPathCommitStampedOnEntry(t *testing.T) {
	store := newStubFilerStore()
	f := newReplayTestFiler(store)
	ctx := context.Background()
	entry := &Entry{FullPath: "/dir/file", Content: []byte("data")}
	f.StampEntryCommitTsNs(entry)
	if err := store.InsertEntry(ctx, entry); err != nil {
		t.Fatalf("InsertEntry: %v", err)
	}
	f.releaseEntryCommitTsNs(entry.CommitTsNs)
	if got, err := f.CommitTsNs(ctx, "/dir/file", 0); err != nil || got != entry.CommitTsNs {
		t.Fatalf("CommitTsNs = %d,%v; want %d", got, err, entry.CommitTsNs)
	}
	if len(store.kv) != 0 {
		t.Fatalf("kv keys written for a change event: %v", store.kv)
	}

	if err := store.DeleteEntry(ctx, entry.FullPath); err != nil {
		t.Fatalf("DeleteEntry: %v", err)
	}
	if len(store.kv) != 0 {
		t.Fatalf("kv keys written for a change or delete: %v", store.kv)
	}
}

// The emitted event's TsNs is the commit timestamp stamped on the entry
// before its store write — identical values so peers and the local store
// agree on last-writer-wins.
func TestEventTsNsEqualsEntryCommitTs(t *testing.T) {
	f := &Filer{
		Signature: 7,
		LocalMetaLogBuffer: log_buffer.NewLogBuffer(
			"commit-ts-test", time.Minute,
			func(*log_buffer.LogBuffer, time.Time, time.Time, []byte, int64, int64) {},
			nil, nil,
		),
	}
	defer f.LocalMetaLogBuffer.ShutdownLogBuffer()

	ctx, sink := WithMetadataEventSink(context.Background())
	newEntry := &Entry{FullPath: util.FullPath("/dir/file")}
	f.StampEntryCommitTsNs(newEntry)
	f.NotifyUpdateEvent(ctx, nil, newEntry, true, false, nil)

	event := sink.Last()
	if event == nil {
		t.Fatal("expected metadata event")
	}
	if event.TsNs != newEntry.CommitTsNs {
		t.Fatalf("event.TsNs = %d, entry.CommitTsNs = %d; must be equal", event.TsNs, newEntry.CommitTsNs)
	}
	if got := f.metaLogInflight.minTsNs(); got != 0 {
		t.Fatalf("stamp still in flight after emit: %d", got)
	}
}

// A notify that never saw a pre-stamp still emits at a fresh stamp — the
// stamp/release pairing must hold either way.
func TestEventWithoutPreStampStillEmits(t *testing.T) {
	f := &Filer{
		Signature: 7,
		LocalMetaLogBuffer: log_buffer.NewLogBuffer(
			"commit-ts-fresh", time.Minute,
			func(*log_buffer.LogBuffer, time.Time, time.Time, []byte, int64, int64) {},
			nil, nil,
		),
	}
	defer f.LocalMetaLogBuffer.ShutdownLogBuffer()

	ctx, sink := WithMetadataEventSink(context.Background())
	f.NotifyUpdateEvent(ctx, nil, &Entry{FullPath: util.FullPath("/dir/file")}, true, false, nil)
	if sink.Last() == nil || sink.Last().TsNs == 0 {
		t.Fatal("expected stamped metadata event")
	}
	if got := f.metaLogInflight.minTsNs(); got != 0 {
		t.Fatalf("stamp leaked: %d", got)
	}
}

// putRecord seeds the per-path key an older build would have left.
func putRecord(t *testing.T, store *stubFilerStore, prefix string, path util.FullPath, tsNs int64) {
	t.Helper()
	value := make([]byte, 8)
	util.Uint64toBytes(value, uint64(tsNs))
	if err := store.KvPut(context.Background(), metaPathKey(prefix, path), value); err != nil {
		t.Fatalf("seed record %s%s: %v", prefix, path, err)
	}
}

// A retained change record still orders a path whose entry is gone: a
// replayed create older than the record must not resurrect it.
func TestRetainedChangeRecordFencesMissingEntry(t *testing.T) {
	store := newStubFilerStore()
	f := newReplayTestFiler(store)
	ctx := context.Background()
	putRecord(t, store, metaPathTsPrefix, "/dir/file", 300)

	if newer, err := f.replayBlockedByLocal(ctx, "/dir/file", 200); err != nil || !newer {
		t.Fatalf("event at 200 should lose to retained record at 300: newer=%v err=%v", newer, err)
	}
	if newer, err := f.replayBlockedByLocal(ctx, "/dir/file", 400); err != nil || newer {
		t.Fatalf("event at 400 should win over retained record at 300: newer=%v err=%v", newer, err)
	}
}

// A directory's retained delete record covers children removed without
// per-child records — the replay check walks the ancestors.
func TestRetainedAncestorDeleteRecordFencesChild(t *testing.T) {
	store := newStubFilerStore()
	f := newReplayTestFiler(store)
	ctx := context.Background()
	putRecord(t, store, metaPathDelPrefix, "/dir", 300)

	if newer, err := f.replayBlockedByLocal(ctx, "/dir/sub/file", 200); err != nil || !newer {
		t.Fatalf("child event at 200 should lose to ancestor delete at 300: newer=%v err=%v", newer, err)
	}
	if newer, err := f.replayBlockedByLocal(ctx, "/dir/sub/file", 400); err != nil || newer {
		t.Fatalf("child event at 400 should win over ancestor delete at 300: newer=%v err=%v", newer, err)
	}
}

// The retained record orders the entry, not its client-settable mtime: a
// mounted client can push mtime into the future, and the record must not
// let that silence newer replayed events.
func TestRetainedRecordOrdersUnstampedEntry(t *testing.T) {
	store := newStubFilerStore()
	f := newReplayTestFiler(store)
	ctx := context.Background()
	path := util.FullPath("/dir/file")
	putRecord(t, store, metaPathTsPrefix, path, 300)
	store.entries[string(path)] = &Entry{
		FullPath: path,
		Attr:     Attr{Mtime: time.Unix(0, 999)},
	}

	if tsNs, err := f.localCommitTsNs(ctx, path); err != nil || tsNs != 300 {
		t.Fatalf("localCommitTsNs = %d,%v; want the record's 300, not the entry's mtime", tsNs, err)
	}
}

// An entry stamped under the current build outranks any record an older
// build left: the stamp is the newest commit.
func TestEntryStampBeatsRetainedRecord(t *testing.T) {
	store := newStubFilerStore()
	f := newReplayTestFiler(store)
	ctx := context.Background()
	path := util.FullPath("/dir/file")
	putRecord(t, store, metaPathTsPrefix, path, 300)
	store.entries[string(path)] = &Entry{FullPath: path, CommitTsNs: 400}

	if tsNs, err := f.localCommitTsNs(ctx, path); err != nil || tsNs != 400 {
		t.Fatalf("localCommitTsNs = %d,%v; want the entry stamp's 400", tsNs, err)
	}
}

// End to end: a replayed create older than a retained delete record leaves
// the path absent.
func TestReplayCreateBlockedByRetainedDelete(t *testing.T) {
	store := newStubFilerStore()
	f := newReplayTestFiler(store)
	ctx := context.Background()
	putRecord(t, store, metaPathDelPrefix, "/dir/file", 300)

	event := &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		EventNotification: &filer_pb.EventNotification{
			NewEntry: &filer_pb.Entry{Name: "file", Content: []byte("resurrected")},
		},
		TsNs: 200,
	}
	if err := f.Replay(event); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if _, err := store.FindEntry(ctx, "/dir/file"); err == nil {
		t.Fatal("replayed create at 200 must not resurrect a path deleted at 300")
	}
}
