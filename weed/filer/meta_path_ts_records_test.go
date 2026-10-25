package filer

import (
	"context"
	"testing"
	"time"

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
