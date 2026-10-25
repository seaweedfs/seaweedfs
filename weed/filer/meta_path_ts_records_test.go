package filer

import (
	"context"
	"strings"
	"testing"
	"time"

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

// TestPathCommitRecordsOff: with the option off nothing is recorded, records
// written before are not read, and the off marker covers the change.
func TestPathCommitRecordsOff(t *testing.T) {
	store := newStubFilerStore()
	f := newReplayTestFiler(store)
	now := time.Now()
	path := util.NewFullPath("/dir", "file")
	// records written while the option was still on
	f.notePathChange(path, now.UnixNano(), true)
	f.pathCommitRecordsOff = true

	f.noteCommittedEvent(overwriteEvent("/dir", "other", now.UnixNano(), "x"))
	f.noteCommittedEvent(deleteEvent("/dir", "gone", now.UnixNano()))
	if n := pathCommitRecordCount(store); n != 2 {
		t.Fatalf("records = %d, want only the 2 written before the option went off", n)
	}
	offUntilNs, found, err := f.kvGetTsNs(context.Background(), []byte(metaPathOffKey))
	if err != nil || !found || offUntilNs <= now.UnixNano() {
		t.Fatalf("off marker = %d (found %v, err %v), want one past the unrecorded changes at %d", offUntilNs, found, err, now.UnixNano())
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
// overwrite the newer entry, while records written afterwards count again.
func TestPathCommitRecordsBackOn(t *testing.T) {
	store := newStubFilerStore()
	now := time.Now()
	path := util.NewFullPath("/dir", "file")

	// records on: the path's change an hour ago is recorded
	on := newReplayTestFiler(store)
	on.notePathChange(path, now.Add(-time.Hour).UnixNano(), false)

	// records off: the path changes again, unrecorded
	off := newReplayTestFiler(store)
	off.pathCommitRecordsOff = true
	store.entries[string(path)] = &Entry{FullPath: path, Attr: Attr{Mtime: now}, Content: []byte("changed while off")}
	off.noteCommittedEvent(overwriteEvent("/dir", "file", now.UnixNano(), "changed while off"))

	// records back on, as a restarted filer
	backOn := newReplayTestFiler(store)
	if err := backOn.Replay(overwriteEvent("/dir", "file", now.Add(-30*time.Minute).UnixNano(), "older peer write")); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	entry, err := store.FindEntry(context.Background(), path)
	if err != nil {
		t.Fatalf("FindEntry: %v", err)
	}
	if string(entry.Content) != "changed while off" {
		t.Fatalf("content = %q, want the change made while the records were off", entry.Content)
	}

	later := util.NewFullPath("/dir", "later")
	laterTsNs := now.Add(3 * pathCommitOffInterval).UnixNano()
	backOn.notePathChange(later, laterTsNs, false)
	tsNs, err := backOn.localCommitTsNs(context.Background(), later)
	if err != nil {
		t.Fatalf("localCommitTsNs: %v", err)
	}
	if tsNs != laterTsNs {
		t.Fatalf("localCommitTsNs = %d, want the record written after the off period %d", tsNs, laterTsNs)
	}
}
