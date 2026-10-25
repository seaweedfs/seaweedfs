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

func TestPathCommitRecordsWrittenByDefault(t *testing.T) {
	store := newStubFilerStore()
	f := newReplayTestFiler(store)

	f.noteCommittedEvent(deleteEvent("/dir", "file", time.Now().UnixNano()))

	// a delete records both the change and the delete
	if n := pathCommitRecordCount(store); n != 2 {
		t.Fatalf("records = %d, want 2", n)
	}
}

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
