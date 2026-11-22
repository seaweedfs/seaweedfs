package filer

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/log_buffer"
)

// countingListStore counts the entries the store's listings hand out.
type countingListStore struct {
	*stubFilerStore
	listed atomic.Int64
}

func (s *countingListStore) ListDirectoryEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, eachEntryFunc ListEachEntryFunc) (string, error) {
	return s.stubFilerStore.ListDirectoryEntries(ctx, dirPath, startFileName, includeStartFile, limit, func(entry *Entry) (bool, error) {
		s.listed.Add(1)
		return eachEntryFunc(entry)
	})
}

func (s *countingListStore) ListDirectoryPrefixedEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, prefix string, eachEntryFunc ListEachEntryFunc) (string, error) {
	return s.stubFilerStore.ListDirectoryPrefixedEntries(ctx, dirPath, startFileName, includeStartFile, limit, prefix, func(entry *Entry) (bool, error) {
		s.listed.Add(1)
		return eachEntryFunc(entry)
	})
}

var listingTestFilerIds = []string{"0000abcd", "0000beef", "0000cafe"}

// fillLogDay puts one log file per minute and filer, up to lastMinute, into
// the day's metadata log directory, the way the filers write them.
func fillLogDay(store *stubFilerStore, day string, lastMinute int, withChunks bool) {
	dayDir := util.NewFullPath(SystemLogDir, day)
	store.entries[string(dayDir)] = &Entry{FullPath: dayDir, Attr: Attr{Mode: os.ModeDir | 0755}}
	for m := 0; m <= lastMinute; m++ {
		for _, filerId := range listingTestFilerIds {
			p := util.NewFullPath(string(dayDir), fmt.Sprintf("%02d-%02d.%s", m/60, m%60, filerId))
			entry := &Entry{FullPath: p}
			if withChunks {
				entry.Chunks = []*filer_pb.FileChunk{{FileId: "1,0a", Size: 10}}
			}
			store.entries[string(p)] = entry
		}
	}
}

// The scan for a cursor at 12:30:30 starts one flush interval earlier, at
// 12:29:30, so it wants the files of 12-29 and 12-30 from each filer.
var listingTestCursor = time.Date(2026, 10, 25, 12, 30, 30, 0, time.UTC)

// TestCollectLogFileRefsListsFromScanStart: the start day's files before the
// scan start's minute are not listed at all, and the refs are the ones a
// whole-day listing followed by the name filter gave.
func TestCollectLogFileRefsListsFromScanStart(t *testing.T) {
	store := &countingListStore{stubFilerStore: newStubFilerStore()}
	f := newReplayTestFiler(store)
	fillLogDay(store.stubFilerStore, "2026-10-25", 12*60+30, true)

	refs, _, err := f.CollectLogFileRefs(context.Background(), log_buffer.NewMessagePosition(listingTestCursor.UnixNano(), -2), 0)
	if err != nil {
		t.Fatalf("CollectLogFileRefs: %v", err)
	}
	want := map[int64]int{
		time.Date(2026, 10, 25, 12, 29, 0, 0, time.UTC).UnixNano(): len(listingTestFilerIds),
		time.Date(2026, 10, 25, 12, 30, 0, 0, time.UTC).UnixNano(): len(listingTestFilerIds),
	}
	got := map[int64]int{}
	for _, ref := range refs {
		got[ref.FileTsNs]++
	}
	if len(got) != len(want) || got[time.Date(2026, 10, 25, 12, 29, 0, 0, time.UTC).UnixNano()] != len(listingTestFilerIds) || got[time.Date(2026, 10, 25, 12, 30, 0, 0, time.UTC).UnixNano()] != len(listingTestFilerIds) {
		t.Fatalf("refs by file minute = %v, want %v", got, want)
	}
	// the day directory, then the six files
	if n := store.listed.Load(); n != 1+2*int64(len(listingTestFilerIds)) {
		t.Fatalf("listed %d entries for %d log files", n, len(refs))
	}
}

// TestPersistedLogScanListsFromScanStart: the same for the pass that reads the
// persisted log for a subscription.
func TestPersistedLogScanListsFromScanStart(t *testing.T) {
	store := &countingListStore{stubFilerStore: newStubFilerStore()}
	f := newReplayTestFiler(store)
	// cached mode: the files hold no chunks, so nothing is read from volumes
	f.persistedLogCache = newPersistedLogCache(1 << 20)
	fillLogDay(store.stubFilerStore, "2026-10-25", 12*60+30, false)

	v, err := f.collectPersistedLogBuffer(log_buffer.NewMessagePosition(listingTestCursor.UnixNano(), -2), 0)
	if err != nil {
		t.Fatalf("collectPersistedLogBuffer: %v", err)
	}
	if len(v.perFilerIteratorMap) != len(listingTestFilerIds) {
		t.Fatalf("iterators for %d filers, want %d", len(v.perFilerIteratorMap), len(listingTestFilerIds))
	}
	// the day directory, then the six files
	if n := store.listed.Load(); n != 1+2*int64(len(listingTestFilerIds)) {
		t.Fatalf("listed %d entries, want %d", n, 1+2*len(listingTestFilerIds))
	}
}

// TestCollectLogFileRefsListsLaterDaysWhole: only the start day is listed from
// the scan start's minute; a scan that runs into the next day lists that day
// from its beginning.
func TestCollectLogFileRefsListsLaterDaysWhole(t *testing.T) {
	store := &countingListStore{stubFilerStore: newStubFilerStore()}
	f := newReplayTestFiler(store)
	fillLogDay(store.stubFilerStore, "2026-10-25", 24*60-1, true)
	fillLogDay(store.stubFilerStore, "2026-10-26", 9, true)

	cursor := time.Date(2026, 10, 25, 23, 58, 30, 0, time.UTC)
	refs, _, err := f.CollectLogFileRefs(context.Background(), log_buffer.NewMessagePosition(cursor.UnixNano(), -2), 0)
	if err != nil {
		t.Fatalf("CollectLogFileRefs: %v", err)
	}
	// 23-57, 23-58 and 23-59 of the first day, then 00-00 to 00-09
	if wantRefs := (3 + 10) * len(listingTestFilerIds); len(refs) != wantRefs {
		t.Fatalf("refs = %d, want %d", len(refs), wantRefs)
	}
	// two day directories, then the files
	if n := store.listed.Load(); n != 2+int64(len(refs)) {
		t.Fatalf("listed %d entries for %d log files", n, len(refs))
	}
}

// unorderedListStore lists like countingListStore but declares its listings
// unordered, as the Elasticsearch store does.
type unorderedListStore struct{ *countingListStore }

func (s *unorderedListStore) ListsUnordered() bool { return true }

// TestLogListingsOnUnorderedStoreListWholeDay: a store whose listings are not
// in name order cannot start at the scan start's minute, so there both passes
// list the whole start day, and still find the same files.
func TestLogListingsOnUnorderedStoreListWholeDay(t *testing.T) {
	wholeDay := int64(1 + (12*60+31)*len(listingTestFilerIds))
	newStore := func(withChunks bool) (*countingListStore, *Filer) {
		counting := &countingListStore{stubFilerStore: newStubFilerStore()}
		f := newReplayTestFiler(&unorderedListStore{countingListStore: counting})
		f.persistedLogCache = newPersistedLogCache(1 << 20)
		fillLogDay(counting.stubFilerStore, "2026-10-25", 12*60+30, withChunks)
		return counting, f
	}

	t.Run("CollectLogFileRefs", func(t *testing.T) {
		store, f := newStore(true)
		refs, _, err := f.CollectLogFileRefs(context.Background(), log_buffer.NewMessagePosition(listingTestCursor.UnixNano(), -2), 0)
		if err != nil {
			t.Fatalf("CollectLogFileRefs: %v", err)
		}
		if len(refs) != 2*len(listingTestFilerIds) {
			t.Fatalf("refs = %d, want %d", len(refs), 2*len(listingTestFilerIds))
		}
		if n := store.listed.Load(); n != wholeDay {
			t.Fatalf("listed %d entries, want the whole day: %d", n, wholeDay)
		}
	})
	t.Run("persisted log scan", func(t *testing.T) {
		store, f := newStore(false)
		v, err := f.collectPersistedLogBuffer(log_buffer.NewMessagePosition(listingTestCursor.UnixNano(), -2), 0)
		if err != nil {
			t.Fatalf("collectPersistedLogBuffer: %v", err)
		}
		if len(v.perFilerIteratorMap) != len(listingTestFilerIds) {
			t.Fatalf("iterators for %d filers, want %d", len(v.perFilerIteratorMap), len(listingTestFilerIds))
		}
		if n := store.listed.Load(); n != wholeDay {
			t.Fatalf("listed %d entries, want the whole day: %d", n, wholeDay)
		}
	})
}

// TestLogListingsOnUnorderedDayStoreListWholeDay: a path-specific store that
// serves the start day's directory decides how that day is listed, even when
// the default store lists in name order.
func TestLogListingsOnUnorderedDayStoreListWholeDay(t *testing.T) {
	ordered := &countingListStore{stubFilerStore: newStubFilerStore()}
	fillLogDay(ordered.stubFilerStore, "2026-10-25", 12*60+30, true)
	dayDir := string(util.NewFullPath(SystemLogDir, "2026-10-25"))
	day := &countingListStore{stubFilerStore: newStubFilerStore()}
	for p, entry := range ordered.entries {
		if strings.HasPrefix(p, dayDir+"/") {
			entry.FullPath = util.FullPath(p[len(dayDir):])
			day.entries[p[len(dayDir):]] = entry
			delete(ordered.entries, p)
		}
	}
	f := newReplayTestFiler(ordered)
	f.Store.AddPathSpecificStore(dayDir+"/", "logday", &unorderedListStore{countingListStore: day})

	refs, _, err := f.CollectLogFileRefs(context.Background(), log_buffer.NewMessagePosition(listingTestCursor.UnixNano(), -2), 0)
	if err != nil {
		t.Fatalf("CollectLogFileRefs: %v", err)
	}
	if len(refs) != 2*len(listingTestFilerIds) {
		t.Fatalf("refs = %d, want %d", len(refs), 2*len(listingTestFilerIds))
	}
	if n, wholeDay := day.listed.Load(), int64((12*60+31)*len(listingTestFilerIds)); n != wholeDay {
		t.Fatalf("day store listed %d entries, want the whole day: %d", n, wholeDay)
	}
}
