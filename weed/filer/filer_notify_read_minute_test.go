package filer

import (
	"bytes"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

// TestPersistedLogScanStartCoversSpanningWindow pins the file-selection window.
// A log file is named for the start of the window it holds, and a window spans
// up to LogFlushInterval, so a cursor inside the window's later minutes must
// still open the file named for its earlier one -- otherwise the read reports
// "nothing on disk" for entries that are sitting in it, and the gap resolvers
// take that miss as proof the range is empty.
func TestPersistedLogScanStartCoversSpanningWindow(t *testing.T) {
	// A window sealed at 12:30:59 ending 12:31:58 is written to "12-30".
	sealed := time.Date(2026, 6, 29, 12, 30, 59, 0, time.UTC)
	fileMinute := sealed.Format("15-04")

	// A subscriber resuming mid-window must not sort past that file.
	for _, cursor := range []time.Time{
		sealed.Add(11 * time.Second), // 12:31:10, the reported case
		sealed.Add(59 * time.Second), // 12:31:58, the window's last entry
		sealed,                       // exactly the window start
	} {
		scanMinute := persistedLogScanStart(cursor).Format("15-04")
		if scanMinute > fileMinute {
			t.Fatalf("cursor %v scans from %q, which sorts past the file %q holding it",
				cursor.Format("15:04:05"), scanMinute, fileMinute)
		}
	}
}

// A cursor just after midnight has to reach back into the previous day.
func TestPersistedLogScanStartCrossesMidnight(t *testing.T) {
	cursor := time.Date(2026, 6, 29, 0, 0, 30, 0, time.UTC)
	scanFrom := persistedLogScanStart(cursor)
	if got, want := scanFrom.Format("2006-01-02"), "2026-06-28"; got != want {
		t.Fatalf("scan date = %q, want %q so the previous day's last file is listed", got, want)
	}
}

// TestSpanningLogFileIsNotSkipped pins the other half of the minute-boundary
// fix, against the predicate the iterator actually calls. Widening which files
// get listed is useless if the iterator then drops the spanning file, which it
// did by treating the following file's name as an upper bound on this file's
// contents -- it is not.
func TestSpanningLogFileIsNotSkipped(t *testing.T) {
	// Window sealed at 12:30:59, ending 12:31:58, written to "12-30".
	spanning := time.Date(2026, 6, 29, 12, 30, 0, 0, time.UTC).UnixNano()
	// The next window starts at 12:31:59 and is written to "12-31".
	following := time.Date(2026, 6, 29, 12, 31, 0, 0, time.UTC).UnixNano()

	cursor := time.Date(2026, 6, 29, 12, 31, 10, 0, time.UTC).UnixNano()
	if following > cursor {
		t.Fatal("precondition: the following file's name sorts at or before the cursor")
	}
	if !logFileMayContainAfter(spanning, cursor) {
		t.Fatal("the spanning file holds entries past the cursor and must be read")
	}

	// A file that genuinely cannot reach the cursor is still skipped, so the
	// widening does not turn into reading the whole day.
	old := time.Date(2026, 6, 29, 12, 0, 0, 0, time.UTC).UnixNano()
	if logFileMayContainAfter(old, cursor) {
		t.Fatal("a file a full interval behind the cursor should still be skipped")
	}
}

// TestClampLogRefsCursorNeverRewinds pins that a chunk-ref read cannot move the
// subscriber backwards. The refs carry minute-level names, so the last one
// normally sorts before the requested position, and the caller assigns that
// value straight to the read cursor.
func TestClampLogRefsCursorNeverRewinds(t *testing.T) {
	cursor := time.Date(2026, 6, 29, 12, 31, 10, 0, time.UTC).UnixNano()
	spanningFile := time.Date(2026, 6, 29, 12, 30, 0, 0, time.UTC).UnixNano()

	if got := clampLogRefsCursor(spanningFile, cursor); got != cursor {
		t.Fatalf("cursor moved to %v, want it held at %v", time.Unix(0, got), time.Unix(0, cursor))
	}
	// A ref genuinely ahead of the cursor still advances it.
	ahead := time.Date(2026, 6, 29, 12, 32, 0, 0, time.UTC).UnixNano()
	if got := clampLogRefsCursor(ahead, cursor); got != ahead {
		t.Fatalf("cursor = %v, want it to advance to %v", time.Unix(0, got), time.Unix(0, ahead))
	}
}

// TestPersistedLogScanStartTsNsMinuteAligned pins the prune bound against the
// collector's minute-granular file comparison: a cursor at 12:31:20 still
// collects the 12-30 file, so the bound must not sit past 12:30:00 - an
// exact-ns bound disowns the file's sent state and the next pass reships it
// whole, re-creating the duplicate-refs class the state exists to prevent.
func TestPersistedLogScanStartTsNsMinuteAligned(t *testing.T) {
	cursor := time.Date(2026, 6, 29, 12, 31, 20, 0, time.UTC)
	fileTsNs := time.Date(2026, 6, 29, 12, 30, 0, 0, time.UTC).UnixNano()

	bound := PersistedLogScanStartTsNs(cursor)
	if fileTsNs < bound {
		t.Fatalf("bound %v disowns the 12-30 file the collector still lists", time.Unix(0, bound).UTC())
	}
	// A file a full interval plus a minute behind is genuinely out of scan range.
	old := time.Date(2026, 6, 29, 12, 28, 0, 0, time.UTC).UnixNano()
	if old >= bound {
		t.Fatalf("bound %v keeps state for files the scan can no longer list", time.Unix(0, bound).UTC())
	}
}

// TestLastShippedLogEntryTsNs pins the chunk-mode cursor source: the answer
// comes from the shipped chunks alone, permanently missing chunks are skipped
// the way every replay path skips them - a dead volume must not wedge the
// stream ahead of its transition marker - and a legacy chunk whose records
// span boundaries falls back to streaming the shipped list itself.
func TestLastShippedLogEntryTsNs(t *testing.T) {
	f := &Filer{persistedLogCache: newPersistedLogCache(1 << 20)}

	entry := func(ts int64) *filer_pb.LogEntry { return &filer_pb.LogEntry{TsNs: ts} }
	chunk := func(id string) *filer_pb.FileChunk { return &filer_pb.FileChunk{FileId: id, Size: 10} }

	byId := map[string]struct {
		entries []*filer_pb.LogEntry
		err     error
	}{
		"good-early": {entries: []*filer_pb.LogEntry{entry(100), entry(200)}},
		"good-late":  {entries: []*filer_pb.LogEntry{entry(300), entry(400)}},
		"missing":    {err: fmt.Errorf("volume 7 not found")},
		"empty":      {},
	}
	origLoad := loadLogFileEntriesFn
	loadLogFileEntriesFn = func(masterClient *wdclient.MasterClient, c *filer_pb.FileChunk) ([]*filer_pb.LogEntry, bool, error) {
		r := byId[c.FileId]
		return r.entries, true, r.err
	}
	defer func() { loadLogFileEntriesFn = origLoad }()

	t.Run("last readable chunk answers", func(t *testing.T) {
		ts, ok, err := f.LastShippedLogEntryTsNs([]*filer_pb.FileChunk{chunk("good-early"), chunk("good-late")})
		if err != nil || !ok || ts != 400 {
			t.Fatalf("ts=%d ok=%v err=%v, want 400", ts, ok, err)
		}
	})

	t.Run("a missing tail walks back instead of failing", func(t *testing.T) {
		ts, ok, err := f.LastShippedLogEntryTsNs([]*filer_pb.FileChunk{chunk("good-early"), chunk("missing")})
		if err != nil || !ok || ts != 200 {
			t.Fatalf("ts=%d ok=%v err=%v, want the previous chunk's 200", ts, ok, err)
		}
	})

	t.Run("nothing readable is a skip, not an error", func(t *testing.T) {
		_, ok, err := f.LastShippedLogEntryTsNs([]*filer_pb.FileChunk{chunk("missing"), chunk("empty")})
		if err != nil || ok {
			t.Fatalf("ok=%v err=%v, want a clean no-answer", ok, err)
		}
	})

	t.Run("spanning records stream the shipped list", func(t *testing.T) {
		// The chunk refuses standalone decode; the streamed bytes carry two
		// length-prefixed entries and the fallback must return the last one.
		byIdIncomplete := chunk("incomplete")
		origLoad2 := loadLogFileEntriesFn
		loadLogFileEntriesFn = func(masterClient *wdclient.MasterClient, c *filer_pb.FileChunk) ([]*filer_pb.LogEntry, bool, error) {
			return nil, false, errLogChunkIncomplete
		}
		origStream := newLogFileStreamReader
		newLogFileStreamReader = func(masterClient *wdclient.MasterClient, chunks []*filer_pb.FileChunk) io.Reader {
			var buf bytes.Buffer
			for _, ts := range []int64{500, 600} {
				data, _ := (&filer_pb.LogEntry{TsNs: ts}).MarshalVT()
				var sizeBuf [4]byte
				util.Uint32toBytes(sizeBuf[:], uint32(len(data)))
				buf.Write(sizeBuf[:])
				buf.Write(data)
			}
			return &buf
		}
		defer func() { loadLogFileEntriesFn = origLoad2; newLogFileStreamReader = origStream }()

		ts, ok, err := f.LastShippedLogEntryTsNs([]*filer_pb.FileChunk{byIdIncomplete})
		if err != nil || !ok || ts != 600 {
			t.Fatalf("ts=%d ok=%v err=%v, want the streamed tail 600", ts, ok, err)
		}
	})
}
