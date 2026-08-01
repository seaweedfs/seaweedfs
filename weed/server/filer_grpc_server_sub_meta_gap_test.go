package weed_server

import (
	"context"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/util/log_buffer"
)

// TestResolveGapResume pins the one decision both subscribe paths share: a gap
// the disk read found empty may be skipped only when it is provably so - the
// ring never evicted past the cursor, or the flush watermark observed before
// the read had already passed the earliest in-memory time. The aggregated ring
// never flushes, so it is the flushedTsNs=0 column of this table.
func TestResolveGapResume(t *testing.T) {
	now := time.Date(2026, 6, 29, 12, 0, 0, 0, time.UTC).UnixNano()
	ago := func(d time.Duration) int64 { return now - int64(d) }

	cases := []struct {
		name            string
		currentTsNs     int64
		currentOffset   int64 // <= 0 is a sentinel (inclusive) cursor
		earliestMemTsNs int64
		flushedTsNs     int64 // 0 also models the never-flushing aggregated ring
		lastEvictedTsNs int64 // zero value is a ring that never evicted
		wantAdvance     bool
	}{
		{
			// The bug this PR fixes: the ring dropped the 30s..25s window
			// before it was flushed, so skipping past it loses those events.
			name:            "gap the ring dropped must NOT skip",
			currentTsNs:     ago(30 * time.Second),
			earliestMemTsNs: ago(25 * time.Second),
			lastEvictedTsNs: ago(26 * time.Second),
			wantAdvance:     false,
		},
		{
			// An ancient cursor is always below the watermark once anything has
			// been evicted: wall-clock age is not a licence to skip.
			name:            "ancient cursor below the watermark must NOT skip",
			currentTsNs:     time.Unix(0, 0).UnixNano(),
			earliestMemTsNs: ago(30 * time.Second),
			lastEvictedTsNs: ago(10 * time.Minute),
			wantAdvance:     false,
		},
		{
			name:            "watermark behind earliest must NOT skip (may be unflushed)",
			currentTsNs:     ago(40 * time.Second),
			earliestMemTsNs: ago(30 * time.Second),
			flushedTsNs:     ago(35 * time.Second),
			lastEvictedTsNs: ago(32 * time.Second),
			wantAdvance:     false,
		},
		{
			// Everything up to earliest was flushed before the read: the miss
			// is authoritative even though the ring dropped the gap.
			name:            "flush watermark at earliest proves the gap and skips",
			currentTsNs:     ago(40 * time.Second),
			earliestMemTsNs: ago(30 * time.Second),
			flushedTsNs:     ago(30 * time.Second),
			lastEvictedTsNs: ago(32 * time.Second),
			wantAdvance:     true,
		},
		{
			name:            "flush watermark past earliest skips",
			currentTsNs:     ago(10 * time.Minute),
			earliestMemTsNs: ago(30 * time.Second),
			flushedTsNs:     ago(10 * time.Second),
			lastEvictedTsNs: ago(32 * time.Second),
			wantAdvance:     true,
		},
		{
			// Nothing was ever evicted, so memory still holds the gap whatever
			// the flush watermark says and however old the cursor is.
			name:            "nothing evicted skips despite a stale flush watermark",
			currentTsNs:     ago(40 * time.Second),
			earliestMemTsNs: ago(30 * time.Second),
			flushedTsNs:     0,
			wantAdvance:     true,
		},
		{
			// The evicted window ends exactly on the cursor. Memory holds
			// nothing at that timestamp and the persisted reader skips
			// ts <= its start, so no wait can produce it and the rest of the
			// gap is in memory: skipping is the only way forward.
			name:            "cursor on the eviction watermark skips with a stalled flush",
			currentTsNs:     ago(30 * time.Second),
			earliestMemTsNs: ago(25 * time.Second),
			flushedTsNs:     0,
			lastEvictedTsNs: ago(30 * time.Second),
			wantAdvance:     true,
		},
		{
			name:            "memory not ahead of current must NOT skip",
			currentTsNs:     ago(20 * time.Second),
			earliestMemTsNs: ago(40 * time.Second),
			flushedTsNs:     ago(10 * time.Second),
			wantAdvance:     false,
		},
		{
			// time.Time{}.UnixNano() is a large negative value: no in-memory data.
			name:            "no in-memory data must NOT skip",
			currentTsNs:     ago(30 * time.Second),
			earliestMemTsNs: time.Time{}.UnixNano(),
			flushedTsNs:     ago(10 * time.Second),
			wantAdvance:     false,
		},
		{
			// Timestamp collision bumps make adjacent entries exactly 1ns
			// apart, so a delivered entry ending an evicted window leaves the
			// cursor exactly one below earliest. The resume target equals the
			// cursor, but the exclusive (positive-offset) cursor cannot be
			// served - ReadFromBuffer refuses positive offsets below the
			// window - while the sentinel resume is, and both deliver exactly
			// the entries after it. Refusing here parked a subscriber whose
			// data was entirely in memory.
			name:            "exclusive cursor adjacent to earliest re-arms to a sentinel",
			currentTsNs:     ago(30 * time.Second),
			currentOffset:   7, // a batch offset from a served memory read
			earliestMemTsNs: ago(30*time.Second) + 1,
			flushedTsNs:     0,
			lastEvictedTsNs: ago(30 * time.Second),
			wantAdvance:     true,
		},
		{
			// The same position already sentinel is served by the memory read,
			// so re-issuing it is not progress.
			name:            "sentinel cursor adjacent to earliest must NOT re-arm",
			currentTsNs:     ago(30 * time.Second),
			currentOffset:   -2,
			earliestMemTsNs: ago(30*time.Second) + 1,
			flushedTsNs:     0,
			lastEvictedTsNs: ago(30 * time.Second),
			wantAdvance:     false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotTo, gotAdvance := resolveGapResume(tc.currentTsNs, tc.currentOffset, tc.earliestMemTsNs, tc.flushedTsNs, tc.lastEvictedTsNs)
			if gotAdvance != tc.wantAdvance {
				t.Fatalf("advance = %v, want %v (current=%v earliest=%v flushed=%v evicted=%v)",
					gotAdvance, tc.wantAdvance, time.Unix(0, tc.currentTsNs), time.Unix(0, tc.earliestMemTsNs),
					time.Unix(0, tc.flushedTsNs), time.Unix(0, tc.lastEvictedTsNs))
			}
			if !gotAdvance {
				return
			}
			// The resume lands just below earliest so the earliest entry is
			// still delivered, even from a single-entry sealed window.
			if gotTo != tc.earliestMemTsNs-1 {
				t.Fatalf("advanceTo = %v, want just below earliest %v", time.Unix(0, gotTo), time.Unix(0, tc.earliestMemTsNs))
			}
		})
	}
}

// TestInclusiveDiskCursorOnWatermarkStillAdvances pins the disk-to-memory
// handoff. A cursor built from a disk position stays inclusive, because memory
// above the eviction watermark may hold a different entry sharing that
// timestamp and an exclusive cursor would drop it. That inclusive cursor can
// land exactly on the watermark, where the read gate sends it to disk and the
// persisted reader — which skips ts <= its start — returns nothing. Waiting
// cannot fix that, so the resolver must skip instead of parking.
func TestInclusiveDiskCursorOnWatermarkStillAdvances(t *testing.T) {
	now := time.Date(2026, 6, 29, 12, 0, 0, 0, time.UTC).UnixNano()
	watermark := now - int64(30*time.Second) // disk delivered exactly this far
	earliest := watermark + int64(time.Second)

	if !memoryHoldsGap(watermark, watermark) {
		t.Fatal("memory holds everything after the watermark, whatever the cursor's inclusivity")
	}
	// No flush has landed, so only the eviction proof can settle this.
	to, advance := resolveGapResume(watermark, -2, earliest, 0, watermark)
	if !advance {
		t.Fatal("an inclusive cursor on the watermark must still advance")
	}
	if to != earliest-1 {
		t.Fatalf("advanceTo = %v, want just below earliest %v", time.Unix(0, to), time.Unix(0, earliest))
	}

	// One nanosecond earlier the gap really was dropped unflushed: park.
	if _, advance := resolveGapResume(watermark-1, -2, earliest, 0, watermark); advance {
		t.Fatal("a cursor below the watermark with no flush proof must wait, not skip")
	}
}

// TestWaitOnGapExits pins every way a park has to end. The park is where a
// stalled subscriber spends all its time and it never re-enters the main loop,
// so any exit the loop relies on has to be honored here as well.
func TestWaitOnGapExits(t *testing.T) {
	fs := &FilerServer{knownListeners: map[int32]int32{7: 3}}
	req := &filer_pb.SubscribeMetadataRequest{ClientId: 7, ClientEpoch: 3}
	cursor := time.Now().UnixNano()

	t.Run("retries on the timer with no notification channel", func(t *testing.T) {
		start := time.Now()
		if got := fs.waitOnGap(context.Background(), req, cursor, nil, 0); got != gapWaitRetry {
			t.Fatalf("outcome = %v, want retry", got)
		}
		if elapsed := time.Since(start); elapsed < unflushedGapRetryInterval {
			t.Fatalf("returned after %v, want the full %v retry interval", elapsed, unflushedGapRetryInterval)
		}
	})

	t.Run("a replaced client ends the stream", func(t *testing.T) {
		// A reconnect at a higher epoch supersedes this stream; without this the
		// old one keeps scanning the filer store until its TCP connection dies.
		superseded := &filer_pb.SubscribeMetadataRequest{ClientId: 7, ClientEpoch: 2}
		if got := fs.waitOnGap(context.Background(), superseded, cursor, nil, 0); got != gapWaitDone {
			t.Fatalf("outcome = %v, want done", got)
		}
	})

	t.Run("a bounded subscription past its window ends the stream", func(t *testing.T) {
		// LoopProcessLogData is the only place UntilNs ends a stream and it is
		// unreachable from a park, so `weed shell fs.verify` would hang here.
		bounded := &filer_pb.SubscribeMetadataRequest{ClientId: 7, ClientEpoch: 3, UntilNs: cursor - 1}
		if got := fs.waitOnGap(context.Background(), bounded, cursor, nil, 0); got != gapWaitDone {
			t.Fatalf("outcome = %v, want done", got)
		}
	})

	t.Run("a stall past the bound fails the stream", func(t *testing.T) {
		if got := fs.waitOnGap(context.Background(), req, cursor, nil, maxGapStall); got != gapWaitStalled {
			t.Fatalf("outcome = %v, want stalled", got)
		}
	})

	t.Run("a cancelled stream ends promptly", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		start := time.Now()
		if got := fs.waitOnGap(ctx, req, cursor, nil, 0); got != gapWaitDone {
			t.Fatalf("outcome = %v, want done", got)
		}
		if elapsed := time.Since(start); elapsed >= unflushedGapRetryInterval {
			t.Fatalf("took %v, want well under the %v retry interval", elapsed, unflushedGapRetryInterval)
		}
	})

	t.Run("a closed notification channel does not spin", func(t *testing.T) {
		// A receive on a closed channel returns instantly forever; selecting on
		// it without checking would burn a core until the retry timer fires.
		closed := make(chan struct{})
		close(closed)
		start := time.Now()
		if got := fs.waitOnGap(context.Background(), req, cursor, closed, 0); got != gapWaitRetry {
			t.Fatalf("outcome = %v, want retry", got)
		}
		if elapsed := time.Since(start); elapsed < unflushedGapRetryInterval {
			t.Fatalf("returned after %v, want the timer to pace it to %v", elapsed, unflushedGapRetryInterval)
		}
	})

	t.Run("a notification wakes it early", func(t *testing.T) {
		notify := make(chan struct{}, 1)
		notify <- struct{}{}
		start := time.Now()
		if got := fs.waitOnGap(context.Background(), req, cursor, notify, 0); got != gapWaitRetry {
			t.Fatalf("outcome = %v, want retry", got)
		}
		if elapsed := time.Since(start); elapsed >= unflushedGapRetryInterval {
			t.Fatalf("took %v, want well under the %v retry interval", elapsed, unflushedGapRetryInterval)
		}
	})
}

// TestGapResumeCursorOffsetIsSentinel ties the resolvers' resume cursor to what
// ReadFromBuffer will serve. That read falls through to memory for a cursor
// below the in-memory window only when the offset is a sentinel; a positive one
// comes back as ResumeFromDiskError, so the resume would bounce to the resolver,
// which sees no progress and parks a subscriber whose data is in the ring.
func TestGapResumeCursorOffsetIsSentinel(t *testing.T) {
	if gapResumeCursorOffset > 0 {
		t.Fatalf("gapResumeCursorOffset = %d, want a sentinel (<= 0) or the memory read refuses every gap resume",
			gapResumeCursorOffset)
	}
}

// TestDiskReadAdvancedRequiresForwardProgress pins that a persisted read only
// counts as progress when it actually moves the cursor. A chunk-ref read
// reports the minute-level name of the last file it shipped, clamped so it
// never rewinds, so it comes back non-zero while naming the position that was
// already current -- and a subscriber parked on a gap it keeps re-shipping the
// same refs for would clear its stall timer on every retry and never reach the
// bound that is supposed to end it.
func TestDiskReadAdvancedRequiresForwardProgress(t *testing.T) {
	cursorTsNs := time.Date(2026, 6, 29, 12, 31, 10, 0, time.UTC).UnixNano()
	cursor := log_buffer.NewMessagePosition(cursorTsNs, gapResumeCursorOffset)

	if diskReadAdvanced(0, cursor) {
		t.Fatal("an empty disk read is not progress")
	}
	if diskReadAdvanced(cursorTsNs, cursor) {
		t.Fatal("a read reporting the position already held is not progress")
	}
	if diskReadAdvanced(cursorTsNs-1, cursor) {
		t.Fatal("a read reporting an earlier position is not progress")
	}
	if !diskReadAdvanced(cursorTsNs+1, cursor) {
		t.Fatal("a read that moves the cursor forward is progress")
	}
}

// TestReportUnprovenAggregatedCrossing pins which advances are flagged. The
// eviction watermark belongs to the merged ring while the disk behind it is the
// union of each peer's own log, so a read that lifts the cursor from below the
// watermark to above it may have done so entirely on a peer that is ahead --
// leaving a lagging peer's unflushed events inside the range just crossed.
func TestReportUnprovenAggregatedCrossing(t *testing.T) {
	const (
		before  = 10
		evicted = 20
		after   = 25
	)
	crossings := func() float64 {
		var m dto.Metric
		if err := stats.FilerSubscribeUnprovenGapCrossings.Write(&m); err != nil {
			t.Fatalf("read counter: %v", err)
		}
		return m.GetCounter().GetValue()
	}

	start := crossings()
	// Nothing evicted: no range to cross.
	reportUnprovenAggregatedCrossing(before, after, 0, "c", "/")
	// Cursor already past the watermark: the evicted range was behind it.
	reportUnprovenAggregatedCrossing(evicted, after, evicted, "c", "/")
	// Cursor still short of the watermark: the gap is open, not crossed.
	reportUnprovenAggregatedCrossing(before, evicted-1, evicted, "c", "/")
	if got := crossings(); got != start {
		t.Fatalf("counter moved by %v on advances that cross nothing", got-start)
	}

	// From below the watermark to above it: unproven.
	reportUnprovenAggregatedCrossing(before, after, evicted, "c", "/")
	if got := crossings(); got != start+1 {
		t.Fatalf("counter = %v, want %v after one unproven crossing", got, start+1)
	}
}
