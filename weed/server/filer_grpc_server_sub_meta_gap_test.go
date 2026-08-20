package weed_server

import (
	"context"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/util/log_buffer"
)

// TestResolveGapResume pins the one decision both subscribe paths share: a gap
// the disk read found empty may be skipped only when it is provably so - the
// ring never evicted past the cursor, or the flush watermark observed before
// the read had already passed the earliest in-memory time. The flushedTsNs=0
// column models an aggregated pass whose peers have not proven anything yet.
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

// TestGapPassProvenEmptyCrossing pins the crossing that needs no memory to
// land in: a cursor below the eviction watermark whose last empty disk pass
// proved coverage through the watermark itself (everything at or below it was
// flushed and inside the pass's listing) crosses to the watermark silently -
// no park, no loss counter. This is how a subscriber gets past the aggregated
// ring's marked pre-subscription boundary, which no rotation will ever prove.
func TestGapPassProvenEmptyCrossing(t *testing.T) {
	now := time.Date(2026, 6, 29, 12, 0, 0, 0, time.UTC).UnixNano()
	evicted := now - int64(time.Minute) // the ring's boundary (e.g. its startFrom mark)
	cursorTs := evicted - int64(2*time.Minute)

	crossings := func() float64 {
		m := &dto.Metric{}
		if err := stats.FilerSubscribeUnprovenGapCrossings.WithLabelValues("aggregated").Write(m); err != nil {
			t.Fatal(err)
		}
		return m.GetCounter().GetValue()
	}

	proven := evicted
	p := &gapPass{
		gapStall: &gapStallReporter{scope: "aggregated", clientName: "c", pathPrefix: "/"},
		earliest: func() time.Time { return time.Time{} }, // ring still empty
		evicted:  func() int64 { return evicted },
		flushed:  func() int64 { return proven },
	}

	cursor := log_buffer.NewMessagePosition(cursorTs, gapResumeCursorOffset)
	latch := error(log_buffer.ResumeFromDiskError) // stale latch must not survive the move
	start := crossings()
	if got := p.resolve(context.Background(), &cursor, &latch, false); got != gapContinue {
		t.Fatalf("outcome = %v, want gapContinue", got)
	}
	if got := cursor.Time.UnixNano(); got != evicted {
		t.Fatalf("cursor = %v, want the proven watermark %v", time.Unix(0, got), time.Unix(0, evicted))
	}
	if latch != nil {
		t.Fatalf("latch not cleared: %v", latch)
	}
	if got := crossings(); got != start {
		t.Fatalf("proven crossing moved the loss counter by %v", got-start)
	}

	// diskAdvanced defers everything: the disk may hold more of the gap.
	cursor = log_buffer.NewMessagePosition(cursorTs, gapResumeCursorOffset)
	if got := p.resolve(context.Background(), &cursor, &latch, true); got != gapContinue {
		t.Fatalf("diskAdvanced: outcome = %v, want gapContinue", got)
	}
	if got := cursor.Time.UnixNano(); got != cursorTs {
		t.Fatalf("diskAdvanced: cursor moved to %v", time.Unix(0, got))
	}
}

// TestChunkRefsStopTsNs pins the chunk listing bound: the newest admitted file
// name must sit a minute plus a flush interval below the hold, so no shipped
// file can hold an entry past it (a file is named for its window's start
// minute and the window spans up to a flush interval); UntilNs caps it.
func TestChunkRefsStopTsNs(t *testing.T) {
	hold := time.Date(2026, 6, 29, 12, 10, 45, 500, time.UTC).UnixNano()

	stop := chunkRefsStopTsNs(hold, 0)
	newestFileTsNs := stop - stop%int64(time.Minute)
	if newestFileTsNs+int64(time.Minute)+int64(filer.LogFlushInterval) > hold {
		t.Fatalf("file named %v may hold entries past the hold %v",
			time.Unix(0, newestFileTsNs), time.Unix(0, hold))
	}
	want := time.Date(2026, 6, 29, 12, 8, 59, 999999999, time.UTC).UnixNano()
	if stop != want {
		t.Fatalf("stop = %v, want %v", time.Unix(0, stop), time.Unix(0, want))
	}

	if got := chunkRefsStopTsNs(hold, want-5); got != want-5 {
		t.Fatalf("UntilNs cap: got %v, want %v", time.Unix(0, got), time.Unix(0, want-5))
	}
	if got := chunkRefsStopTsNs(hold, want+5); got != want {
		t.Fatalf("UntilNs above the bound must not widen it: got %v", time.Unix(0, got))
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

// TestParkOnGapExits pins every way a park has to end. The park is where a
// stalled subscriber spends all its time and it never re-enters the read loop,
// so any exit the loop relies on has to be honored here as well - and the done
// exits must run before the reporter marks the stream parked, or a healthy
// completion leaves a phantom "still behind" trace.
func TestParkOnGapExits(t *testing.T) {
	fs := &FilerServer{knownListeners: map[int32]int32{7: 3}}
	req := &filer_pb.SubscribeMetadataRequest{ClientId: 7, ClientEpoch: 3}
	cursorTs := time.Now().UnixNano()
	cursor := log_buffer.NewMessagePosition(cursorTs, -2)
	noEviction := func() int64 { return 0 }
	newStall := func() *gapStallReporter {
		return &gapStallReporter{scope: "aggregated", clientName: "c", pathPrefix: "/"}
	}

	t.Run("retries on the timer with no notification channel", func(t *testing.T) {
		gapStall := newStall()
		defer gapStall.resumed()
		start := time.Now()
		_, skip, done := fs.parkOnGap(context.Background(), req, gapStall, noEviction, cursor, nil, "test")
		if skip || done {
			t.Fatalf("skip=%v done=%v, want a plain retry", skip, done)
		}
		if elapsed := time.Since(start); elapsed < unflushedGapRetryInterval {
			t.Fatalf("returned after %v, want the full %v retry interval", elapsed, unflushedGapRetryInterval)
		}
	})

	t.Run("a replaced client ends the stream without parking", func(t *testing.T) {
		// A reconnect at a higher epoch supersedes this stream; without this the
		// old one keeps scanning the filer store until its TCP connection dies.
		gapStall := newStall()
		superseded := &filer_pb.SubscribeMetadataRequest{ClientId: 7, ClientEpoch: 2}
		if _, _, done := fs.parkOnGap(context.Background(), superseded, gapStall, noEviction, cursor, nil, "test"); !done {
			t.Fatal("want done")
		}
		if !gapStall.since.IsZero() {
			t.Fatal("a finished stream must not be marked parked")
		}
	})

	t.Run("a bounded subscription past its window ends without parking", func(t *testing.T) {
		gapStall := newStall()
		bounded := &filer_pb.SubscribeMetadataRequest{ClientId: 7, ClientEpoch: 3, UntilNs: cursorTs - 1}
		if _, _, done := fs.parkOnGap(context.Background(), bounded, gapStall, noEviction, cursor, nil, "test"); !done {
			t.Fatal("want done")
		}
		if !gapStall.since.IsZero() {
			t.Fatal("a finished stream must not be marked parked")
		}
	})

	t.Run("a bounded subscription exactly at its window ends without parking", func(t *testing.T) {
		// The bound is inclusive and cursors are exclusive: a disk read whose
		// last entry sits exactly on UntilNs leaves the cursor there with
		// everything <= UntilNs delivered. Parking would make fs.verify hang.
		gapStall := newStall()
		bounded := &filer_pb.SubscribeMetadataRequest{ClientId: 7, ClientEpoch: 3, UntilNs: cursorTs}
		if _, _, done := fs.parkOnGap(context.Background(), bounded, gapStall, noEviction, cursor, nil, "test"); !done {
			t.Fatal("want done")
		}
		if !gapStall.since.IsZero() {
			t.Fatal("a finished stream must not be marked parked")
		}
	})

	t.Run("a cancelled stream ends promptly", func(t *testing.T) {
		gapStall := newStall()
		defer gapStall.resumed()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		start := time.Now()
		if _, _, done := fs.parkOnGap(ctx, req, gapStall, noEviction, cursor, nil, "test"); !done {
			t.Fatal("want done")
		}
		if elapsed := time.Since(start); elapsed >= unflushedGapRetryInterval {
			t.Fatalf("took %v, want well under the %v retry interval", elapsed, unflushedGapRetryInterval)
		}
	})

	t.Run("a closed notification channel does not spin", func(t *testing.T) {
		// A receive on a closed channel returns instantly forever; selecting on
		// it without checking would burn a core until the retry timer fires.
		gapStall := newStall()
		defer gapStall.resumed()
		closed := make(chan struct{})
		close(closed)
		start := time.Now()
		_, skip, done := fs.parkOnGap(context.Background(), req, gapStall, noEviction, cursor, closed, "test")
		if skip || done {
			t.Fatalf("skip=%v done=%v, want a plain retry", skip, done)
		}
		if elapsed := time.Since(start); elapsed < unflushedGapRetryInterval {
			t.Fatalf("returned after %v, want the timer to pace it to %v", elapsed, unflushedGapRetryInterval)
		}
	})

	t.Run("a notification wakes it early", func(t *testing.T) {
		gapStall := newStall()
		defer gapStall.resumed()
		notify := make(chan struct{}, 1)
		notify <- struct{}{}
		start := time.Now()
		_, skip, done := fs.parkOnGap(context.Background(), req, gapStall, noEviction, cursor, notify, "test")
		if skip || done {
			t.Fatalf("skip=%v done=%v, want a plain retry", skip, done)
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
		if err := stats.FilerSubscribeUnprovenGapCrossings.WithLabelValues("aggregated").Write(&m); err != nil {
			t.Fatalf("read counter: %v", err)
		}
		return m.GetCounter().GetValue()
	}

	start := crossings()
	// Nothing evicted: no range to cross.
	reportUnprovenAggregatedCrossing(before, after, 0, 0, "c", "/")
	// Cursor already past the watermark: the evicted range was behind it.
	reportUnprovenAggregatedCrossing(evicted, after, evicted, 0, "c", "/")
	// Cursor still short of the watermark: the gap is open, not crossed.
	reportUnprovenAggregatedCrossing(before, evicted-1, evicted, 0, "c", "/")
	// Crossed, but every peer's flush watermark already passed the eviction
	// boundary: the crossed range was fully on peer disks — proven, not counted.
	reportUnprovenAggregatedCrossing(before, after, evicted, evicted, "c", "/")
	if got := crossings(); got != start {
		t.Fatalf("counter moved by %v on advances that cross nothing", got-start)
	}

	// From below the watermark to above it, flush watermark short of the
	// boundary: unproven.
	reportUnprovenAggregatedCrossing(before, after, evicted, evicted-1, "c", "/")
	if got := crossings(); got != start+1 {
		t.Fatalf("counter = %v, want %v after one unproven crossing", got, start+1)
	}
}

// TestParkOnGapStallOutcomes pins what a park that outlived maxGapStall does.
// Failing the stream would just move the loop into the client, which
// reconnects at the same position and hits the same wall delivering nothing;
// instead the subscriber abandons the gap, loudly: the skip lands exactly on
// the eviction watermark (everything retained starts strictly after it) and
// the unproven-crossing counter records the loss. A stall with nothing evicted
// past the cursor has nothing to skip and keeps waiting on a fresh cycle.
func TestParkOnGapStallOutcomes(t *testing.T) {
	fs := &FilerServer{knownListeners: map[int32]int32{7: 3}}
	req := &filer_pb.SubscribeMetadataRequest{ClientId: 7, ClientEpoch: 3}

	lb := log_buffer.NewLogBuffer("park-stall", time.Minute, nil, nil, nil)
	defer lb.ShutdownLogBuffer()
	// Entries far enough apart that every append seals the window before it;
	// the ring evicts once it wraps, moving the watermark for real.
	base := time.Now().Add(-time.Hour).Truncate(time.Second)
	for i := 0; i < log_buffer.PreviousBufferCount+2; i++ {
		if err := lb.AddLogEntryToBuffer(&filer_pb.LogEntry{
			TsNs: base.Add(time.Duration(i) * 2 * time.Minute).UnixNano(), Data: []byte("x"), Key: []byte("k"),
		}); err != nil {
			t.Fatalf("add %d: %v", i, err)
		}
	}
	evicted := lb.GetLastEvictedTsNs()
	if evicted == 0 {
		t.Fatal("precondition: the ring evicted nothing")
	}

	crossings := func() float64 {
		var m dto.Metric
		if err := stats.FilerSubscribeUnprovenGapCrossings.WithLabelValues("aggregated").Write(&m); err != nil {
			t.Fatalf("read counter: %v", err)
		}
		return m.GetCounter().GetValue()
	}
	var g0 dto.Metric
	if err := stats.FilerSubscribeGapStalledGauge.WithLabelValues("aggregated").Write(&g0); err != nil {
		t.Fatalf("read gauge: %v", err)
	}
	gaugeBefore := g0.GetGauge().GetValue()

	t.Run("a stalled park below the watermark skips to it", func(t *testing.T) {
		gapStall := &gapStallReporter{scope: "aggregated", clientName: "c", pathPrefix: "/"}
		cursor := log_buffer.NewMessagePosition(evicted-int64(time.Minute), -2)
		// Park through the real path so the gauge Inc that gaveUp() will Dec
		// exists, then age the park to the give-up bound.
		gapStall.park(cursor.Time, "test")
		gapStall.since = time.Now().Add(-maxGapStall)

		before := crossings()
		skipTo, skip, done := fs.parkOnGap(context.Background(), req, gapStall, lb.GetLastEvictedTsNs, cursor, nil, "test")
		if done || !skip {
			t.Fatalf("skip=%v done=%v, want a forced skip", skip, done)
		}
		if skipTo != evicted {
			t.Fatalf("skipTo = %v, want the eviction watermark %v", time.Unix(0, skipTo), time.Unix(0, evicted))
		}
		if got := crossings(); got != before+1 {
			t.Fatalf("crossing counter moved by %v, want 1: the loss must be recorded", got-before)
		}
		if !gapStall.since.IsZero() {
			t.Fatal("the stall must be cleared after giving up")
		}
	})

	t.Run("a stalled park with nothing to skip to keeps waiting", func(t *testing.T) {
		gapStall := &gapStallReporter{scope: "aggregated", clientName: "c", pathPrefix: "/"}
		cursor := log_buffer.NewMessagePosition(evicted, -2) // at the watermark: nothing withheld
		gapStall.park(cursor.Time, "test")
		gapStall.since = time.Now().Add(-maxGapStall)
		defer gapStall.close() // release the gauge this test's park holds

		before := crossings()
		_, skip, done := fs.parkOnGap(context.Background(), req, gapStall, lb.GetLastEvictedTsNs, cursor, nil, "test")
		if skip || done {
			t.Fatalf("skip=%v done=%v, want neither: nothing is being lost", skip, done)
		}
		if got := crossings(); got != before {
			t.Fatal("no loss happened, the counter must not move")
		}
		if gapStall.stalledFor() >= maxGapStall {
			t.Fatal("the stall clock must restart, or this branch retriggers every retry")
		}
	})

	// The shared gauge must come back to its starting value: a test leaving it
	// skewed corrupts every later assertion on it in this package.
	var g dto.Metric
	if err := stats.FilerSubscribeGapStalledGauge.WithLabelValues("aggregated").Write(&g); err != nil {
		t.Fatalf("read gauge: %v", err)
	}
	if got := g.GetGauge().GetValue(); got != gaugeBefore {
		t.Fatalf("stalled gauge = %v, want %v: parks and releases must balance", got, gaugeBefore)
	}
}

// TestDeltaLogFileRefs pins the per-stream ref dedup. Collections overlap by
// design - the scan backs off a flush interval to catch a spanning file, and a
// filer appends chunks to its newest file - so without the delta a subscriber
// receives the same file twice: re-downloaded chunks at best, and a mid-stream
// timestamp rewind inside the client's sorted per-filer merge at worst.
func TestDeltaLogFileRefs(t *testing.T) {
	chunk := func(id string, offset, size int64) *filer_pb.FileChunk {
		return &filer_pb.FileChunk{FileId: id, Offset: offset, Size: uint64(size)}
	}
	ref := func(filerId string, fileTsNs int64, chunks ...*filer_pb.FileChunk) *filer_pb.LogFileChunkRef {
		return &filer_pb.LogFileChunkRef{FilerId: filerId, FileTsNs: fileTsNs, Chunks: chunks}
	}
	sent := make(map[string]sentRefState)

	// First collection ships everything.
	out := deltaLogFileRefs([]*filer_pb.LogFileChunkRef{ref("a", 100, chunk("c1", 0, 10), chunk("c2", 10, 10))}, sent, 0)
	if len(out) != 1 || len(out[0].Chunks) != 2 {
		t.Fatalf("first collection: got %d refs, want the whole file", len(out))
	}

	// Re-collection of the identical file ships nothing.
	out = deltaLogFileRefs([]*filer_pb.LogFileChunkRef{ref("a", 100, chunk("c1", 0, 10), chunk("c2", 10, 10))}, sent, 0)
	if len(out) != 0 {
		t.Fatalf("unchanged re-collection: got %d refs, want none", len(out))
	}

	// A grown file ships only its new chunks; a new file ships whole.
	out = deltaLogFileRefs([]*filer_pb.LogFileChunkRef{
		ref("a", 100, chunk("c1", 0, 10), chunk("c2", 10, 10), chunk("c3", 20, 10)),
		ref("a", 200, chunk("d1", 0, 10)),
	}, sent, 0)
	if len(out) != 2 {
		t.Fatalf("growth pass: got %d refs, want 2", len(out))
	}
	if len(out[0].Chunks) != 1 || out[0].Chunks[0].FileId != "c3" {
		t.Fatalf("grown file must ship only the appended suffix, got %+v", out[0].Chunks)
	}
	// The suffix must read from logical zero: the client's chunk reader starts
	// there, and a list opening at a higher offset is an instant EOF - a
	// silently empty replay of the appended events.
	if out[0].Chunks[0].Offset != 0 {
		t.Fatalf("suffix chunk keeps file offset %d; it must be rebased to 0", out[0].Chunks[0].Offset)
	}
	if len(out[1].Chunks) != 1 || out[1].Chunks[0].FileId != "d1" {
		t.Fatalf("new file must ship whole, got %+v", out[1].Chunks)
	}

	// Files behind the scan window are pruned; one that somehow reappears ships
	// again rather than leaking state forever.
	deltaLogFileRefs(nil, sent, 150)
	if _, kept := sent["a/100"]; kept {
		t.Fatal("file behind the scan window must be pruned")
	}
	if _, kept := sent["a/200"]; !kept {
		t.Fatal("file inside the scan window must be kept")
	}
}

// TestRefNeedsReship pins the sent-state rollback rules. Sent state that
// outlives what the probe could not verify strands the cursor behind shipped
// content for the life of the connection; sent state dropped for files the
// client has moved past only re-ships noise it will filter.
func TestRefNeedsReship(t *testing.T) {
	const answered = 2000
	cases := []struct {
		name     string
		fileTsNs int64
		ok       bool
		complete bool
		want     bool
	}{
		{"file above the answer re-ships", 3000, true, true, true},
		{"complete answering file stays sent", answered, true, true, false},
		{"prefix-limited answering file re-ships its suffix", answered, true, false, true},
		{"file below a complete answer stays sent", 1000, true, true, false},
		{"file below an incomplete answer stays sent (client moved past)", 1000, true, false, false},
		{"everything re-ships when nothing answered", 1000, false, false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := refNeedsReship(tc.fileTsNs, tc.ok, answered, tc.complete); got != tc.want {
				t.Fatalf("refNeedsReship(%d, %v, %d, %v) = %v, want %v",
					tc.fileTsNs, tc.ok, answered, tc.complete, got, tc.want)
			}
		})
	}
}
