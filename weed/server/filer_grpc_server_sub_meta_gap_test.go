package weed_server

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// TestResolveAggregatedGapResume pins the gap-skip fix on the aggregated
// stream: that ring never flushes, so a disk miss proves nothing. Only the
// eviction watermark can prove the gap empty — anything the ring dropped may
// still be sitting unflushed on the peer that produced it.
func TestResolveAggregatedGapResume(t *testing.T) {
	// A fixed "now" so the cases are deterministic.
	now := time.Date(2026, 6, 29, 12, 0, 0, 0, time.UTC).UnixNano()
	ago := func(d time.Duration) int64 { return now - int64(d) }

	cases := []struct {
		name            string
		currentTsNs     int64
		earliestMemTsNs int64
		lastEvictedTsNs int64
		wantAdvance     bool
	}{
		{
			// The bug: the ring dropped the 30s..25s window before any peer
			// flushed it, so skipping past it loses those events for good.
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
			// Nothing was ever evicted, so memory holds the whole history and
			// the gap is provably empty however old the cursor is.
			name:            "nothing evicted skips however old the cursor",
			currentTsNs:     time.Unix(0, 0).UnixNano(),
			earliestMemTsNs: ago(30 * time.Second),
			lastEvictedTsNs: 0,
			wantAdvance:     true,
		},
		{
			// The evicted window ends exactly on the cursor. Memory holds
			// nothing at that timestamp and the persisted reader skips
			// ts <= its start, so no wait can produce it and the rest of the
			// gap is in memory: skipping is the only way forward.
			name:            "inclusive cursor on the watermark skips",
			currentTsNs:     ago(30 * time.Second),
			earliestMemTsNs: ago(25 * time.Second),
			lastEvictedTsNs: ago(30 * time.Second),
			wantAdvance:     true,
		},
		{
			name:            "exclusive cursor on the watermark skips",
			currentTsNs:     ago(30 * time.Second),
			earliestMemTsNs: ago(25 * time.Second),
			lastEvictedTsNs: ago(30 * time.Second),
			wantAdvance:     true,
		},
		{
			name:            "memory not ahead of current must NOT skip",
			currentTsNs:     ago(20 * time.Second),
			earliestMemTsNs: ago(40 * time.Second),
			wantAdvance:     false,
		},
		{
			// time.Time{}.UnixNano() is a large negative value: no in-memory data.
			name:            "no in-memory data must NOT skip",
			currentTsNs:     ago(30 * time.Second),
			earliestMemTsNs: time.Time{}.UnixNano(),
			wantAdvance:     false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotTo, gotAdvance := resolveAggregatedGapResume(tc.currentTsNs, tc.earliestMemTsNs, tc.lastEvictedTsNs)
			if gotAdvance != tc.wantAdvance {
				t.Fatalf("advance = %v, want %v (current=%v earliest=%v evicted=%v)",
					gotAdvance, tc.wantAdvance, time.Unix(0, tc.currentTsNs),
					time.Unix(0, tc.earliestMemTsNs), time.Unix(0, tc.lastEvictedTsNs))
			}
			if !gotAdvance {
				return
			}
			if gotTo != tc.earliestMemTsNs {
				t.Fatalf("advanceTo = %v, want earliest %v", time.Unix(0, gotTo), time.Unix(0, tc.earliestMemTsNs))
			}
			if gotTo <= tc.currentTsNs {
				t.Fatalf("advanceTo %v must be strictly ahead of current %v",
					time.Unix(0, gotTo), time.Unix(0, tc.currentTsNs))
			}
		})
	}
}

// TestResolveLocalGapResume pins the watermark gate used by local subscriptions:
// a disk miss proves a gap empty only if the flush watermark observed before the
// read had already passed the earliest in-memory time.
func TestResolveLocalGapResume(t *testing.T) {
	now := time.Date(2026, 6, 29, 12, 0, 0, 0, time.UTC).UnixNano()
	ago := func(d time.Duration) int64 { return now - int64(d) }

	cases := []struct {
		name            string
		currentTsNs     int64
		earliestMemTsNs int64
		flushedTsNs     int64
		lastEvictedTsNs int64 // zero value is a ring that never evicted
		wantAdvance     bool
	}{
		{
			name:            "watermark behind earliest must NOT skip (may be unflushed)",
			currentTsNs:     ago(40 * time.Second),
			earliestMemTsNs: ago(30 * time.Second),
			flushedTsNs:     ago(35 * time.Second),
			lastEvictedTsNs: ago(32 * time.Second), // the ring dropped the gap
			wantAdvance:     false,
		},
		{
			name:            "no flush ever (watermark 0) must NOT skip",
			currentTsNs:     ago(40 * time.Second),
			earliestMemTsNs: ago(30 * time.Second),
			flushedTsNs:     0,
			lastEvictedTsNs: ago(32 * time.Second), // the ring dropped the gap
			wantAdvance:     false,
		},
		{
			// Everything up to earliest was flushed before the read: the miss is
			// authoritative, jump to earliest (the reader includes it from memory).
			name:            "watermark at earliest skips to earliest",
			currentTsNs:     ago(40 * time.Second),
			earliestMemTsNs: ago(30 * time.Second),
			flushedTsNs:     ago(30 * time.Second),
			wantAdvance:     true,
		},
		{
			name:            "watermark past earliest skips to earliest",
			currentTsNs:     ago(10 * time.Minute),
			earliestMemTsNs: ago(30 * time.Second),
			flushedTsNs:     ago(10 * time.Second),
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
			name:            "no in-memory data must NOT skip",
			currentTsNs:     ago(30 * time.Second),
			earliestMemTsNs: time.Time{}.UnixNano(),
			flushedTsNs:     ago(10 * time.Second),
			wantAdvance:     false,
		},
		{
			// Nothing was ever evicted, so memory still holds the gap and the
			// flush watermark does not have to prove anything.
			name:            "nothing evicted skips despite a stale flush watermark",
			currentTsNs:     ago(40 * time.Second),
			earliestMemTsNs: ago(30 * time.Second),
			flushedTsNs:     0,
			wantAdvance:     true,
		},
		{
			// The gap starts right after the evicted window, so memory holds all
			// of it even though the flush is stalled well behind.
			name:            "cursor on the eviction watermark skips with a stalled flush",
			currentTsNs:     ago(30 * time.Second),
			earliestMemTsNs: ago(25 * time.Second),
			flushedTsNs:     0,
			lastEvictedTsNs: ago(30 * time.Second),
			wantAdvance:     true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotTo, gotAdvance := resolveLocalGapResume(tc.currentTsNs, tc.earliestMemTsNs, tc.flushedTsNs, tc.lastEvictedTsNs)
			if gotAdvance != tc.wantAdvance {
				t.Fatalf("advance = %v, want %v", gotAdvance, tc.wantAdvance)
			}
			// Positions are exclusive: the jump lands just below earliest so the
			// earliest entry itself is still delivered.
			if gotAdvance && gotTo != tc.earliestMemTsNs {
				t.Fatalf("advanceTo = %v, want earliest %v", time.Unix(0, gotTo), time.Unix(0, tc.earliestMemTsNs))
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
// cannot fix that, so the resolvers must skip instead of parking.
func TestInclusiveDiskCursorOnWatermarkStillAdvances(t *testing.T) {
	now := time.Date(2026, 6, 29, 12, 0, 0, 0, time.UTC).UnixNano()
	watermark := now - int64(30*time.Second) // disk delivered exactly this far
	earliest := watermark + int64(time.Second)

	if !memoryHoldsGap(watermark, watermark) {
		t.Fatal("memory holds everything after the watermark, whatever the cursor's inclusivity")
	}
	to, advance := resolveAggregatedGapResume(watermark, earliest, watermark)
	if !advance {
		t.Fatal("aggregated: an inclusive cursor on the watermark must still advance")
	}
	if to != earliest {
		t.Fatalf("aggregated: advanceTo = %v, want earliest %v", time.Unix(0, to), time.Unix(0, earliest))
	}
	// No flush has landed, so only the eviction proof can settle this one.
	if _, advance := resolveLocalGapResume(watermark, earliest, 0, watermark); !advance {
		t.Fatal("local: an inclusive cursor on the watermark must still advance")
	}

	// One nanosecond earlier the gap really was dropped unflushed: park.
	dropped := watermark - 1
	if _, advance := resolveAggregatedGapResume(dropped, earliest, watermark); advance {
		t.Fatal("aggregated: a cursor below the watermark must wait, not skip")
	}
	if _, advance := resolveLocalGapResume(dropped, earliest, 0, watermark); advance {
		t.Fatal("local: a cursor below the watermark with no flush must wait, not skip")
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
