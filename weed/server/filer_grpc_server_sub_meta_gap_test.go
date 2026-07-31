package weed_server

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/util/log_buffer"
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
		currentOffset   int64 // zero value is an inclusive (sentinel) cursor
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
			currentOffset:   gapResumeOffset,
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
		{
			// The resume target collapses onto the cursor. An inclusive cursor
			// still advances, because the exclusive resume drops its own
			// timestamp; parking here is what stranded the subscriber.
			name:            "adjacent inclusive cursor advances to an exclusive one",
			currentTsNs:     ago(30*time.Second) - 1,
			earliestMemTsNs: ago(30 * time.Second),
			lastEvictedTsNs: ago(31 * time.Second),
			wantAdvance:     true,
		},
		{
			// Already exclusive at that timestamp: re-arming it is not progress.
			name:            "adjacent exclusive cursor must NOT skip",
			currentTsNs:     ago(30*time.Second) - 1,
			currentOffset:   gapResumeOffset,
			earliestMemTsNs: ago(30 * time.Second),
			lastEvictedTsNs: ago(31 * time.Second),
			wantAdvance:     false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotTo, gotAdvance := resolveAggregatedGapResume(tc.currentTsNs, tc.currentOffset, tc.earliestMemTsNs, tc.lastEvictedTsNs)
			if gotAdvance != tc.wantAdvance {
				t.Fatalf("advance = %v, want %v (current=%v earliest=%v evicted=%v)",
					gotAdvance, tc.wantAdvance, time.Unix(0, tc.currentTsNs),
					time.Unix(0, tc.earliestMemTsNs), time.Unix(0, tc.lastEvictedTsNs))
			}
			if !gotAdvance {
				return
			}
			if gotTo != tc.earliestMemTsNs-1 {
				t.Fatalf("advanceTo = %v, want just below earliest %v", time.Unix(0, gotTo), time.Unix(0, tc.earliestMemTsNs))
			}
			if !gapResumeAdvances(gotTo, tc.currentTsNs, tc.currentOffset) {
				t.Fatalf("advanceTo %v (exclusive) must be ahead of current %v offset %d",
					time.Unix(0, gotTo), time.Unix(0, tc.currentTsNs), tc.currentOffset)
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
		currentOffset   int64 // zero value is an inclusive (sentinel) cursor
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
			// The cursor already sits just below earliest. Advancing it to an
			// exclusive cursor at the same timestamp is the only way out: an
			// inclusive one keeps asking for a timestamp the eviction gate
			// answers from disk, and the disk has nothing.
			name:            "adjacent inclusive cursor advances to an exclusive one",
			currentTsNs:     ago(30*time.Second) - 1,
			earliestMemTsNs: ago(30 * time.Second),
			flushedTsNs:     ago(10 * time.Second),
			wantAdvance:     true,
		},
		{
			// Already exclusive at that timestamp: re-arming it is not progress.
			name:            "adjacent exclusive cursor must NOT skip",
			currentTsNs:     ago(30*time.Second) - 1,
			currentOffset:   gapResumeOffset,
			earliestMemTsNs: ago(30 * time.Second),
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
			gotTo, gotAdvance := resolveLocalGapResume(tc.currentTsNs, tc.currentOffset, tc.earliestMemTsNs, tc.flushedTsNs, tc.lastEvictedTsNs)
			if gotAdvance != tc.wantAdvance {
				t.Fatalf("advance = %v, want %v", gotAdvance, tc.wantAdvance)
			}
			// Positions are exclusive: the jump lands just below earliest so the
			// earliest entry itself is still delivered.
			if gotAdvance && gotTo != tc.earliestMemTsNs-1 {
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
// cannot fix that, so the resolvers must skip instead of parking.
func TestInclusiveDiskCursorOnWatermarkStillAdvances(t *testing.T) {
	now := time.Date(2026, 6, 29, 12, 0, 0, 0, time.UTC).UnixNano()
	watermark := now - int64(30*time.Second) // disk delivered exactly this far
	earliest := watermark + int64(time.Second)
	inclusive := int64(-2)

	if !memoryHoldsGap(watermark, watermark) {
		t.Fatal("memory holds everything after the watermark, whatever the cursor's inclusivity")
	}
	to, advance := resolveAggregatedGapResume(watermark, inclusive, earliest, watermark)
	if !advance {
		t.Fatal("aggregated: an inclusive cursor on the watermark must still advance")
	}
	if to != earliest-1 {
		t.Fatalf("aggregated: advanceTo = %v, want just below earliest %v", time.Unix(0, to), time.Unix(0, earliest))
	}
	// No flush has landed, so only the eviction proof can settle this one.
	if _, advance := resolveLocalGapResume(watermark, inclusive, earliest, 0, watermark); !advance {
		t.Fatal("local: an inclusive cursor on the watermark must still advance")
	}

	// One nanosecond earlier the gap really was dropped unflushed: park.
	dropped := watermark - 1
	if _, advance := resolveAggregatedGapResume(dropped, inclusive, earliest, watermark); advance {
		t.Fatal("aggregated: a cursor below the watermark must wait, not skip")
	}
	if _, advance := resolveLocalGapResume(dropped, inclusive, earliest, 0, watermark); advance {
		t.Fatal("local: a cursor below the watermark with no flush must wait, not skip")
	}
}

// TestWaitForLocalFlushIgnoresAppends pins the parked-subscriber cadence. The
// subscriber channel carries an append per metadata write as well as the flush,
// and only the flush can settle a gap, so appends must not release the wait:
// during the write burst that causes these stalls they arrive continuously, and
// returning on one runs the caller's recovery loop at write rate.
func TestWaitForLocalFlushIgnoresAppends(t *testing.T) {
	lb := log_buffer.NewLogBuffer("wait-for-flush", time.Minute, nil, nil, nil)
	defer lb.ShutdownLogBuffer()
	lb.SetLastFlushTsNs(100)

	notify := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// A steady stream of appends, none of which moves the flush watermark.
	appendsDone := make(chan struct{})
	stopAppends := make(chan struct{})
	go func() {
		defer close(appendsDone)
		for {
			select {
			case <-stopAppends:
				return
			case notify <- struct{}{}:
			}
		}
	}()

	start := time.Now()
	if !waitForLocalFlush(ctx, lb, notify, 100) {
		t.Fatal("wait reported a closed stream")
	}
	elapsed := time.Since(start)
	close(stopAppends)
	<-appendsDone
	if elapsed < unflushedGapRetryInterval {
		t.Fatalf("appends released the wait after %v, want the full %v retry interval", elapsed, unflushedGapRetryInterval)
	}

	// A flush releases it immediately.
	drain(notify)
	lb.SetLastFlushTsNs(200)
	notify <- struct{}{}
	start = time.Now()
	if !waitForLocalFlush(ctx, lb, notify, 100) {
		t.Fatal("wait reported a closed stream")
	}
	if elapsed := time.Since(start); elapsed >= unflushedGapRetryInterval {
		t.Fatalf("flush wake-up took %v, want well under the %v retry interval", elapsed, unflushedGapRetryInterval)
	}

	// A cancelled stream unblocks it and reports the stream is gone.
	drain(notify)
	cancel()
	if waitForLocalFlush(ctx, lb, notify, 200) {
		t.Fatal("wait must report false once the stream is gone")
	}
}

func drain(ch chan struct{}) {
	select {
	case <-ch:
	default:
	}
}
