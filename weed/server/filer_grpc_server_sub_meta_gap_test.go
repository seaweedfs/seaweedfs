package weed_server

import (
	"testing"
	"time"
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
			// The evicted window ends exactly on the cursor, which still wants
			// that entry: it is gone from memory and not proven to be on disk.
			name:            "inclusive cursor on the watermark must NOT skip",
			currentTsNs:     ago(30 * time.Second),
			earliestMemTsNs: ago(25 * time.Second),
			lastEvictedTsNs: ago(30 * time.Second),
			wantAdvance:     false,
		},
		{
			// Same timestamp, but the cursor already excludes it.
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
		wantAdvance     bool
	}{
		{
			name:            "watermark behind earliest must NOT skip (may be unflushed)",
			currentTsNs:     ago(40 * time.Second),
			earliestMemTsNs: ago(30 * time.Second),
			flushedTsNs:     ago(35 * time.Second),
			wantAdvance:     false,
		},
		{
			name:            "no flush ever (watermark 0) must NOT skip",
			currentTsNs:     ago(40 * time.Second),
			earliestMemTsNs: ago(30 * time.Second),
			flushedTsNs:     0,
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
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotTo, gotAdvance := resolveLocalGapResume(tc.currentTsNs, tc.currentOffset, tc.earliestMemTsNs, tc.flushedTsNs)
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
