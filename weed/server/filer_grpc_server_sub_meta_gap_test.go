package weed_server

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
)

// TestResolveDiskGapResume pins the gap-skip fix: a subscriber that fell behind
// the in-memory ring must not skip past a window that may still hold unflushed
// events; only a window older than the settled horizon may be skipped.
func TestResolveDiskGapResume(t *testing.T) {
	// A fixed "now" so the cases are deterministic.
	now := time.Date(2026, 6, 29, 12, 0, 0, 0, time.UTC).UnixNano()
	horizon := metadataGapSettledHorizon // 2 * LogFlushInterval
	ago := func(d time.Duration) int64 { return now - int64(d) }

	cases := []struct {
		name            string
		currentTsNs     int64
		earliestMemTsNs int64
		wantAdvance     bool
		wantAdvanceToNs int64 // only checked when wantAdvance
	}{
		{
			// The bug: the 30s..25s window may hold unflushed events → must wait.
			name:            "recent unflushed gap must NOT skip",
			currentTsNs:     ago(30 * time.Second),
			earliestMemTsNs: ago(25 * time.Second),
			wantAdvance:     false,
		},
		{
			// Ancient start: skip to avoid an infinite loop, but never past the horizon.
			name:            "ancient empty gap skips, capped below settled horizon",
			currentTsNs:     time.Unix(0, 0).UnixNano(),
			earliestMemTsNs: ago(30 * time.Second),
			wantAdvance:     true,
			wantAdvanceToNs: ago(horizon) - 1,
		},
		{
			// Advance only to the settled horizon, not to earliest.
			name:            "settled empty gap skips to horizon not earliest",
			currentTsNs:     ago(10 * time.Minute),
			earliestMemTsNs: ago(30 * time.Second),
			wantAdvance:     true,
			wantAdvanceToNs: ago(horizon) - 1,
		},
		{
			// Persisted reads exclude ts <= cursor, so an event exactly on the
			// boundary must stay ahead of the cursor.
			name:            "earliest exactly at horizon boundary is capped below it",
			currentTsNs:     ago(10 * time.Minute),
			earliestMemTsNs: ago(horizon),
			wantAdvance:     true,
			wantAdvanceToNs: ago(horizon) - 1,
		},
		{
			// The whole gap is older than the horizon → safe to reach earliest
			// (just below it: positions are exclusive).
			name:            "earliest older than horizon skips to just below earliest",
			currentTsNs:     ago(10 * time.Minute),
			earliestMemTsNs: ago(5 * time.Minute),
			wantAdvance:     true,
			wantAdvanceToNs: ago(5*time.Minute) - 1,
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
			gotTo, gotAdvance := resolveDiskGapResume(tc.currentTsNs, tc.earliestMemTsNs, now, horizon)
			if gotAdvance != tc.wantAdvance {
				t.Fatalf("advance = %v, want %v (current=%v earliest=%v)",
					gotAdvance, tc.wantAdvance, time.Unix(0, tc.currentTsNs), time.Unix(0, tc.earliestMemTsNs))
			}
			if tc.wantAdvance {
				if gotTo != tc.wantAdvanceToNs {
					t.Fatalf("advanceTo = %v, want %v", time.Unix(0, gotTo), time.Unix(0, tc.wantAdvanceToNs))
				}
				if gotTo <= tc.currentTsNs {
					t.Fatalf("advanceTo %v must be strictly after current %v", time.Unix(0, gotTo), time.Unix(0, tc.currentTsNs))
				}
				// Never advance to or past the unsettled boundary (persisted
				// reads exclude ts <= cursor).
				if gotTo >= now-int64(horizon) {
					t.Fatalf("advanceTo %v must stay strictly below settled horizon %v", time.Unix(0, gotTo), time.Unix(0, now-int64(horizon)))
				}
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
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotTo, gotAdvance := resolveLocalGapResume(tc.currentTsNs, tc.earliestMemTsNs, tc.flushedTsNs)
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

// oldGapSkipWouldSkipPast models the pre-fix behaviour: always advance to the
// earliest in-memory time; an event at eventTsNs behind the jump is dropped.
func oldGapSkipWouldSkipPast(currentTsNs, earliestMemTsNs, eventTsNs int64) bool {
	if earliestMemTsNs > 0 && earliestMemTsNs > currentTsNs {
		newPos := earliestMemTsNs
		return newPos > eventTsNs // event is now behind the read position → never read
	}
	return false
}

// newGapSkipWouldSkipPast models the post-fix behaviour via resolveDiskGapResume.
func newGapSkipWouldSkipPast(currentTsNs, earliestMemTsNs, eventTsNs, nowTsNs int64, horizon time.Duration) bool {
	advanceTo, advance := resolveDiskGapResume(currentTsNs, earliestMemTsNs, nowTsNs, horizon)
	if !advance {
		return false // no skip → event preserved, re-read after flush
	}
	return advanceTo > eventTsNs
}

// TestGapSkipDropsUnflushedEvent is a deterministic loss proof: the pre-fix
// gap-skip drops an evicted-but-unflushed event, the post-fix logic preserves it,
// and a genuinely old settled gap is still skipped (no infinite-loop regression).
// The loss is fully determined by this position-advance decision, so the repro is
// expressed at the decision level.
func TestGapSkipDropsUnflushedEvent(t *testing.T) {
	now := time.Date(2026, 6, 29, 12, 0, 0, 0, time.UTC).UnixNano()
	horizon := metadataGapSettledHorizon
	ago := func(d time.Duration) int64 { return now - int64(d) }

	t.Run("unflushed event in a recent gap", func(t *testing.T) {
		// Subscriber stalled at now-40s; an event at now-35s is not yet flushed;
		// earliest in-memory has advanced to now-30s.
		current := ago(40 * time.Second)
		event := ago(35 * time.Second)
		earliest := ago(30 * time.Second)

		if !oldGapSkipWouldSkipPast(current, earliest, event) {
			t.Fatal("expected the PRE-FIX gap-skip to drop the unflushed event (repro precondition)")
		}
		if newGapSkipWouldSkipPast(current, earliest, event, now, horizon) {
			t.Fatal("POST-FIX must NOT drop an unflushed event in a recent gap")
		}
	})

	t.Run("settled empty gap still skipped (no infinite-loop regression)", func(t *testing.T) {
		// Ancient start, disk genuinely empty: both skip so the reader makes progress.
		current := time.Unix(0, 0).UnixNano()
		event := ago(10 * time.Minute) // already flushed long ago; disk authoritative
		earliest := ago(20 * time.Second)

		if !oldGapSkipWouldSkipPast(current, earliest, event) {
			t.Fatal("sanity: pre-fix skips the ancient settled gap")
		}
		if !newGapSkipWouldSkipPast(current, earliest, event, now, horizon) {
			t.Fatal("POST-FIX must still skip a genuinely-old settled gap (infinite-loop guard)")
		}
	})
}

// The horizon must exceed the flush interval, otherwise a window one flush cycle
// old could be treated as settled while its events are still only in memory.
func TestMetadataGapSettledHorizonExceedsFlushInterval(t *testing.T) {
	if metadataGapSettledHorizon <= filer.LogFlushInterval {
		t.Fatalf("metadataGapSettledHorizon (%v) must exceed LogFlushInterval (%v) so a disk miss is authoritative",
			metadataGapSettledHorizon, filer.LogFlushInterval)
	}
}
