package blockvol

import (
	"testing"
	"time"
)

func TestHealthScore_DefaultPerfect(t *testing.T) {
	h := NewHealthScore()
	if got := h.Score(); got != 1.0 {
		t.Fatalf("default score: got %f, want 1.0", got)
	}
}

func TestHealthScore_ScrubErrorsLowerScore(t *testing.T) {
	h := NewHealthScore()
	h.RecordScrubError()
	if got := h.Score(); got != 0.9 {
		t.Fatalf("1 error: got %f, want 0.9", got)
	}
	h.RecordScrubError()
	if got := h.Score(); got != 0.8 {
		t.Fatalf("2 errors: got %f, want 0.8", got)
	}
}

func TestHealthScore_CappedPenalty(t *testing.T) {
	h := NewHealthScore()
	for i := 0; i < 10; i++ {
		h.RecordScrubError()
	}
	// 10 errors × 0.1 = 1.0, but capped at 0.5 penalty → score 0.5
	if got := h.Score(); got != 0.5 {
		t.Fatalf("10 errors: got %f, want 0.5", got)
	}
}

func TestHealthScore_StaleScrubPenalty(t *testing.T) {
	h := NewHealthScore()
	// Set last scrub to 49 hours ago.
	h.lastScrubTime.Store(time.Now().Add(-49 * time.Hour).Unix())
	got := h.Score()
	if got < 0.89 || got > 0.91 {
		t.Fatalf("stale scrub: got %f, want ~0.9", got)
	}
}

func TestHealthScore_RecordScrubComplete(t *testing.T) {
	h := NewHealthScore()
	h.RecordScrubComplete()
	stats := h.Stats()
	if stats.LastScrubTime == 0 {
		t.Fatal("LastScrubTime should be set after RecordScrubComplete")
	}
	if stats.Score != 1.0 {
		t.Fatalf("score after complete: got %f, want 1.0", stats.Score)
	}
}
