package app

import (
	"testing"
	"time"
)

func TestFormatLastHeartbeat(t *testing.T) {
	if got := formatLastHeartbeat(time.Time{}); got != "unknown" {
		t.Fatalf("zero time = %q, want unknown", got)
	}
	beat := time.Date(2026, 1, 2, 3, 4, 5, 0, time.Local)
	if got := formatLastHeartbeat(beat); got != "2026-01-02 03:04:05" {
		t.Fatalf("formatLastHeartbeat = %q", got)
	}
}
