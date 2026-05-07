package s3lifecycle

import (
	"testing"
	"time"
)

func TestEventLogHorizon_AgeKinds(t *testing.T) {
	cases := []struct {
		name string
		rule *Rule
		want time.Duration
	}{
		{"ExpirationDays=30", &Rule{ExpirationDays: 30}, 30 * 24 * time.Hour},
		{"NoncurrentDays=14", &Rule{NoncurrentVersionExpirationDays: 14}, 14 * 24 * time.Hour},
		{"AbortMPU=7", &Rule{AbortMPUDaysAfterInitiation: 7}, 7 * 24 * time.Hour},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := EventLogHorizon(c.rule); got != c.want {
				t.Fatalf("want %v, got %v", c.want, got)
			}
		})
	}
}

func TestEventLogHorizon_TakesMaxAcrossActions(t *testing.T) {
	rule := &Rule{
		ExpirationDays:                  7,
		NoncurrentVersionExpirationDays: 30, // bigger wins
		AbortMPUDaysAfterInitiation:     14,
	}
	if got := EventLogHorizon(rule); got != 30*24*time.Hour {
		t.Fatalf("want 30d, got %v", got)
	}
}

func TestEventLogHorizon_NewerNoncurrentCountOnlyIsSmallDelay(t *testing.T) {
	rule := &Rule{NewerNoncurrentVersions: 5}
	if got := EventLogHorizon(rule); got != SmallDelay {
		t.Fatalf("want SmallDelay, got %v", got)
	}
}

func TestEventLogHorizon_NewerNoncurrentWithDaysUsesDays(t *testing.T) {
	// When a count-based filter is paired with NoncurrentDays, the days
	// horizon dominates — SmallDelay is only used for pure count rules.
	rule := &Rule{NoncurrentVersionExpirationDays: 30, NewerNoncurrentVersions: 5}
	if got := EventLogHorizon(rule); got != 30*24*time.Hour {
		t.Fatalf("want 30d, got %v", got)
	}
}

func TestEventLogHorizon_ExpiredDeleteMarkerIsSmallDelay(t *testing.T) {
	rule := &Rule{ExpiredObjectDeleteMarker: true}
	if got := EventLogHorizon(rule); got != SmallDelay {
		t.Fatalf("want SmallDelay, got %v", got)
	}
}

func TestEventLogHorizon_DateOnlyReturnsZero(t *testing.T) {
	// Date rules bypass the retention gate entirely; horizon undefined.
	rule := &Rule{ExpirationDate: mustTime(t, "2025-06-15T00:00:00Z")}
	if got := EventLogHorizon(rule); got != 0 {
		t.Fatalf("date-only should return 0, got %v", got)
	}
}

func TestEventLogHorizon_NilRule(t *testing.T) {
	if got := EventLogHorizon(nil); got != 0 {
		t.Fatalf("nil should return 0, got %v", got)
	}
}
