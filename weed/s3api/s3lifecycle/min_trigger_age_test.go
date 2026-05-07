package s3lifecycle

import (
	"testing"
	"time"
)

func TestMinTriggerAge_PerKind(t *testing.T) {
	rule := &Rule{
		ExpirationDays:                  30,
		NoncurrentVersionExpirationDays: 7,
		AbortMPUDaysAfterInitiation:     14,
	}
	cases := []struct {
		kind ActionKind
		want time.Duration
	}{
		{ActionKindExpirationDays, 30 * 24 * time.Hour},
		{ActionKindNoncurrentDays, 7 * 24 * time.Hour},
		{ActionKindAbortMPU, 14 * 24 * time.Hour},
		{ActionKindExpirationDate, 0},
		{ActionKindNewerNoncurrent, 0},
		{ActionKindExpiredDeleteMarker, 0},
	}
	for _, c := range cases {
		t.Run(c.kind.String(), func(t *testing.T) {
			if got := MinTriggerAge(rule, c.kind); got != c.want {
				t.Fatalf("want %v, got %v", c.want, got)
			}
		})
	}
}

func TestMinTriggerAge_KindNotSetReturnsZero(t *testing.T) {
	// Asking for a kind that the rule doesn't actually declare returns 0,
	// so the safety-scan cadence falls through to the kind floor.
	rule := &Rule{ExpirationDays: 30}
	if got := MinTriggerAge(rule, ActionKindAbortMPU); got != 0 {
		t.Fatalf("want 0, got %v", got)
	}
}

func TestMinTriggerAge_DateOnlyReturnsZero(t *testing.T) {
	rule := &Rule{ExpirationDate: mustTime(t, "2025-06-15T00:00:00Z")}
	if got := MinTriggerAge(rule, ActionKindExpirationDate); got != 0 {
		t.Fatalf("date kind has no day threshold, got %v", got)
	}
}

func TestMinTriggerAge_NilRule(t *testing.T) {
	if got := MinTriggerAge(nil, ActionKindExpirationDays); got != 0 {
		t.Fatalf("nil rule should return 0, got %v", got)
	}
}
