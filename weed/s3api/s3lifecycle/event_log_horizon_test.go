package s3lifecycle

import (
	"testing"
	"time"
)

func TestEventLogHorizon_PerActionIsIndependent(t *testing.T) {
	// A multi-action rule's horizon is computed per kind: the gate runs on
	// each compiled action separately, so EXPIRATION_DAYS at 90d does NOT
	// share a horizon with its sibling ABORT_MPU at 7d.
	rule := &Rule{ExpirationDays: 90, AbortMPUDaysAfterInitiation: 7, NoncurrentVersionExpirationDays: 30}
	cases := []struct {
		kind ActionKind
		want time.Duration
	}{
		{ActionKindExpirationDays, 90 * 24 * time.Hour},
		{ActionKindAbortMPU, 7 * 24 * time.Hour},
		{ActionKindNoncurrentDays, 30 * 24 * time.Hour},
		{ActionKindExpirationDate, 0},
	}
	for _, c := range cases {
		t.Run(c.kind.String(), func(t *testing.T) {
			if got := EventLogHorizon(rule, c.kind); got != c.want {
				t.Fatalf("want %v, got %v", c.want, got)
			}
		})
	}
}

func TestEventLogHorizon_NewerNoncurrentCountOnlyIsSmallDelay(t *testing.T) {
	rule := &Rule{NewerNoncurrentVersions: 5}
	if got := EventLogHorizon(rule, ActionKindNewerNoncurrent); got != SmallDelay {
		t.Fatalf("want SmallDelay, got %v", got)
	}
}

func TestEventLogHorizon_NewerNoncurrentWithDaysIsZero(t *testing.T) {
	// Pure NEWER_NONCURRENT only when NoncurrentDays is unset; otherwise the
	// rule produces a NONCURRENT_DAYS action and asking for NEWER_NONCURRENT
	// returns 0 (not declared by this rule).
	rule := &Rule{NoncurrentVersionExpirationDays: 30, NewerNoncurrentVersions: 5}
	if got := EventLogHorizon(rule, ActionKindNewerNoncurrent); got != 0 {
		t.Fatalf("NEWER_NONCURRENT not declared when paired with days, got %v", got)
	}
	// The actual action this rule declares is NONCURRENT_DAYS — verify horizon there.
	if got := EventLogHorizon(rule, ActionKindNoncurrentDays); got != 30*24*time.Hour {
		t.Fatalf("NoncurrentDays horizon, want 30d, got %v", got)
	}
}

func TestEventLogHorizon_ExpiredDeleteMarkerIsSmallDelay(t *testing.T) {
	rule := &Rule{ExpiredObjectDeleteMarker: true}
	if got := EventLogHorizon(rule, ActionKindExpiredDeleteMarker); got != SmallDelay {
		t.Fatalf("want SmallDelay, got %v", got)
	}
}

func TestEventLogHorizon_DateKindReturnsZero(t *testing.T) {
	rule := &Rule{ExpirationDate: mustTime(t, "2025-06-15T00:00:00Z")}
	if got := EventLogHorizon(rule, ActionKindExpirationDate); got != 0 {
		t.Fatalf("date kind bypasses gate, got %v", got)
	}
}

func TestEventLogHorizon_NilRule(t *testing.T) {
	if got := EventLogHorizon(nil, ActionKindExpirationDays); got != 0 {
		t.Fatalf("nil rule should return 0, got %v", got)
	}
}

func TestEventLogHorizon_KindNotDeclaredReturnsZero(t *testing.T) {
	// Asking for a kind the rule doesn't declare returns 0; the gate then
	// trivially passes for that compiled action (which doesn't exist).
	rule := &Rule{ExpirationDays: 30}
	if got := EventLogHorizon(rule, ActionKindAbortMPU); got != 0 {
		t.Fatalf("want 0 for undeclared kind, got %v", got)
	}
}
