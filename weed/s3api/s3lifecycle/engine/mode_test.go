package engine

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
	"github.com/stretchr/testify/assert"
)

// decideMode is the per-action mode classifier the scheduler uses when
// no durable Mode exists yet. Compile tests cover most paths
// indirectly; these direct tests pin every branch so a regression in
// decideMode itself can't slip through behind a more elaborate Compile
// test failure.

func TestDecideMode_NilRuleIsDisabled(t *testing.T) {
	got := decideMode(nil, s3lifecycle.ActionKindExpirationDays, 0, 0)
	assert.Equal(t, ModeDisabled, got)
}

func TestDecideMode_DisabledStatusIsDisabled(t *testing.T) {
	rule := &s3lifecycle.Rule{Status: s3lifecycle.StatusDisabled, ExpirationDays: 30}
	got := decideMode(rule, s3lifecycle.ActionKindExpirationDays, 0, 0)
	assert.Equal(t, ModeDisabled, got)
}

func TestDecideMode_ExpirationDateIsScanAtDate(t *testing.T) {
	// ExpirationDate is a one-shot wall-clock trigger; no event log
	// involvement, so retention gates don't apply.
	rule := &s3lifecycle.Rule{
		Status:         s3lifecycle.StatusEnabled,
		ExpirationDate: time.Now().Add(24 * time.Hour),
	}
	got := decideMode(rule, s3lifecycle.ActionKindExpirationDate, 0, 0)
	assert.Equal(t, ModeScanAtDate, got)
}

func TestDecideMode_UnboundedRetentionIsEventDriven(t *testing.T) {
	// metaLogRetention=0 means unbounded; the retention gate doesn't
	// trip, so any horizon falls back to event-driven.
	rule := &s3lifecycle.Rule{
		Status:         s3lifecycle.StatusEnabled,
		ExpirationDays: 365,
	}
	got := decideMode(rule, s3lifecycle.ActionKindExpirationDays, 0, time.Hour)
	assert.Equal(t, ModeEventDriven, got)
}

func TestDecideMode_HorizonWithinRetentionIsEventDriven(t *testing.T) {
	// retention=30d, horizon=7d, lookback=1m → 7d+1m < 30d → no gate.
	rule := &s3lifecycle.Rule{
		Status:         s3lifecycle.StatusEnabled,
		ExpirationDays: 7,
	}
	got := decideMode(rule, s3lifecycle.ActionKindExpirationDays, 30*24*time.Hour, time.Minute)
	assert.Equal(t, ModeEventDriven, got)
}

func TestDecideMode_HorizonExceedsRetentionIsScanOnly(t *testing.T) {
	// retention=7d, horizon=30d → can't drive from event log alone, so
	// the action is promoted to scan_only and only fires from the
	// bootstrap walk.
	rule := &s3lifecycle.Rule{
		Status:         s3lifecycle.StatusEnabled,
		ExpirationDays: 30,
	}
	got := decideMode(rule, s3lifecycle.ActionKindExpirationDays, 7*24*time.Hour, time.Minute)
	assert.Equal(t, ModeScanOnly, got)
}

func TestDecideMode_LookbackPushesAcrossThreshold(t *testing.T) {
	// retention=7d, horizon=7d - the bare horizon fits, but adding the
	// bootstrapLookbackMin must push it past retention so the gate
	// trips. Pins that lookback is added to horizon (not retention).
	rule := &s3lifecycle.Rule{
		Status:         s3lifecycle.StatusEnabled,
		ExpirationDays: 7,
	}
	retention := 7 * 24 * time.Hour
	// horizon already ~= retention; even a tiny lookback should trip.
	got := decideMode(rule, s3lifecycle.ActionKindExpirationDays, retention, 5*time.Minute)
	assert.Equal(t, ModeScanOnly, got)
}

func TestDecideMode_ZeroHorizonNeverGates(t *testing.T) {
	// EventLogHorizon returns 0 for kinds whose rule fields aren't set
	// (e.g. an EXPIRATION_DAYS kind on a rule with ExpirationDays=0).
	// A zero horizon must skip the gate so the action falls through to
	// event-driven rather than incorrectly promote to scan_only.
	rule := &s3lifecycle.Rule{
		Status: s3lifecycle.StatusEnabled,
		// ExpirationDays unset -> EventLogHorizon returns 0
	}
	got := decideMode(rule, s3lifecycle.ActionKindExpirationDays, time.Hour, time.Minute)
	assert.Equal(t, ModeEventDriven, got)
}

func TestRuleMode_StringRendersDocumentedNames(t *testing.T) {
	// Operators read these via metrics labels and admin UI; pin the
	// exact strings so a rename can't break dashboards.
	cases := []struct {
		mode RuleMode
		want string
	}{
		{ModeUnspecified, "unspecified"},
		{ModeEventDriven, "event_driven"},
		{ModeScanAtDate, "scan_at_date"},
		{ModeScanOnly, "scan_only"},
		{ModeDisabled, "disabled"},
		{ModePendingBootstrap, "pending_bootstrap"},
	}
	for _, c := range cases {
		t.Run(c.want, func(t *testing.T) {
			assert.Equal(t, c.want, c.mode.String())
		})
	}
}

func TestRuleMode_StringFallsToUnspecifiedForUnknown(t *testing.T) {
	// A bogus enum value (e.g., a future RuleMode added without a
	// String() update) must render as "unspecified" rather than empty
	// or panic.
	bogus := RuleMode(999)
	assert.Equal(t, "unspecified", bogus.String())
}
