package dailyrun

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newSnapshotWith(t *testing.T, inputs []engine.CompileInput) *engine.Snapshot {
	t.Helper()
	e := engine.New()
	e.Compile(inputs, engine.CompileOptions{})
	snap := e.Snapshot()
	// Mark every action active so isActive checks fire as expected; the
	// production compile path also marks them active for replay-eligible
	// kinds when prior.BootstrapComplete is true. Tests just need them
	// visible to AllActions().
	for _, a := range snap.AllActions() {
		snap.MarkActive(a.Key)
	}
	return snap
}

func ruleExpirationDays(days int) *s3lifecycle.Rule {
	return &s3lifecycle.Rule{ID: "r", Status: s3lifecycle.StatusEnabled, ExpirationDays: days}
}

func ruleExpirationDate(t time.Time) *s3lifecycle.Rule {
	return &s3lifecycle.Rule{ID: "r-date", Status: s3lifecycle.StatusEnabled, ExpirationDate: t}
}

func ruleNoncurrentDays(days int) *s3lifecycle.Rule {
	return &s3lifecycle.Rule{ID: "r-nc", Status: s3lifecycle.StatusEnabled, NoncurrentVersionExpirationDays: days}
}

func ruleAbortMPU(days int) *s3lifecycle.Rule {
	return &s3lifecycle.Rule{ID: "r-mpu", Status: s3lifecycle.StatusEnabled, AbortMPUDaysAfterInitiation: days}
}

func ruleNewerNoncurrent(n int) *s3lifecycle.Rule {
	return &s3lifecycle.Rule{ID: "r-newer", Status: s3lifecycle.StatusEnabled, NewerNoncurrentVersions: n}
}

func ruleExpiredDeleteMarker() *s3lifecycle.Rule {
	return &s3lifecycle.Rule{ID: "r-edm", Status: s3lifecycle.StatusEnabled, ExpiredObjectDeleteMarker: true}
}

func TestCheckSnapshotForUnsupported_AllReplayKindsAccepted(t *testing.T) {
	snap := newSnapshotWith(t, []engine.CompileInput{
		{Bucket: "b1", Rules: []*s3lifecycle.Rule{
			ruleExpirationDays(30),
			ruleNoncurrentDays(7),
			ruleAbortMPU(7),
		}},
	})
	require.Nil(t, checkSnapshotForUnsupported(snap))
}

func TestCheckSnapshotForUnsupported_ExpirationDateRejected(t *testing.T) {
	snap := newSnapshotWith(t, []engine.CompileInput{
		{Bucket: "b1", Rules: []*s3lifecycle.Rule{ruleExpirationDate(time.Now().Add(48 * time.Hour))}},
	})
	err := checkSnapshotForUnsupported(snap)
	require.NotNil(t, err)
	assert.Equal(t, s3lifecycle.ActionKindExpirationDate, err.Kind)
	assert.Equal(t, "b1", err.Bucket)
}

func TestCheckSnapshotForUnsupported_NewerNoncurrentRejected(t *testing.T) {
	snap := newSnapshotWith(t, []engine.CompileInput{
		{Bucket: "b1", Rules: []*s3lifecycle.Rule{ruleNewerNoncurrent(2)}},
	})
	err := checkSnapshotForUnsupported(snap)
	require.NotNil(t, err)
	assert.Equal(t, s3lifecycle.ActionKindNewerNoncurrent, err.Kind)
}

func TestCheckSnapshotForUnsupported_ExpiredDeleteMarkerRejected(t *testing.T) {
	snap := newSnapshotWith(t, []engine.CompileInput{
		{Bucket: "b1", Rules: []*s3lifecycle.Rule{ruleExpiredDeleteMarker()}},
	})
	err := checkSnapshotForUnsupported(snap)
	require.NotNil(t, err)
	assert.Equal(t, s3lifecycle.ActionKindExpiredDeleteMarker, err.Kind)
}

func TestCheckSnapshotForUnsupported_NonEventDrivenModeRejected(t *testing.T) {
	// A replay-eligible action whose Mode isn't ModeEventDriven (e.g.
	// promoted to ModeScanOnly by retention checks) is silently
	// ignored by router.Route. Phase 2 must catch this loudly. Build a
	// snapshot with one ExpirationDays action and mutate its Mode to
	// ModeScanOnly directly — there's no production path that produces
	// this without retention plumbing we don't have here, but the gate
	// must still reject it.
	snap := newSnapshotWith(t, []engine.CompileInput{
		{Bucket: "b1", Rules: []*s3lifecycle.Rule{ruleExpirationDays(30)}},
	})
	// Mutate the compiled action to ModeScanOnly. AllActions returns
	// the live snapshot's actions; this is safe in a test where no
	// concurrent worker is reading.
	for _, a := range snap.AllActions() {
		if a.Key.ActionKind == s3lifecycle.ActionKindExpirationDays {
			a.Mode = engine.ModeScanOnly
		}
	}
	err := checkSnapshotForUnsupported(snap)
	require.NotNil(t, err, "ModeScanOnly on a replay-kind action must be rejected")
	assert.Equal(t, s3lifecycle.ActionKindExpirationDays, err.Kind)
	assert.Contains(t, err.Reason, "ModeEventDriven")
}

func TestIsUnsupportedRule_TypeCheck(t *testing.T) {
	var u error = &UnsupportedRuleError{Bucket: "b", Kind: s3lifecycle.ActionKindExpirationDate, Reason: "x"}
	assert.True(t, IsUnsupportedRule(u))
	assert.False(t, IsUnsupportedRule(nil))
	assert.False(t, IsUnsupportedRule(assertNonNilError()))
}

// assertNonNilError returns a plain non-nil error of a different type
// so IsUnsupportedRule's errors.As check has a negative case to refuse.
func assertNonNilError() error { return errPlain }

type plainErr struct{}

func (plainErr) Error() string { return "plain" }

var errPlain = plainErr{}

func TestLocalReplayContentHash_StableAcrossReorderings(t *testing.T) {
	// Compile the same rules in two snapshots and verify the hash is
	// identical — sort order in AllActions() is implementation detail
	// and must not affect the cursor's content hash.
	rules := []*s3lifecycle.Rule{ruleExpirationDays(30), ruleNoncurrentDays(7), ruleAbortMPU(7)}
	snapA := newSnapshotWith(t, []engine.CompileInput{{Bucket: "b1", Rules: rules}})
	// Build snapB with the same rules in reverse order; hash must match.
	rev := []*s3lifecycle.Rule{rules[2], rules[1], rules[0]}
	snapB := newSnapshotWith(t, []engine.CompileInput{{Bucket: "b1", Rules: rev}})
	assert.Equal(t, localReplayContentHash(snapA), localReplayContentHash(snapB))
}

func TestLocalReplayContentHash_ChangesOnTTLEdit(t *testing.T) {
	snap30 := newSnapshotWith(t, []engine.CompileInput{
		{Bucket: "b1", Rules: []*s3lifecycle.Rule{ruleExpirationDays(30)}},
	})
	snap60 := newSnapshotWith(t, []engine.CompileInput{
		{Bucket: "b1", Rules: []*s3lifecycle.Rule{ruleExpirationDays(60)}},
	})
	assert.NotEqual(t, localReplayContentHash(snap30), localReplayContentHash(snap60))
}

func TestLocalReplayContentHash_EmptyIsZero(t *testing.T) {
	snap := newSnapshotWith(t, nil)
	var zero [32]byte
	assert.Equal(t, zero, localReplayContentHash(snap))
}

func TestLocalMaxEffectiveTTL_PicksLargest(t *testing.T) {
	snap := newSnapshotWith(t, []engine.CompileInput{
		{Bucket: "b1", Rules: []*s3lifecycle.Rule{
			ruleExpirationDays(7),
			ruleNoncurrentDays(30),
			ruleAbortMPU(14),
		}},
	})
	got := localMaxEffectiveTTL(snap)
	assert.Equal(t, s3lifecycle.DaysToDuration(30), got)
}

func TestLocalMaxEffectiveTTL_EmptyReturnsZero(t *testing.T) {
	snap := newSnapshotWith(t, nil)
	assert.Equal(t, time.Duration(0), localMaxEffectiveTTL(snap))
}
