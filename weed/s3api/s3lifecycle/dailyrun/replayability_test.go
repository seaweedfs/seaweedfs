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
	// router.Route silently drops non-ModeEventDriven actions; gate
	// must reject loudly.
	snap := newSnapshotWith(t, []engine.CompileInput{
		{Bucket: "b1", Rules: []*s3lifecycle.Rule{ruleExpirationDays(30)}},
	})
	for _, a := range snap.AllActions() {
		if a.Key.ActionKind == s3lifecycle.ActionKindExpirationDays {
			a.Mode = engine.ModeScanOnly
		}
	}
	err := checkSnapshotForUnsupported(snap)
	require.NotNil(t, err)
	assert.Equal(t, s3lifecycle.ActionKindExpirationDays, err.Kind)
	assert.Contains(t, err.Reason, "ModeEventDriven")
}

func TestIsUnsupportedRule_TypeCheck(t *testing.T) {
	var u error = &UnsupportedRuleError{Bucket: "b", Kind: s3lifecycle.ActionKindExpirationDate, Reason: "x"}
	assert.True(t, IsUnsupportedRule(u))
	assert.False(t, IsUnsupportedRule(nil))
	assert.False(t, IsUnsupportedRule(assertNonNilError()))
}

func assertNonNilError() error { return errPlain }

type plainErr struct{}

func (plainErr) Error() string { return "plain" }

var errPlain = plainErr{}
