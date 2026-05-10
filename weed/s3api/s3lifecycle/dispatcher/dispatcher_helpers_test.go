package dispatcher

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/router"
	"github.com/stretchr/testify/assert"
)

// Direct coverage for dispatcher pure helpers (keyOf, budget, backoff)
// and the retryKey identity that drives the retry-budget map. The
// existing dispatcher tests exercise these only through Tick; pinning
// each one separately makes a regression in the helper itself fail at
// the helper level.

func TestKeyOf_DerivesIdentityFromMatch(t *testing.T) {
	hash := [8]byte{0xde, 0xad, 0xbe, 0xef, 1, 2, 3, 4}
	m := router.Match{
		Key: s3lifecycle.ActionKey{
			Bucket:     "bk",
			RuleHash:   hash,
			ActionKind: s3lifecycle.ActionKindExpirationDays,
		},
		ObjectKey: "obj.txt",
		VersionID: "v_abc",
	}
	got := keyOf(m)
	assert.Equal(t, "bk", got.bucket)
	assert.Equal(t, hash, got.ruleHash)
	assert.Equal(t, s3lifecycle.ActionKindExpirationDays, got.kind)
	assert.Equal(t, "obj.txt", got.objectKey)
	assert.Equal(t, "v_abc", got.versionID)
}

func TestKeyOf_EqualMatchesProduceEqualKeys(t *testing.T) {
	// retryKey is used as a map key in the retry budget; equality must
	// hold between two Match values with identical fields so the second
	// dispatch finds the first's retry counter.
	hash := [8]byte{0xde, 0xad, 0xbe, 0xef}
	m1 := router.Match{
		Key:       s3lifecycle.ActionKey{Bucket: "bk", RuleHash: hash, ActionKind: s3lifecycle.ActionKindExpirationDays},
		ObjectKey: "obj",
		VersionID: "v_x",
	}
	m2 := m1
	assert.Equal(t, keyOf(m1), keyOf(m2))
}

func TestKeyOf_DistinctVersionIDsProduceDistinctKeys(t *testing.T) {
	// Two versions of the same logical object must NOT share a retry
	// budget; otherwise a noisy version could starve a healthy one.
	base := router.Match{
		Key:       s3lifecycle.ActionKey{Bucket: "bk", ActionKind: s3lifecycle.ActionKindNoncurrentDays},
		ObjectKey: "obj",
		VersionID: "v_a",
	}
	other := base
	other.VersionID = "v_b"
	assert.NotEqual(t, keyOf(base), keyOf(other))
}

func TestKeyOf_DistinctActionKindsProduceDistinctKeys(t *testing.T) {
	// The same (bucket, object, version) hit by two different action
	// kinds must each have their own retry budget.
	base := router.Match{
		Key:       s3lifecycle.ActionKey{Bucket: "bk", ActionKind: s3lifecycle.ActionKindExpirationDays},
		ObjectKey: "obj",
	}
	other := base
	other.Key.ActionKind = s3lifecycle.ActionKindNoncurrentDays
	assert.NotEqual(t, keyOf(base), keyOf(other))
}

func TestDispatcherBudget_ReturnsConfiguredValueWhenSet(t *testing.T) {
	d := &Dispatcher{RetryBudget: 9}
	assert.Equal(t, 9, d.budget())
}

func TestDispatcherBudget_FallsBackToDefaultWhenZero(t *testing.T) {
	// Operators leaving the budget at zero opt into the documented
	// default. A regression that returns 0 would NOOP every retry.
	d := &Dispatcher{}
	assert.Equal(t, defaultRetryBudget, d.budget())
}

func TestDispatcherBudget_NegativeFallsBackToDefault(t *testing.T) {
	// budget() guards on > 0, so a negative value falls back rather
	// than producing nonsense. Pin the contract so a refactor that
	// flips the comparison is caught.
	d := &Dispatcher{RetryBudget: -1}
	assert.Equal(t, defaultRetryBudget, d.budget())
}

func TestDispatcherBackoff_ReturnsConfiguredValueWhenSet(t *testing.T) {
	d := &Dispatcher{RetryBackoff: 5 * time.Second}
	assert.Equal(t, 5*time.Second, d.backoff())
}

func TestDispatcherBackoff_FallsBackToDefaultWhenZero(t *testing.T) {
	d := &Dispatcher{}
	assert.Equal(t, defaultRetryBackoff, d.backoff())
}

func TestDispatcherBackoff_NegativeFallsBackToDefault(t *testing.T) {
	d := &Dispatcher{RetryBackoff: -time.Second}
	assert.Equal(t, defaultRetryBackoff, d.backoff())
}
