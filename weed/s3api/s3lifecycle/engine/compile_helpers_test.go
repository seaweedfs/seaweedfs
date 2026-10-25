package engine

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
	"github.com/stretchr/testify/assert"
)

// rulePredicateSensitive's non-nil branches are exercised by the
// existing Compile tests; this test pins the defensive nil-rule
// branch directly. Compile shouldn't ever pass a nil rule, but a
// future caller that does shouldn't panic.

func TestRulePredicateSensitive_NilRuleReturnsFalse(t *testing.T) {
	assert.False(t, rulePredicateSensitive(nil))
}

func TestRulePredicateSensitive_NoFilterTagsReturnsFalse(t *testing.T) {
	// A rule without FilterTags is not predicate-sensitive; the
	// router's MatchPredicateChange path skips actions whose rule
	// returns false here.
	rule := &s3lifecycle.Rule{ID: "r", Status: s3lifecycle.StatusEnabled, ExpirationDays: 7}
	assert.False(t, rulePredicateSensitive(rule))
}

func TestRulePredicateSensitive_EmptyFilterTagsReturnsFalse(t *testing.T) {
	// An empty (non-nil) FilterTags map must classify as non-sensitive
	// — the function checks len, so empty == 0 == false.
	rule := &s3lifecycle.Rule{
		ID:         "r",
		Status:     s3lifecycle.StatusEnabled,
		FilterTags: map[string]string{},
	}
	assert.False(t, rulePredicateSensitive(rule))
}

func TestRulePredicateSensitive_PopulatedFilterTagsReturnsTrue(t *testing.T) {
	rule := &s3lifecycle.Rule{
		ID:         "r",
		Status:     s3lifecycle.StatusEnabled,
		FilterTags: map[string]string{"env": "prod"},
	}
	assert.True(t, rulePredicateSensitive(rule))
}
