package dailyrun

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/s3_lifecycle_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/reader"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/router"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recordingClient captures every LifecycleDelete request the
// daily-run path emits. The default outcome is DONE; tests that need
// other outcomes set responses by index.
type recordingClient struct {
	mu        sync.Mutex
	requests  []*s3_lifecycle_pb.LifecycleDeleteRequest
	responses []s3_lifecycle_pb.LifecycleDeleteOutcome
	calls     atomic.Int32
}

func (c *recordingClient) LifecycleDelete(_ context.Context, req *s3_lifecycle_pb.LifecycleDeleteRequest) (*s3_lifecycle_pb.LifecycleDeleteResponse, error) {
	c.mu.Lock()
	c.requests = append(c.requests, req)
	idx := int(c.calls.Add(1)) - 1
	c.mu.Unlock()
	out := s3_lifecycle_pb.LifecycleDeleteOutcome_DONE
	if idx < len(c.responses) {
		out = c.responses[idx]
	}
	return &s3_lifecycle_pb.LifecycleDeleteResponse{Outcome: out}, nil
}

func (c *recordingClient) seenObjects() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, 0, len(c.requests))
	for _, r := range c.requests {
		out = append(out, r.ObjectPath)
	}
	return out
}

// Phase 2's processMatches must NOT use a per-rule done flag.
// routePointerTransitionExpand can return multiple matches for the
// same ActionKey on different objects, each with its own
// SuccessorModTime from a distinct demoting event. A not-yet-due
// sibling must never gate a sibling that's already past its DueTime.
// Pin that here.
func TestProcessMatches_NotYetDueDoesNotSuppressDueOnSameRule(t *testing.T) {
	now := time.Now()
	clock := func() time.Time { return now }

	rh := [8]byte{1, 2, 3, 4, 5, 6, 7, 8}
	rule := s3lifecycle.ActionKey{
		Bucket:     "b",
		RuleHash:   rh,
		ActionKind: s3lifecycle.ActionKindNoncurrentDays,
	}
	// Three matches for the same ActionKey, different objects, mixed
	// due-times. Order intentionally puts the not-yet-due match first
	// so a buggy per-rule done flag would suppress the others.
	matches := []router.Match{
		{Key: rule, Bucket: "b", ObjectKey: "future-sibling", DueTime: now.Add(48 * time.Hour)},
		{Key: rule, Bucket: "b", ObjectKey: "due-sibling-a", DueTime: now.Add(-48 * time.Hour)},
		{Key: rule, Bucket: "b", ObjectKey: "due-sibling-b", DueTime: now.Add(-24 * time.Hour)},
	}
	client := &recordingClient{}
	cfg := Config{Client: client}
	halted, err := processMatches(context.Background(), cfg, clock, &reader.Event{TsNs: now.UnixNano()}, matches)
	require.NoError(t, err)
	require.False(t, halted)

	// Both due siblings must have been dispatched. The future one is
	// skipped — but it does NOT gate the others.
	seen := client.seenObjects()
	assert.ElementsMatch(t, []string{"due-sibling-a", "due-sibling-b"}, seen,
		"due matches must dispatch regardless of where the future-sibling sits in the slice")
}

func TestProcessMatches_OrderingDoesNotMatter(t *testing.T) {
	// Reverse the previous test's order: put future LAST. Result must
	// be identical — neither position should affect dispatch.
	now := time.Now()
	clock := func() time.Time { return now }
	rh := [8]byte{0xaa}
	rule := s3lifecycle.ActionKey{Bucket: "b", RuleHash: rh, ActionKind: s3lifecycle.ActionKindExpirationDays}
	matches := []router.Match{
		{Key: rule, Bucket: "b", ObjectKey: "a", DueTime: now.Add(-2 * time.Hour)},
		{Key: rule, Bucket: "b", ObjectKey: "b", DueTime: now.Add(-1 * time.Hour)},
		{Key: rule, Bucket: "b", ObjectKey: "future", DueTime: now.Add(time.Hour)},
	}
	client := &recordingClient{}
	cfg := Config{Client: client}
	halted, err := processMatches(context.Background(), cfg, clock, &reader.Event{}, matches)
	require.NoError(t, err)
	require.False(t, halted)
	assert.ElementsMatch(t, []string{"a", "b"}, client.seenObjects())
}

func TestProcessMatches_HaltOnServerOutcomeStopsRemaining(t *testing.T) {
	// A halt-on-failure outcome on match[1] must stop dispatch on
	// match[2]. The cursor stays where the caller left it.
	now := time.Now()
	clock := func() time.Time { return now }
	rh := [8]byte{0xbb}
	rule := s3lifecycle.ActionKey{Bucket: "b", RuleHash: rh, ActionKind: s3lifecycle.ActionKindExpirationDays}
	matches := []router.Match{
		{Key: rule, Bucket: "b", ObjectKey: "ok", DueTime: now.Add(-time.Hour)},
		{Key: rule, Bucket: "b", ObjectKey: "blocked", DueTime: now.Add(-time.Hour)},
		{Key: rule, Bucket: "b", ObjectKey: "would-be-next", DueTime: now.Add(-time.Hour)},
	}
	client := &recordingClient{responses: []s3_lifecycle_pb.LifecycleDeleteOutcome{
		s3_lifecycle_pb.LifecycleDeleteOutcome_DONE,
		s3_lifecycle_pb.LifecycleDeleteOutcome_BLOCKED,
		s3_lifecycle_pb.LifecycleDeleteOutcome_DONE, // would be a bug if reached
	}}
	cfg := Config{Client: client}
	halted, err := processMatches(context.Background(), cfg, clock, &reader.Event{}, matches)
	require.NoError(t, err)
	require.True(t, halted, "BLOCKED outcome must halt the event loop")
	assert.Equal(t, []string{"ok", "blocked"}, client.seenObjects(),
		"would-be-next must NOT be dispatched after the halt")
}

func TestProcessMatches_EmptyMatchesIsNoop(t *testing.T) {
	client := &recordingClient{}
	cfg := Config{Client: client}
	halted, err := processMatches(context.Background(), cfg, time.Now, &reader.Event{}, nil)
	require.NoError(t, err)
	require.False(t, halted)
	assert.Empty(t, client.seenObjects())
}
