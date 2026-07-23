package dailyrun

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/seaweedfs/seaweedfs/weed/pb/s3_lifecycle_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/reader"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/router"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recordingClient captures every LifecycleDelete request the
// daily-run path emits. Default outcome is DONE; tests that need
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
	runNow := time.Now()
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
		{Key: rule, Bucket: "b", ObjectKey: "future-sibling", DueTime: runNow.Add(48 * time.Hour)},
		{Key: rule, Bucket: "b", ObjectKey: "due-sibling-a", DueTime: runNow.Add(-48 * time.Hour)},
		{Key: rule, Bucket: "b", ObjectKey: "due-sibling-b", DueTime: runNow.Add(-24 * time.Hour)},
	}
	client := &recordingClient{}
	cfg := Config{Client: client}
	skipped, halted, err := processMatches(context.Background(), cfg, runNow, &reader.Event{TsNs: runNow.UnixNano()}, matches)
	require.NoError(t, err)
	require.False(t, halted)
	require.True(t, skipped, "at least one future-DueTime match must flag skippedAny")

	// Both due siblings must have been dispatched.
	seen := client.seenObjects()
	assert.ElementsMatch(t, []string{"due-sibling-a", "due-sibling-b"}, seen,
		"due matches must dispatch regardless of where the future-sibling sits in the slice")
}

func TestProcessMatches_OrderingDoesNotMatter(t *testing.T) {
	runNow := time.Now()
	rh := [8]byte{0xaa}
	rule := s3lifecycle.ActionKey{Bucket: "b", RuleHash: rh, ActionKind: s3lifecycle.ActionKindExpirationDays}
	matches := []router.Match{
		{Key: rule, Bucket: "b", ObjectKey: "a", DueTime: runNow.Add(-2 * time.Hour)},
		{Key: rule, Bucket: "b", ObjectKey: "b", DueTime: runNow.Add(-1 * time.Hour)},
		{Key: rule, Bucket: "b", ObjectKey: "future", DueTime: runNow.Add(time.Hour)},
	}
	client := &recordingClient{}
	cfg := Config{Client: client}
	skipped, halted, err := processMatches(context.Background(), cfg, runNow, &reader.Event{}, matches)
	require.NoError(t, err)
	require.False(t, halted)
	require.True(t, skipped)
	assert.ElementsMatch(t, []string{"a", "b"}, client.seenObjects())
}

func TestProcessMatches_HaltOnServerOutcomeStopsRemaining(t *testing.T) {
	runNow := time.Now()
	rh := [8]byte{0xbb}
	rule := s3lifecycle.ActionKey{Bucket: "b", RuleHash: rh, ActionKind: s3lifecycle.ActionKindExpirationDays}
	matches := []router.Match{
		{Key: rule, Bucket: "b", ObjectKey: "ok", DueTime: runNow.Add(-time.Hour)},
		{Key: rule, Bucket: "b", ObjectKey: "blocked", DueTime: runNow.Add(-time.Hour)},
		{Key: rule, Bucket: "b", ObjectKey: "would-be-next", DueTime: runNow.Add(-time.Hour)},
	}
	client := &recordingClient{responses: []s3_lifecycle_pb.LifecycleDeleteOutcome{
		s3_lifecycle_pb.LifecycleDeleteOutcome_DONE,
		s3_lifecycle_pb.LifecycleDeleteOutcome_BLOCKED,
		s3_lifecycle_pb.LifecycleDeleteOutcome_DONE, // would be a bug if reached
	}}
	cfg := Config{Client: client}
	_, halted, err := processMatches(context.Background(), cfg, runNow, &reader.Event{}, matches)
	require.NoError(t, err)
	require.True(t, halted, "BLOCKED outcome must halt the event loop")
	assert.Equal(t, []string{"ok", "blocked"}, client.seenObjects(),
		"would-be-next must NOT be dispatched after the halt")
}

func TestProcessMatches_EmptyMatchesIsNoop(t *testing.T) {
	runNow := time.Now()
	client := &recordingClient{}
	cfg := Config{Client: client}
	skipped, halted, err := processMatches(context.Background(), cfg, runNow, &reader.Event{}, nil)
	require.NoError(t, err)
	require.False(t, halted)
	require.False(t, skipped)
	assert.Empty(t, client.seenObjects())
}

func TestProcessMatches_AllDueNoSkippedFlag(t *testing.T) {
	// All matches past their DueTime — skippedAny must be false so the
	// caller can advance the cursor past this event.
	runNow := time.Now()
	rh := [8]byte{0xcc}
	rule := s3lifecycle.ActionKey{Bucket: "b", RuleHash: rh, ActionKind: s3lifecycle.ActionKindExpirationDays}
	matches := []router.Match{
		{Key: rule, Bucket: "b", ObjectKey: "a", DueTime: runNow.Add(-time.Hour)},
		{Key: rule, Bucket: "b", ObjectKey: "b", DueTime: runNow.Add(-30 * time.Minute)},
	}
	client := &recordingClient{}
	cfg := Config{Client: client}
	skipped, halted, err := processMatches(context.Background(), cfg, runNow, &reader.Event{}, matches)
	require.NoError(t, err)
	require.False(t, halted)
	require.False(t, skipped, "every match was past DueTime; skippedAny must be false")
}

func TestProcessMatches_DispatchCounterIncrements(t *testing.T) {
	// Pin that dispatched matches increment S3LifecycleDispatchCounter
	// with the outcome label so a refactor doesn't silently drop the
	// observability hook. Use a bucket/kind no other test touches to
	// keep the read-after-write stable.
	runNow := time.Now()
	rh := [8]byte{0xfe}
	rule := s3lifecycle.ActionKey{Bucket: "metrics-pin-bkt", RuleHash: rh, ActionKind: s3lifecycle.ActionKindExpirationDays}
	matches := []router.Match{
		{Key: rule, Bucket: "metrics-pin-bkt", ObjectKey: "obj", DueTime: runNow.Add(-time.Hour)},
	}
	client := &recordingClient{} // default DONE
	before := dispatchCounterValue("metrics-pin-bkt", "expiration_days", "DONE")
	// Delete the label row on exit so this test doesn't leak into the
	// in-process Prometheus registry that other tests share.
	defer stats.S3LifecycleDispatchCounter.DeleteLabelValues("metrics-pin-bkt", "expiration_days", "DONE")
	cfg := Config{Client: client}
	_, _, err := processMatches(context.Background(), cfg, runNow, &reader.Event{}, matches)
	require.NoError(t, err)
	after := dispatchCounterValue("metrics-pin-bkt", "expiration_days", "DONE")
	assert.Equal(t, before+1, after, "DONE outcome must increment the dispatch counter")
}

// dispatchCounterValue reads the current value of the shared
// S3LifecycleDispatchCounter for the given (bucket, kind, outcome).
func dispatchCounterValue(bucket, kind, outcome string) float64 {
	m := stats.S3LifecycleDispatchCounter.WithLabelValues(bucket, kind, outcome)
	var pm dto.Metric
	if err := m.Write(&pm); err != nil {
		return 0
	}
	if pm.Counter == nil {
		return 0
	}
	return pm.Counter.GetValue()
}
