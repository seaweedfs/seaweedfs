package s3api

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestVersionsHealQueue_DedupOnEnqueue ensures multiple enqueues of the
// same bucket/object collapse into a single pending entry, so a hot
// failure path doesn't bloat the queue.
func TestVersionsHealQueue_DedupOnEnqueue(t *testing.T) {
	q := newVersionsHealQueue()
	for i := 0; i < 5; i++ {
		q.Enqueue("b", "obj")
	}
	assert.Equal(t, 1, q.Len(), "duplicate enqueues collapse")
}

// TestVersionsHealQueue_CapacityCap ensures the queue refuses growth
// past the static cap and logs at V(1) instead of OOM-ing.
func TestVersionsHealQueue_CapacityCap(t *testing.T) {
	q := newVersionsHealQueue()
	for i := 0; i < versionsHealQueueCapacity+50; i++ {
		q.Enqueue("b", string(rune(i)))
	}
	assert.Equal(t, versionsHealQueueCapacity, q.Len(), "queue clamps at capacity")
}

// TestVersionsHealQueue_PopReadyOnlyDueItems checks that nextRetry
// gating keeps not-yet-ready candidates in the queue.
func TestVersionsHealQueue_PopReadyOnlyDueItems(t *testing.T) {
	q := newVersionsHealQueue()
	q.Enqueue("b", "due")

	// Inject a deferred candidate directly so we control its nextRetry.
	q.pending[versionsHealKey("b", "later")] = &versionsHealCandidate{
		bucket:    "b",
		object:    "later",
		enqueued:  time.Now(),
		nextRetry: time.Now().Add(10 * time.Minute),
	}

	due := q.popReady(time.Now())
	require.Len(t, due, 1, "only the due candidate pops")
	assert.Equal(t, "due", due[0].object)
	assert.Equal(t, 1, q.Len(), "deferred candidate still queued")
}

// TestVersionsHealQueue_RequeueWithBackoff verifies failed candidates
// re-enter the queue with an extended nextRetry.
func TestVersionsHealQueue_RequeueWithBackoff(t *testing.T) {
	q := newVersionsHealQueue()
	c := &versionsHealCandidate{bucket: "b", object: "obj", attempts: 1}

	q.requeue(c, 500*time.Millisecond)
	assert.Equal(t, 1, q.Len())

	now := time.Now()
	due := q.popReady(now)
	assert.Empty(t, due, "not yet due immediately after requeue")

	due = q.popReady(now.Add(time.Second))
	assert.Len(t, due, 1, "due after backoff window passes")
}

// TestVersionsHealQueue_GiveUpAfterMaxAttempts ensures we don't loop
// forever against a deterministically broken state.
func TestVersionsHealQueue_GiveUpAfterMaxAttempts(t *testing.T) {
	q := newVersionsHealQueue()
	c := &versionsHealCandidate{bucket: "b", object: "obj", attempts: versionsHealMaxRetries}
	q.requeue(c, time.Millisecond)
	assert.Equal(t, 0, q.Len(), "candidate at max attempts is dropped, read-path heal still covers it")
}

// TestRetryFilerOp_SucceedsBeforeExhaustion confirms a flaky op that
// eventually succeeds is reported as success without surfacing prior
// errors.
func TestRetryFilerOp_SucceedsBeforeExhaustion(t *testing.T) {
	calls := 0
	err := retryFilerOp("test", func() error {
		calls++
		if calls < 3 {
			return errors.New("transient")
		}
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, 3, calls, "stops calling once the op succeeds")
}

// TestRetryFilerOp_PropagatesAfterExhaustion confirms a deterministic
// failure is wrapped with the attempt count so operators can tell at
// a glance whether the underlying issue is transient.
func TestRetryFilerOp_PropagatesAfterExhaustion(t *testing.T) {
	calls := 0
	err := retryFilerOp("test", func() error {
		calls++
		return errors.New("permanent")
	})
	require.Error(t, err)
	assert.Equal(t, updateLatestRetryAttempts, calls, "ran the full retry budget")
	assert.Contains(t, err.Error(), "exhausted")
	assert.Contains(t, err.Error(), "permanent", "underlying error preserved")
}
