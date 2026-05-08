package dispatcher

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/s3_lifecycle_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/reader"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/router"
)

// LifecycleClient abstracts the LifecycleDelete RPC so the dispatcher is
// testable without a live S3 server.
type LifecycleClient interface {
	LifecycleDelete(ctx context.Context, req *s3_lifecycle_pb.LifecycleDeleteRequest) (*s3_lifecycle_pb.LifecycleDeleteResponse, error)
}

// Dispatcher consumes due Matches, calls LifecycleDelete, and routes the
// outcome back to the per-shard cursor.
//
// State machine for one Match:
//   DONE / NOOP_RESOLVED / SKIPPED_OBJECT_LOCK -> Cursor.Advance
//   RETRY_LATER (within budget) -> back into the schedule with backoff
//   RETRY_LATER (budget exhausted) / BLOCKED   -> Cursor.Freeze in-memory
//   FATAL_EVENT_ERROR / unknown                -> treat as BLOCKED
//
// A frozen cursor doesn't advance, so the durable cursor is the durable
// "stuck" state on its own: a worker restart re-encounters the poison
// event at MinTsNs and re-freezes after the same retry cycle. No separate
// blocker store is needed.
type Dispatcher struct {
	ShardID  int
	Client   LifecycleClient
	Cursor   *reader.Cursor
	Schedule *router.Schedule

	// RetryBudget caps RETRY_LATER attempts before escalating to BLOCKED.
	// Zero defaults to defaultRetryBudget.
	RetryBudget int

	// RetryBackoff is the wait between RETRY_LATER re-schedules. Zero
	// defaults to defaultRetryBackoff.
	RetryBackoff time.Duration

	// retries[Match.Key+ObjectKey] = attempts so far. In-memory only:
	// worker restart resets the budget, which is fine because the cursor
	// is durable and the same poison event will land us here again.
	retries map[retryKey]int
}

const (
	defaultRetryBudget   = 5
	defaultRetryBackoff  = 30 * time.Second
)

type retryKey struct {
	bucket    string
	ruleHash  [8]byte
	kind      s3lifecycle.ActionKind
	objectKey string
	versionID string
}

func keyOf(m router.Match) retryKey {
	return retryKey{
		bucket:    m.Key.Bucket,
		ruleHash:  m.Key.RuleHash,
		kind:      m.Key.ActionKind,
		objectKey: m.ObjectKey,
		versionID: m.VersionID,
	}
}

func (d *Dispatcher) budget() int {
	if d.RetryBudget > 0 {
		return d.RetryBudget
	}
	return defaultRetryBudget
}

func (d *Dispatcher) backoff() time.Duration {
	if d.RetryBackoff > 0 {
		return d.RetryBackoff
	}
	return defaultRetryBackoff
}

// Tick drains the schedule for due Matches and dispatches each. Returns the
// count of Matches processed. Safe to call repeatedly.
func (d *Dispatcher) Tick(ctx context.Context, now time.Time) int {
	if d.retries == nil {
		d.retries = map[retryKey]int{}
	}
	due := d.Schedule.Drain(now)
	for _, m := range due {
		if err := ctx.Err(); err != nil {
			// Re-queue and return; the caller is shutting down.
			d.Schedule.Add(m)
			return 0
		}
		d.dispatchOne(ctx, m, now)
	}
	return len(due)
}

func (d *Dispatcher) dispatchOne(ctx context.Context, m router.Match, now time.Time) {
	// A frozen cursor means a prior BLOCKED is still active for this
	// (shard, ActionKey); skip until operator clears it.
	if d.Cursor.IsFrozen(m.Key) {
		return
	}

	ruleHash := m.Key.RuleHash
	req := &s3_lifecycle_pb.LifecycleDeleteRequest{
		Bucket:           m.Bucket,
		ObjectPath:       m.ObjectKey,
		VersionId:        m.VersionID,
		RuleHash:         ruleHash[:],
		ActionKind:       toProtoActionKind(m.Key.ActionKind),
		ExpectedIdentity: toProtoIdentity(m.Identity),
	}
	resp, err := d.Client.LifecycleDelete(ctx, req)
	if err != nil {
		// Context cancellation is shutdown, not a transport failure: put
		// the Match back on the schedule untouched so the next worker
		// run picks it up at its original DueTime, with no retry-budget
		// burn.
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			d.Schedule.Add(m)
			return
		}
		// Transport error: classify as RETRY_LATER. The remote handler
		// already classifies its own filer-side errors; the only path
		// that hits this branch is the RPC itself failing.
		d.handleRetryLater(ctx, m, fmt.Sprintf("RPC: %v", err), now)
		return
	}
	switch resp.Outcome {
	case s3_lifecycle_pb.LifecycleDeleteOutcome_DONE,
		s3_lifecycle_pb.LifecycleDeleteOutcome_NOOP_RESOLVED,
		s3_lifecycle_pb.LifecycleDeleteOutcome_SKIPPED_OBJECT_LOCK:
		d.advance(m)
	case s3_lifecycle_pb.LifecycleDeleteOutcome_RETRY_LATER:
		d.handleRetryLater(ctx, m, resp.Reason, now)
	case s3_lifecycle_pb.LifecycleDeleteOutcome_BLOCKED:
		d.handleBlocked(ctx, m, resp.Reason)
	default:
		d.handleBlocked(ctx, m, fmt.Sprintf("unknown outcome %v: %s", resp.Outcome, resp.Reason))
	}
}

func (d *Dispatcher) advance(m router.Match) {
	delete(d.retries, keyOf(m))
	d.Cursor.Advance(m.Key, m.EventTs.UnixNano())
}

func (d *Dispatcher) handleRetryLater(ctx context.Context, m router.Match, reason string, now time.Time) {
	rk := keyOf(m)
	d.retries[rk]++
	if d.retries[rk] > d.budget() {
		d.handleBlocked(ctx, m, fmt.Sprintf("retry budget exhausted: %s", reason))
		return
	}
	// Re-schedule with backoff; same Match, new DueTime.
	m.DueTime = now.Add(d.backoff())
	d.Schedule.Add(m)
}

func (d *Dispatcher) handleBlocked(ctx context.Context, m router.Match, reason string) {
	delete(d.retries, keyOf(m))
	glog.Warningf("lifecycle: cursor frozen shard=%d key=%+v eventTs=%s reason=%s",
		d.ShardID, m.Key, m.EventTs.UTC().Format(time.RFC3339Nano), reason)
	d.Cursor.Freeze(m.Key, m.EventTs.UnixNano())
}

func toProtoActionKind(k s3lifecycle.ActionKind) s3_lifecycle_pb.ActionKind {
	switch k {
	case s3lifecycle.ActionKindExpirationDays:
		return s3_lifecycle_pb.ActionKind_EXPIRATION_DAYS
	case s3lifecycle.ActionKindExpirationDate:
		return s3_lifecycle_pb.ActionKind_EXPIRATION_DATE
	case s3lifecycle.ActionKindNoncurrentDays:
		return s3_lifecycle_pb.ActionKind_NONCURRENT_DAYS
	case s3lifecycle.ActionKindNewerNoncurrent:
		return s3_lifecycle_pb.ActionKind_NEWER_NONCURRENT
	case s3lifecycle.ActionKindAbortMPU:
		return s3_lifecycle_pb.ActionKind_ABORT_MPU
	case s3lifecycle.ActionKindExpiredDeleteMarker:
		return s3_lifecycle_pb.ActionKind_EXPIRED_DELETE_MARKER
	}
	return s3_lifecycle_pb.ActionKind_ACTION_KIND_UNSPECIFIED
}

func toProtoIdentity(id *router.EntryIdentity) *s3_lifecycle_pb.EntryIdentity {
	if id == nil {
		return nil
	}
	return &s3_lifecycle_pb.EntryIdentity{
		MtimeNs:      id.MtimeNs,
		Size:         id.Size,
		HeadFid:      id.HeadFid,
		ExtendedHash: id.ExtendedHash,
	}
}
