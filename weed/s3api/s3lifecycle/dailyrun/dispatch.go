package dailyrun

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/s3_lifecycle_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/router"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"golang.org/x/time/rate"
	"google.golang.org/grpc/codes"
)

// transportRetryAttempts is the in-run retry budget shared by the walker
// and replay deletes. Only transport failures and attempt timeouts are
// retried; server outcomes (RETRY_LATER, BLOCKED) wait for the next run.
const transportRetryAttempts = 3

// transportRetryInitial is the first sleep after a failed attempt.
// Doubled per attempt up to transportRetryMax. Phase 2 keeps these
// small because the daily run's own MaxRuntime is the larger cap.
const (
	transportRetryInitial = 200 * time.Millisecond
	transportRetryMax     = 5 * time.Second
)

// DefaultDeleteTimeout bounds one LifecycleDelete attempt when the
// caller sets no timeout.
const DefaultDeleteTimeout = time.Minute

var (
	errDeleteAttemptTimedOut = errors.New("lifecycle delete attempt timed out")
	errNilDeleteResponse     = errors.New("nil response")
)

// deleteOnce sends one LifecycleDelete attempt under its own deadline,
// so a stalled RPC fails that attempt instead of holding the shard for
// the job's lifetime.
func deleteOnce(ctx context.Context, client LifecycleClient, req *s3_lifecycle_pb.LifecycleDeleteRequest, timeout time.Duration) (*s3_lifecycle_pb.LifecycleDeleteResponse, error) {
	if timeout <= 0 {
		timeout = DefaultDeleteTimeout
	}
	attemptCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	resp, err := client.LifecycleDelete(attemptCtx, req)
	if err != nil && ctx.Err() == nil && attemptCtx.Err() != nil {
		err = fmt.Errorf("%w after %s: %w", errDeleteAttemptTimedOut, timeout, err)
	}
	if err == nil && resp == nil {
		err = errNilDeleteResponse
	}
	return resp, err
}

// isRetryableDeleteError reports whether a failed attempt may be resent.
// Authentication, permission and argument errors fail the same way
// again, so they surface at once.
func isRetryableDeleteError(err error) bool {
	if errors.Is(err, errDeleteAttemptTimedOut) || util.IsTransientError(err) {
		return true
	}
	st, ok := util.ServerStatus(err)
	return ok && st.Code() == codes.DeadlineExceeded
}

// deleteWithRetry sends req, retrying transport failures and attempt
// timeouts up to transportRetryAttempts times with jittered exponential
// backoff. Every attempt waits on limiter, so retries stay inside the
// shared delete budget. The resent request keeps its version and identity
// witness, so a delete that committed before its reply was lost resolves
// as NOOP_RESOLVED on the retry.
func deleteWithRetry(ctx context.Context, client LifecycleClient, limiter *rate.Limiter, req *s3_lifecycle_pb.LifecycleDeleteRequest, timeout time.Duration) (*s3_lifecycle_pb.LifecycleDeleteResponse, error) {
	backoff := transportRetryInitial
	for attempt := 1; ; attempt++ {
		if limiter != nil {
			waitStart := time.Now()
			if err := limiter.Wait(ctx); err != nil {
				return nil, fmt.Errorf("limiter: %w", err)
			}
			stats.S3LifecycleDispatchLimiterWaitSeconds.Observe(time.Since(waitStart).Seconds())
		}
		resp, err := deleteOnce(ctx, client, req, timeout)
		if err == nil {
			return resp, nil
		}
		// Parent cancellation is shutdown, not a transport flake.
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		if attempt >= transportRetryAttempts || !isRetryableDeleteError(err) {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(jitter(backoff)):
		}
		backoff = min(backoff*2, transportRetryMax)
	}
}

// dispatchWithRetry sends the LifecycleDelete for m under the shared
// retry policy and returns the server's outcome.
func dispatchWithRetry(ctx context.Context, client LifecycleClient, limiter *rate.Limiter, m router.Match, timeout time.Duration) (s3_lifecycle_pb.LifecycleDeleteOutcome, error) {
	resp, err := deleteWithRetry(ctx, client, limiter, buildDeleteRequest(m), timeout)
	if err != nil {
		return s3_lifecycle_pb.LifecycleDeleteOutcome_LIFECYCLE_DELETE_OUTCOME_UNSPECIFIED, err
	}
	return resp.Outcome, nil
}

// jitter returns a duration in the range [d/2, d) using equal jitter.
// Prevents thundering herds when many daily-run workers retry simultaneously.
func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	half := d / 2
	if half <= 0 {
		return d
	}
	return half + time.Duration(rand.Int63n(int64(half)))
}

// buildDeleteRequest constructs the LifecycleDelete RPC payload for a
// router Match. Mirrors dispatcher.dispatchOne's request shape — both
// targets the same server-side handler and the proto encoding must
// match exactly. Duplicated rather than shared because Phase 5
// deletes the dispatcher's call site and the cross-package dependency
// would just create a temporary import.
func buildDeleteRequest(m router.Match) *s3_lifecycle_pb.LifecycleDeleteRequest {
	rh := m.Key.RuleHash
	return &s3_lifecycle_pb.LifecycleDeleteRequest{
		Bucket:           m.Bucket,
		ObjectPath:       m.ObjectKey,
		VersionId:        m.VersionID,
		RuleHash:         rh[:],
		ActionKind:       toProtoActionKind(m.Key.ActionKind),
		ExpectedIdentity: toProtoIdentity(m.Identity),
	}
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
