package dailyrun

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/s3_lifecycle_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/router"
)

// transportRetryAttempts is the in-run retry budget for transport
// errors (RPC dial failures, stream resets). Server-side outcomes
// (RETRY_LATER, BLOCKED) are NOT retried in-run — the server already
// classified them as "wait until next run." The retry exists only to
// paper over a single flake from the network.
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
		err = fmt.Errorf("lifecycle delete attempt timed out after %s: %w", timeout, err)
	}
	return resp, err
}

// dispatchWithRetry sends one LifecycleDelete request, retrying on
// transport errors and attempt timeouts up to transportRetryAttempts
// times with exponential backoff. Returns the server's outcome on
// success, or an error if all retries exhausted. Server outcomes
// (RETRY_LATER / BLOCKED) bypass the retry — the daily run's
// halt-on-failure semantics handles them.
func dispatchWithRetry(ctx context.Context, client LifecycleClient, m router.Match, timeout time.Duration) (s3_lifecycle_pb.LifecycleDeleteOutcome, error) {
	req := buildDeleteRequest(m)
	backoff := transportRetryInitial
	var lastErr error
	for attempt := 1; attempt <= transportRetryAttempts; attempt++ {
		resp, err := deleteOnce(ctx, client, req, timeout)
		if err == nil {
			return resp.Outcome, nil
		}
		// Parent cancellation is shutdown, not a transport flake.
		// Surface it immediately so the caller halts and tomorrow
		// resumes from the same cursor.
		if ctxErr := ctx.Err(); ctxErr != nil {
			return s3_lifecycle_pb.LifecycleDeleteOutcome_LIFECYCLE_DELETE_OUTCOME_UNSPECIFIED, ctxErr
		}
		lastErr = err
		if attempt == transportRetryAttempts {
			break
		}
		select {
		case <-ctx.Done():
			return s3_lifecycle_pb.LifecycleDeleteOutcome_LIFECYCLE_DELETE_OUTCOME_UNSPECIFIED, ctx.Err()
		case <-time.After(jitter(backoff)):
		}
		backoff *= 2
		if backoff > transportRetryMax {
			backoff = transportRetryMax
		}
	}
	return s3_lifecycle_pb.LifecycleDeleteOutcome_LIFECYCLE_DELETE_OUTCOME_UNSPECIFIED, lastErr
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
