package dailyrun

import (
	"context"
	"errors"
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

// dispatchWithRetry sends one LifecycleDelete request, retrying on
// transport errors up to transportRetryAttempts times with exponential
// backoff. Returns the server's outcome on success, or an error if all
// retries exhausted. Server outcomes (RETRY_LATER / BLOCKED) bypass
// the retry — the daily run's halt-on-failure semantics handles them.
func dispatchWithRetry(ctx context.Context, client LifecycleClient, m router.Match) (s3_lifecycle_pb.LifecycleDeleteOutcome, error) {
	req := buildDeleteRequest(m)
	backoff := transportRetryInitial
	var lastErr error
	for attempt := 1; attempt <= transportRetryAttempts; attempt++ {
		resp, err := client.LifecycleDelete(ctx, req)
		if err == nil {
			return resp.Outcome, nil
		}
		// Context cancellation is shutdown, not a transport flake.
		// Surface it immediately so the caller halts and tomorrow
		// resumes from the same cursor.
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return s3_lifecycle_pb.LifecycleDeleteOutcome_LIFECYCLE_DELETE_OUTCOME_UNSPECIFIED, err
		}
		lastErr = err
		if attempt == transportRetryAttempts {
			break
		}
		select {
		case <-ctx.Done():
			return s3_lifecycle_pb.LifecycleDeleteOutcome_LIFECYCLE_DELETE_OUTCOME_UNSPECIFIED, ctx.Err()
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > transportRetryMax {
			backoff = transportRetryMax
		}
	}
	return s3_lifecycle_pb.LifecycleDeleteOutcome_LIFECYCLE_DELETE_OUTCOME_UNSPECIFIED, lastErr
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
