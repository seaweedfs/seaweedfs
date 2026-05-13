package dailyrun

import (
	"context"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/pb/s3_lifecycle_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/bootstrap"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/engine"
	"github.com/seaweedfs/seaweedfs/weed/stats"
)

// WalkerDispatcher adapts LifecycleClient to bootstrap.Dispatcher so
// the Phase 4b walker can drive the same LifecycleDelete RPC the
// meta-log replay path uses. No CAS witness is supplied; the server's
// identityMatches treats nil ExpectedIdentity as "bootstrap call, skip
// witness" (see weed/s3api/s3api_internal_lifecycle.go), which is the
// right contract for a full-tree walk that has just observed the entry.
type WalkerDispatcher struct {
	Client LifecycleClient
}

// Compile-time check.
var _ bootstrap.Dispatcher = (*WalkerDispatcher)(nil)

// Delete classifies non-DONE server outcomes as errors so the walker
// halts and the caller persists progress under the bootstrap
// checkpoint rather than silently skipping objects.
func (d *WalkerDispatcher) Delete(ctx context.Context, action *engine.CompiledAction, entry *bootstrap.Entry) error {
	if d == nil || d.Client == nil {
		return fmt.Errorf("walker dispatch: nil client")
	}
	if action == nil || entry == nil {
		return fmt.Errorf("walker dispatch: nil action or entry")
	}
	objectPath := entry.Path
	if entry.IsMPUInit {
		// Rule-prefix matching uses DestKey (the user's intended
		// object key); dispatch uses entry.Path (.uploads/<id>),
		// which is what the server's ABORT_MPU handler expects in
		// req.ObjectPath — it strips the .uploads/ prefix to get
		// the upload id and reads the init record from that
		// directory. DestKey is the dispatch ANTI-pattern here: it
		// looks like a regular object path, the server's check for
		// the .uploads/ prefix fails, and the dispatch comes back
		// as BLOCKED FATAL_EVENT_ERROR.
		if entry.DestKey == "" {
			return fmt.Errorf("walker dispatch: MPU init entry with empty DestKey: %s", entry.Path)
		}
	}
	rh := action.Key.RuleHash
	req := &s3_lifecycle_pb.LifecycleDeleteRequest{
		Bucket:     action.Bucket,
		ObjectPath: objectPath,
		VersionId:  entry.VersionID,
		RuleHash:   rh[:],
		ActionKind: toProtoActionKind(action.Key.ActionKind),
		// ExpectedIdentity intentionally nil; server bootstraps from
		// the live entry on this code path.
	}
	kindLabel := action.Key.ActionKind.String()
	resp, err := d.Client.LifecycleDelete(ctx, req)
	if err != nil {
		// "RPC_ERROR" matches the streaming dispatcher and processMatches
		// labels so transport-class failures aggregate under one key.
		stats.S3LifecycleDispatchCounter.WithLabelValues(action.Bucket, kindLabel, "RPC_ERROR").Inc()
		return fmt.Errorf("walker dispatch %s/%s %s: %w", action.Bucket, objectPath, action.Key.ActionKind, err)
	}
	if resp == nil {
		// A misbehaving server stub returning (nil, nil) would panic on
		// the switch below. Bucket under RPC_ERROR rather than a new
		// label — operationally it's the same class of failure.
		stats.S3LifecycleDispatchCounter.WithLabelValues(action.Bucket, kindLabel, "RPC_ERROR").Inc()
		return fmt.Errorf("walker dispatch %s/%s %s: nil response", action.Bucket, objectPath, action.Key.ActionKind)
	}
	stats.S3LifecycleDispatchCounter.WithLabelValues(action.Bucket, kindLabel, resp.Outcome.String()).Inc()
	switch resp.Outcome {
	case s3_lifecycle_pb.LifecycleDeleteOutcome_DONE,
		s3_lifecycle_pb.LifecycleDeleteOutcome_NOOP_RESOLVED,
		s3_lifecycle_pb.LifecycleDeleteOutcome_SKIPPED_OBJECT_LOCK:
		return nil
	default:
		// RETRY_LATER / BLOCKED / UNSPECIFIED: surface as error so the
		// walk halts at this entry and resumes from
		// Checkpoint.LastScannedPath on the next run.
		return fmt.Errorf("walker dispatch %s/%s %s: outcome=%s reason=%s",
			action.Bucket, objectPath, action.Key.ActionKind, resp.Outcome, resp.Reason)
	}
}
