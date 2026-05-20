package dailyrun

import (
	"context"
	"errors"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/bootstrap"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/engine"
)

// WalkBuckets runs bootstrap.Walk for each bucket using a per-bucket
// ListFunc wrapped to drop entries whose ShardID doesn't belong to
// shardID. Returns the first bucket error; remaining buckets log and
// continue so one bucket's filer error doesn't kill the whole walk.
//
// Composable shape: callers (the worker handler) supply a real
// filer-backed ListFunc and a real Dispatcher (WalkerDispatcher).
// Tests pass bootstrap.EntryCallback and a stub Dispatcher.
func WalkBuckets(ctx context.Context, view *engine.Snapshot, shardID int, buckets []string, list bootstrap.ListFunc, dispatch bootstrap.Dispatcher) error {
	if view == nil {
		return errors.New("WalkBuckets: nil view")
	}
	if list == nil {
		return errors.New("WalkBuckets: nil list")
	}
	if dispatch == nil {
		return errors.New("WalkBuckets: nil dispatch")
	}
	filtered := perShardListFunc(list, shardID)
	var firstErr error
	for _, b := range buckets {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, err := bootstrap.Walk(ctx, view, b, filtered, dispatch, bootstrap.WalkOptions{}); err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("walk %s: %w", b, err)
			} else {
				glog.V(1).Infof("walker: additional bucket %s: %v", b, err)
			}
		}
	}
	return firstErr
}

// perShardListFunc wraps an inner ListFunc so only entries whose
// logical-key shard matches shardID reach the callback. The walker
// emits one entry per logical object — versioned siblings share the
// same logical key so they're either all in-shard or all out — and
// MPU init records carry the user's destination in DestKey, so the
// shard predicate must use DestKey there to match what the dispatcher
// will send.
func perShardListFunc(inner bootstrap.ListFunc, shardID int) bootstrap.ListFunc {
	return func(ctx context.Context, bucket, start string, cb func(*bootstrap.Entry) error) error {
		return inner(ctx, bucket, start, func(e *bootstrap.Entry) error {
			if e == nil {
				return nil
			}
			if entryShardID(bucket, e) != shardID {
				return nil
			}
			return cb(e)
		})
	}
}

func entryShardID(bucket string, e *bootstrap.Entry) int {
	key := e.Path
	if e.IsMPUInit && e.DestKey != "" {
		key = e.DestKey
	}
	return s3lifecycle.ShardID(bucket, key)
}
