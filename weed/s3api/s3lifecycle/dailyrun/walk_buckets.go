package dailyrun

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/bootstrap"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/engine"
)

// WalkBuckets runs bootstrap.Walk for each bucket using a per-bucket
// ListFunc wrapped to drop entries whose ShardID isn't in shards.
// Returns the first bucket error; remaining buckets log and continue
// so one bucket's filer error doesn't kill the whole walk. Every
// bucket's result goes to runStats when it is non-nil.
//
// A non-nil progress keeps an unfinished walk's finished buckets and
// positions, and the next call for the same view and shards resumes
// from them; a completed walk clears them, so the next one is full.
// The returned time is when the earliest bucket the walk covers was
// listed: its own start, or the start of the record it resumed.
//
// Composable shape: callers (the worker handler) supply a real
// filer-backed ListFunc and a real Dispatcher (WalkerDispatcher).
// Tests pass bootstrap.EntryCallback and a stub Dispatcher.
func WalkBuckets(ctx context.Context, view *engine.Snapshot, shards []int, buckets []string, list bootstrap.ListFunc, dispatch bootstrap.Dispatcher, runStats *RunStats, progress WalkCheckpointPersister) (time.Time, error) {
	if view == nil {
		return time.Time{}, errors.New("WalkBuckets: nil view")
	}
	if list == nil {
		return time.Time{}, errors.New("WalkBuckets: nil list")
	}
	if dispatch == nil {
		return time.Time{}, errors.New("WalkBuckets: nil dispatch")
	}
	shardSet := make(map[int]bool, len(shards))
	for _, sh := range shards {
		shardSet[sh] = true
	}
	filtered := perShardListFunc(list, shardSet, runStats)
	walk := loadWalkProgress(ctx, progress, view, shards, buckets)
	var firstErr error
	for i, b := range buckets {
		if err := ctx.Err(); err != nil {
			for _, rest := range buckets[i:] {
				runStats.finishBucket(rest, bucketSkipped, nil)
			}
			_ = walk.finish(ctx, err)
			return walk.cp.StartedAt, err
		}
		if walk.done[b] {
			runStats.finishBucket(b, bucketWalked, nil)
			continue
		}
		runStats.startBucket(b)
		_, err := bootstrap.Walk(ctx, view, b, walk.track(b, filtered), dispatch, bootstrap.WalkOptions{Resume: walk.cp.Resume[b]})
		switch {
		case err == nil:
			runStats.finishBucket(b, bucketWalked, nil)
			walk.bucketDone(ctx, b)
			continue
		case ctx.Err() != nil:
			runStats.finishBucket(b, bucketSkipped, nil)
		default:
			runStats.finishBucket(b, bucketFailed, err)
		}
		if firstErr == nil {
			firstErr = fmt.Errorf("walk %s: %w", b, err)
		} else {
			glog.V(1).Infof("walker: additional bucket %s: %v", b, err)
		}
	}
	if err := walk.finish(ctx, firstErr); err != nil {
		return walk.cp.StartedAt, err
	}
	return walk.cp.StartedAt, firstErr
}

// perShardListFunc wraps an inner ListFunc so only entries whose
// logical-key shard is in shardSet reach the callback. The walker
// emits one entry per logical object — versioned siblings share the
// same logical key so they're either all in-shard or all out — and
// MPU init records carry the user's destination in DestKey, so the
// shard predicate must use DestKey there to match what the dispatcher
// will send.
func perShardListFunc(inner bootstrap.ListFunc, shardSet map[int]bool, runStats *RunStats) bootstrap.ListFunc {
	return func(ctx context.Context, bucket, start string, cb func(*bootstrap.Entry) error) error {
		// Listings emit every version of a key back to back.
		lastPath := ""
		return inner(ctx, bucket, start, func(e *bootstrap.Entry) error {
			if e == nil {
				return nil
			}
			if !shardSet[entryShardID(bucket, e)] {
				return nil
			}
			if !e.IsDirectory || e.IsMPUInit {
				runStats.visitEntry(!e.IsMPUInit && e.Path != lastPath)
				lastPath = e.Path
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
