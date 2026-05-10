package scheduler

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/dispatcher"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/reader"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// pipelineFanout is the thin shard-routing layer the bootstrapper sees
// as an EventInjector. It maps Event.ShardID to the pipeline that owns
// that shard. Was previously at 0% coverage.

func TestPipelineFanout_NilEventIsNoOp(t *testing.T) {
	// A nil event must be silently absorbed; otherwise a follow-up
	// panic in the receiving pipeline would crash the bootstrapper.
	f := pipelineFanout{}
	assert.NoError(t, f.InjectEvent(context.Background(), nil))
}

func TestPipelineFanout_UnknownShardIsNoOp(t *testing.T) {
	// A shard not covered by any pipeline in the fanout returns nil
	// rather than erroring; the comment in scheduler.go documents this
	// as forward-compat for future shard-mapping changes that might
	// introduce gaps.
	f := pipelineFanout{0: &dispatcher.Pipeline{EventBuffer: 1}}
	assert.NoError(t, f.InjectEvent(context.Background(), &reader.Event{ShardID: 99}))
}

func TestPipelineFanout_KnownShardSucceeds(t *testing.T) {
	// A matching shard reaches the pipeline's InjectEvent, which writes
	// to its (buffered) events channel and returns nil.
	f := pipelineFanout{0: &dispatcher.Pipeline{EventBuffer: 1}}
	assert.NoError(t, f.InjectEvent(context.Background(), &reader.Event{ShardID: 0}))
}

func TestPipelineFanout_PropagatesContextCancellation(t *testing.T) {
	// When the underlying pipeline's InjectEvent blocks on a full
	// buffer and the ctx is canceled, the fanout must propagate the
	// ctx error. Pre-fill the pipeline's buffer (size 1) so the second
	// send blocks long enough for the cancellation to win the select.
	p := &dispatcher.Pipeline{EventBuffer: 1}
	require.NoError(t, p.InjectEvent(context.Background(), &reader.Event{}))
	f := pipelineFanout{0: p}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := f.InjectEvent(ctx, &reader.Event{ShardID: 0})
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestPipelineFanout_RoutesToCorrectPipeline(t *testing.T) {
	// Two pipelines, each with buffer=1: an event for shard 7 must
	// fill pipeline B's buffer (proven by the second send to that
	// pipeline blocking with canceled ctx) without affecting pipeline
	// A's buffer (proven by the third send still succeeding to A
	// because A's buffer is still empty).
	pA := &dispatcher.Pipeline{EventBuffer: 1}
	pB := &dispatcher.Pipeline{EventBuffer: 1}
	f := pipelineFanout{0: pA, 7: pB}

	require.NoError(t, f.InjectEvent(context.Background(), &reader.Event{ShardID: 7}))

	// Second send to shard 7 would block on the full buffer; use a
	// pre-canceled ctx to detect the buffer-full state without hanging.
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	err := f.InjectEvent(canceledCtx, &reader.Event{ShardID: 7})
	require.Error(t, err, "B's buffer should be full so a canceled-ctx send returns ctx.Err")

	// Send to shard 0 still succeeds because A's buffer is untouched.
	require.NoError(t, f.InjectEvent(context.Background(), &reader.Event{ShardID: 0}))
}
