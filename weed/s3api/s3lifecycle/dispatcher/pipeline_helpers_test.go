package dispatcher

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/reader"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/router"
	stats_collect "github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Direct tests for dispatcher / pipeline pure helpers and their side
// effects on the Prometheus surface. The Tick + Run integration tests
// drive these only as one slice of a larger flow; pinning each helper
// individually makes a regression in the helper itself fail at the
// helper level.

// ---------- observeScheduleDepth ----------

func TestObserveScheduleDepth_ReportsScheduleLen(t *testing.T) {
	// observeScheduleDepth sets the gauge to the current Schedule.Len.
	// The dispatcher calls it on every Tick so operators can see how
	// far behind schedule the worker is. Use a distinct shard label so
	// other tests sharing the global gauge don't bleed in.
	const shard = 137
	d := &Dispatcher{ShardID: shard, Schedule: router.NewSchedule()}
	g := stats_collect.S3LifecycleScheduleDepthGauge.WithLabelValues(strconv.Itoa(shard))
	// Drop the label series at end of test so the global registry
	// doesn't accumulate stale entries across test runs in the same
	// process.
	defer stats_collect.S3LifecycleScheduleDepthGauge.DeleteLabelValues(strconv.Itoa(shard))

	// Empty schedule -> 0.
	d.observeScheduleDepth()
	assert.Equal(t, float64(0), testutil.ToFloat64(g))

	// Adding two matches lifts the gauge to 2 on the next observe.
	t0 := time.Now()
	d.Schedule.Add(router.Match{DueTime: t0})
	d.Schedule.Add(router.Match{DueTime: t0.Add(time.Second)})
	d.observeScheduleDepth()
	assert.Equal(t, float64(2), testutil.ToFloat64(g))

	// Draining drops it back to 0; Tick would re-call but observe is a
	// pure setter so we drive it manually here.
	_ = d.Schedule.Drain(t0.Add(2 * time.Second))
	d.observeScheduleDepth()
	assert.Equal(t, float64(0), testutil.ToFloat64(g))
}

// ---------- ensureEventsChan ----------

func TestEnsureEventsChan_HonorsEventBuffer(t *testing.T) {
	p := &Pipeline{EventBuffer: 7}
	p.ensureEventsChan()
	require.NotNil(t, p.events)
	assert.Equal(t, 7, cap(p.events), "EventBuffer must size the channel")
	require.NotNil(t, p.eventsReady)
}

func TestEnsureEventsChan_FallsBackToDefault(t *testing.T) {
	// EventBuffer=0 means "use the default"; non-positive must not
	// produce a zero-sized channel that would block every InjectEvent.
	p := &Pipeline{}
	p.ensureEventsChan()
	assert.Equal(t, defaultEventBuffer, cap(p.events))
}

func TestEnsureEventsChan_NegativeFallsBackToDefault(t *testing.T) {
	p := &Pipeline{EventBuffer: -1}
	p.ensureEventsChan()
	assert.Equal(t, defaultEventBuffer, cap(p.events))
}

func TestEnsureEventsChan_IdempotentAcrossConcurrentCallers(t *testing.T) {
	// sync.Once must guarantee a single channel allocation under
	// concurrent calls; otherwise a races would create two channels
	// and lose events on the discarded one.
	p := &Pipeline{EventBuffer: 4}
	const N = 32
	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func() {
			defer wg.Done()
			p.ensureEventsChan()
		}()
	}
	wg.Wait()
	require.NotNil(t, p.events)
	assert.Equal(t, 4, cap(p.events))
}

// ---------- InjectEvent ----------

func TestInjectEvent_DeliversToEventsChannel(t *testing.T) {
	p := &Pipeline{EventBuffer: 1}
	ev := &reader.Event{Bucket: "bk", Key: "k"}
	require.NoError(t, p.InjectEvent(context.Background(), ev))

	select {
	case got := <-p.events:
		assert.Same(t, ev, got, "InjectEvent must deliver the same pointer")
	default:
		t.Fatal("event was not enqueued")
	}
}

func TestInjectEvent_ReturnsCtxErrWhenCanceled(t *testing.T) {
	// A canceled context must propagate the cancellation; otherwise
	// the bootstrap walker could keep injecting after the worker is
	// shutting down. ensureEventsChan clamps EventBuffer<=0 to the
	// default, so to force the select to actually block we pre-fill
	// the buffer (size 1) and then send into the canceled context.
	p := &Pipeline{EventBuffer: 1}
	require.NoError(t, p.InjectEvent(context.Background(), &reader.Event{}))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := p.InjectEvent(ctx, &reader.Event{})
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestInjectEvent_EnsureEventsChanCalledIfFirstCall(t *testing.T) {
	// InjectEvent must call ensureEventsChan internally so a caller can
	// inject before Run starts (e.g. bootstrap walks that fire before
	// the reader is up). Pin that the channel exists post-call.
	p := &Pipeline{}
	require.Nil(t, p.events, "preconditions: events not yet allocated")
	require.NoError(t, p.InjectEvent(context.Background(), &reader.Event{}))
	require.NotNil(t, p.events, "InjectEvent must allocate the channel via ensureEventsChan")
}

// ---------- isCtxShutdown ----------

func TestIsCtxShutdown_NilIsNotShutdown(t *testing.T) {
	assert.False(t, isCtxShutdown(nil))
}

func TestIsCtxShutdown_ContextCanceledIsShutdown(t *testing.T) {
	assert.True(t, isCtxShutdown(context.Canceled))
	assert.True(t, isCtxShutdown(context.DeadlineExceeded))
}

func TestIsCtxShutdown_GRPCCanceledStatusIsShutdown(t *testing.T) {
	// Filer / S3 RPCs can come back wrapped as gRPC status codes; the
	// helper must recognize Canceled / DeadlineExceeded by code so a
	// shutdown doesn't get misclassified as a transport failure.
	assert.True(t, isCtxShutdown(status.Error(codes.Canceled, "client closed")))
	assert.True(t, isCtxShutdown(status.Error(codes.DeadlineExceeded, "stream timeout")))
}

func TestIsCtxShutdown_OtherErrorsAreNotShutdown(t *testing.T) {
	// A garden-variety transport error must NOT classify as shutdown;
	// otherwise the worker would silently swallow real failures.
	assert.False(t, isCtxShutdown(errors.New("connection refused")))
	assert.False(t, isCtxShutdown(status.Error(codes.Unavailable, "filer down")))
	assert.False(t, isCtxShutdown(status.Error(codes.Internal, "boom")))
}

// ---------- cursorFileName ----------

func TestCursorFileName_PadsShardIDToTwoDigits(t *testing.T) {
	// The persister stores per-shard cursor files at predictable paths
	// so a manual operator inspection finds them sorted by shard. Pin
	// the zero-padded form so a refactor that drops %02d doesn't break
	// existing on-disk filenames.
	cases := map[int]string{
		0:  "shard-00.json",
		1:  "shard-01.json",
		9:  "shard-09.json",
		10: "shard-10.json",
		15: "shard-15.json",
	}
	for shard, want := range cases {
		t.Run(want, func(t *testing.T) {
			assert.Equal(t, want, cursorFileName(shard))
		})
	}
}
