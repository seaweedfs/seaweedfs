package dailyrun

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/s3_lifecycle_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/bootstrap"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/reader"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/router"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
	"google.golang.org/protobuf/proto"
)

// stallingClient blocks each attempt until its context ends, except
// attempts listed in answerAfter, which answer DONE after that delay.
type stallingClient struct {
	mu          sync.Mutex
	requests    []*s3_lifecycle_pb.LifecycleDeleteRequest
	answerAfter map[int]time.Duration
	onCall      func(attempt int)
}

func (c *stallingClient) LifecycleDelete(ctx context.Context, req *s3_lifecycle_pb.LifecycleDeleteRequest) (*s3_lifecycle_pb.LifecycleDeleteResponse, error) {
	c.mu.Lock()
	c.requests = append(c.requests, req)
	attempt := len(c.requests)
	delay, answers := c.answerAfter[attempt]
	c.mu.Unlock()
	if c.onCall != nil {
		c.onCall(attempt)
	}
	if answers {
		select {
		case <-time.After(delay):
			return &s3_lifecycle_pb.LifecycleDeleteResponse{Outcome: s3_lifecycle_pb.LifecycleDeleteOutcome_DONE}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

func (c *stallingClient) calls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.requests)
}

func versionedMatch() router.Match {
	m := sampleMatch()
	m.Key.ActionKind = s3lifecycle.ActionKindNoncurrentDays
	m.VersionID = "v_1"
	m.Identity = &router.EntryIdentity{MtimeNs: 1, Size: 7, HeadFid: "3,01", ExtendedHash: []byte{0xaa}}
	return m
}

func TestDispatch_StalledAttemptsTimeOutAndFail(t *testing.T) {
	c := &stallingClient{}
	start := time.Now()
	_, err := dispatchWithRetry(context.Background(), c, sampleMatch(), 20*time.Millisecond)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Contains(t, err.Error(), "timed out after 20ms")
	assert.Equal(t, transportRetryAttempts, c.calls(), "an attempt timeout is retried like a transport error")
	assert.Less(t, time.Since(start), 5*time.Second)
}

func TestDispatch_SlowAttemptWithinTimeoutSucceeds(t *testing.T) {
	c := &stallingClient{answerAfter: map[int]time.Duration{1: 30 * time.Millisecond}}
	out, err := dispatchWithRetry(context.Background(), c, sampleMatch(), time.Second)
	require.NoError(t, err)
	assert.Equal(t, s3_lifecycle_pb.LifecycleDeleteOutcome_DONE, out)
	assert.Equal(t, 1, c.calls())
}

func TestDispatch_RetryAfterTimeoutResendsSameRequest(t *testing.T) {
	c := &stallingClient{answerAfter: map[int]time.Duration{2: 0}}
	out, err := dispatchWithRetry(context.Background(), c, versionedMatch(), 20*time.Millisecond)
	require.NoError(t, err)
	assert.Equal(t, s3_lifecycle_pb.LifecycleDeleteOutcome_DONE, out)
	require.Len(t, c.requests, 2)
	assert.True(t, proto.Equal(c.requests[0], c.requests[1]), "a retry after a lost reply must keep the version and identity witness")
	assert.Equal(t, "v_1", c.requests[1].VersionId)
	assert.Equal(t, "3,01", c.requests[1].ExpectedIdentity.GetHeadFid())
}

func TestDispatch_ParentCancelInterruptsActiveAttempt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	c := &stallingClient{onCall: func(int) { time.AfterFunc(20*time.Millisecond, cancel) }}
	start := time.Now()
	_, err := dispatchWithRetry(ctx, c, sampleMatch(), time.Hour)
	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 1, c.calls(), "parent cancellation must not be retried")
	assert.Less(t, time.Since(start), 5*time.Second)
}

func TestDispatch_ParentCancelInterruptsBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	c := &fakeLifecycleClient{scripted: []scriptedResp{{err: errors.New("transport boom")}}}
	time.AfterFunc(10*time.Millisecond, cancel)
	_, err := dispatchWithRetry(ctx, c, sampleMatch(), time.Hour)
	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, int32(1), c.calls, "cancellation during backoff must stop before the next attempt")
}

func TestProcessMatches_AttemptTimeoutHaltsShard(t *testing.T) {
	runNow := time.Now()
	m := sampleMatch()
	m.DueTime = runNow.Add(-time.Hour)
	c := &stallingClient{}
	cfg := Config{Client: c, DeleteTimeout: 10 * time.Millisecond}
	_, halted, err := processMatches(context.Background(), cfg, runNow, &reader.Event{TsNs: runNow.UnixNano()}, []router.Match{m})
	require.NoError(t, err)
	assert.True(t, halted, "an unresolved delete must hold the cursor")
}

func TestProcessMatches_LimiterWaitDoesNotSpendDeleteTimeout(t *testing.T) {
	runNow := time.Now()
	m := sampleMatch()
	m.DueTime = runNow.Add(-time.Hour)
	lim := rate.NewLimiter(rate.Every(150*time.Millisecond), 1)
	_ = lim.AllowN(time.Now(), 1)
	c := &stallingClient{answerAfter: map[int]time.Duration{1: 20 * time.Millisecond}}
	cfg := Config{Client: c, Limiter: lim, DeleteTimeout: 100 * time.Millisecond}
	_, halted, err := processMatches(context.Background(), cfg, runNow, &reader.Event{TsNs: runNow.UnixNano()}, []router.Match{m})
	require.NoError(t, err)
	assert.False(t, halted)
	assert.Equal(t, 1, c.calls())
}

func TestWalkerDispatcher_AttemptTimeoutHaltsWalk(t *testing.T) {
	c := &stallingClient{}
	d := &WalkerDispatcher{Client: c, DeleteTimeout: 10 * time.Millisecond}
	err := d.Delete(context.Background(), sampleAction(t, s3lifecycle.ActionKindExpirationDays), &bootstrap.Entry{Path: "obj"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "timed out after 10ms")
	assert.Equal(t, 1, c.calls())
}

func TestWalkerDispatcher_LimiterWaitDoesNotSpendDeleteTimeout(t *testing.T) {
	lim := rate.NewLimiter(rate.Every(150*time.Millisecond), 1)
	_ = lim.AllowN(time.Now(), 1)
	c := &stallingClient{answerAfter: map[int]time.Duration{1: 20 * time.Millisecond}}
	d := &WalkerDispatcher{Client: c, Limiter: lim, DeleteTimeout: 100 * time.Millisecond}
	err := d.Delete(context.Background(), sampleAction(t, s3lifecycle.ActionKindExpirationDays), &bootstrap.Entry{Path: "obj"})
	require.NoError(t, err)
}
