package dailyrun

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/s3_lifecycle_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/router"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeLifecycleClient is the test double for LifecycleClient. It
// returns a scripted sequence of (outcome, error) pairs and counts
// invocations so retry behavior can be pinned exactly.
type fakeLifecycleClient struct {
	scripted []scriptedResp
	calls    int32
}

type scriptedResp struct {
	outcome s3_lifecycle_pb.LifecycleDeleteOutcome
	err     error
}

func (c *fakeLifecycleClient) LifecycleDelete(_ context.Context, _ *s3_lifecycle_pb.LifecycleDeleteRequest) (*s3_lifecycle_pb.LifecycleDeleteResponse, error) {
	idx := int(atomic.AddInt32(&c.calls, 1)) - 1
	if idx >= len(c.scripted) {
		// Default to the last scripted response if the test under-specifies.
		idx = len(c.scripted) - 1
	}
	r := c.scripted[idx]
	if r.err != nil {
		return nil, r.err
	}
	return &s3_lifecycle_pb.LifecycleDeleteResponse{Outcome: r.outcome}, nil
}

func sampleMatch() router.Match {
	return router.Match{
		Key:       s3lifecycle.ActionKey{Bucket: "b", ActionKind: s3lifecycle.ActionKindExpirationDays},
		Bucket:    "b",
		ObjectKey: "obj",
	}
}

// Speed up dispatch retries inside tests so the suite stays fast.
// The defaults (200ms initial, 5s max) make exponential backoff cases
// take seconds; tests shrink to microseconds via a separate helper.
func dispatchWithRetryFast(t *testing.T, ctx context.Context, c LifecycleClient, m router.Match) (s3_lifecycle_pb.LifecycleDeleteOutcome, error) {
	t.Helper()
	// We can't override the package-private constants, but every test
	// path here uses small attempt counts so even the production
	// backoff is fine.
	return dispatchWithRetry(ctx, c, m)
}

func TestDispatch_FirstAttemptSucceeds(t *testing.T) {
	c := &fakeLifecycleClient{scripted: []scriptedResp{
		{outcome: s3_lifecycle_pb.LifecycleDeleteOutcome_DONE},
	}}
	out, err := dispatchWithRetryFast(t, context.Background(), c, sampleMatch())
	require.NoError(t, err)
	assert.Equal(t, s3_lifecycle_pb.LifecycleDeleteOutcome_DONE, out)
	assert.Equal(t, int32(1), c.calls)
}

func TestDispatch_TransportRetryThenSucceed(t *testing.T) {
	// Two transport flakes then a success. The retry loop tries 3 times
	// by default; we expect 3 calls and the final outcome.
	c := &fakeLifecycleClient{scripted: []scriptedResp{
		{err: errors.New("transport boom")},
		{err: errors.New("transport boom")},
		{outcome: s3_lifecycle_pb.LifecycleDeleteOutcome_NOOP_RESOLVED},
	}}
	out, err := dispatchWithRetryFast(t, context.Background(), c, sampleMatch())
	require.NoError(t, err)
	assert.Equal(t, s3_lifecycle_pb.LifecycleDeleteOutcome_NOOP_RESOLVED, out)
	assert.Equal(t, int32(3), c.calls)
}

func TestDispatch_ExhaustsRetriesReturnsError(t *testing.T) {
	// All N attempts fail with transport errors. Caller must see an
	// error and the call count must equal transportRetryAttempts.
	failing := errors.New("transport persistent")
	c := &fakeLifecycleClient{scripted: []scriptedResp{
		{err: failing}, {err: failing}, {err: failing},
	}}
	_, err := dispatchWithRetryFast(t, context.Background(), c, sampleMatch())
	require.Error(t, err)
	assert.Equal(t, int32(transportRetryAttempts), c.calls)
}

func TestDispatch_ServerOutcomeRetryLaterIsNotRetried(t *testing.T) {
	// RETRY_LATER is a SERVER outcome; the daily run's halt-on-failure
	// caller handles it. dispatchWithRetry must surface it on the first
	// successful RPC and NOT retry in-run.
	c := &fakeLifecycleClient{scripted: []scriptedResp{
		{outcome: s3_lifecycle_pb.LifecycleDeleteOutcome_RETRY_LATER},
	}}
	out, err := dispatchWithRetryFast(t, context.Background(), c, sampleMatch())
	require.NoError(t, err)
	assert.Equal(t, s3_lifecycle_pb.LifecycleDeleteOutcome_RETRY_LATER, out)
	assert.Equal(t, int32(1), c.calls, "server outcome must not trigger transport retry")
}

func TestDispatch_ServerOutcomeBlockedNotRetried(t *testing.T) {
	c := &fakeLifecycleClient{scripted: []scriptedResp{
		{outcome: s3_lifecycle_pb.LifecycleDeleteOutcome_BLOCKED},
	}}
	out, err := dispatchWithRetryFast(t, context.Background(), c, sampleMatch())
	require.NoError(t, err)
	assert.Equal(t, s3_lifecycle_pb.LifecycleDeleteOutcome_BLOCKED, out)
	assert.Equal(t, int32(1), c.calls)
}

func TestDispatch_ContextCancelledShortCircuits(t *testing.T) {
	// Context cancellation must surface immediately, not consume the
	// retry budget. Cursor-staying semantics in the caller depends on
	// this distinction.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c := &fakeLifecycleClient{scripted: []scriptedResp{
		{err: context.Canceled},
	}}
	_, err := dispatchWithRetryFast(t, ctx, c, sampleMatch())
	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, int32(1), c.calls, "context cancellation must not consume the retry budget")
}

func TestBuildDeleteRequest_RuleHashAndIdentity(t *testing.T) {
	// Pin the request shape so a future Match-struct refactor doesn't
	// silently drop the identity-CAS witness or the rule hash slice.
	m := router.Match{
		Bucket:    "bucket",
		ObjectKey: "obj.txt",
		VersionID: "v_1",
		Key: s3lifecycle.ActionKey{
			Bucket:     "bucket",
			RuleHash:   [8]byte{1, 2, 3, 4, 5, 6, 7, 8},
			ActionKind: s3lifecycle.ActionKindNoncurrentDays,
		},
		Identity: &router.EntryIdentity{
			MtimeNs:      time.Unix(1700000000, 12345).UnixNano(),
			Size:         42,
			HeadFid:      "1,abc",
			ExtendedHash: []byte{0xaa, 0xbb},
		},
	}
	req := buildDeleteRequest(m)
	assert.Equal(t, "bucket", req.Bucket)
	assert.Equal(t, "obj.txt", req.ObjectPath)
	assert.Equal(t, "v_1", req.VersionId)
	assert.Equal(t, []byte{1, 2, 3, 4, 5, 6, 7, 8}, req.RuleHash)
	assert.Equal(t, s3_lifecycle_pb.ActionKind_NONCURRENT_DAYS, req.ActionKind)
	require.NotNil(t, req.ExpectedIdentity)
	assert.Equal(t, int64(42), req.ExpectedIdentity.Size)
	assert.Equal(t, "1,abc", req.ExpectedIdentity.HeadFid)
	assert.Equal(t, []byte{0xaa, 0xbb}, req.ExpectedIdentity.ExtendedHash)
}
