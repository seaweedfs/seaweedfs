package dailyrun

import (
	"context"
	"errors"
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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDispatch_UnavailableThenSuccess(t *testing.T) {
	c := &fakeLifecycleClient{scripted: []scriptedResp{
		{err: status.Error(codes.Unavailable, "connection error")},
		{outcome: s3_lifecycle_pb.LifecycleDeleteOutcome_DONE},
	}}
	out, err := dispatchWithRetry(context.Background(), c, nil, sampleMatch(), 0)
	require.NoError(t, err)
	assert.Equal(t, s3_lifecycle_pb.LifecycleDeleteOutcome_DONE, out)
	assert.Equal(t, int32(2), c.calls)
}

func TestDispatch_ServerDeadlineIsRetried(t *testing.T) {
	c := &fakeLifecycleClient{scripted: []scriptedResp{
		{err: status.Error(codes.DeadlineExceeded, "context deadline exceeded")},
		{outcome: s3_lifecycle_pb.LifecycleDeleteOutcome_DONE},
	}}
	out, err := dispatchWithRetry(context.Background(), c, nil, sampleMatch(), time.Minute)
	require.NoError(t, err)
	assert.Equal(t, s3_lifecycle_pb.LifecycleDeleteOutcome_DONE, out)
	assert.Equal(t, int32(2), c.calls)
}

func TestDispatch_PermanentErrorsAreNotRetried(t *testing.T) {
	for _, code := range []codes.Code{codes.Unauthenticated, codes.PermissionDenied, codes.InvalidArgument, codes.Unimplemented} {
		t.Run(code.String(), func(t *testing.T) {
			c := &fakeLifecycleClient{scripted: []scriptedResp{{err: status.Error(code, "rejected")}}}
			_, err := dispatchWithRetry(context.Background(), c, nil, sampleMatch(), 0)
			require.Error(t, err)
			assert.Equal(t, code, status.Code(err))
			assert.Equal(t, int32(1), c.calls)
		})
	}
}

func TestDispatch_NilResponseIsAnErrorNotAPanic(t *testing.T) {
	c := &walkerStubClient{nilResp: true}
	_, err := dispatchWithRetry(context.Background(), c, nil, sampleMatch(), 0)
	require.ErrorIs(t, err, errNilDeleteResponse)
}

func TestDispatch_EveryAttemptWaitsOnLimiter(t *testing.T) {
	lim := rate.NewLimiter(rate.Every(time.Hour), transportRetryAttempts)
	c := &fakeLifecycleClient{scripted: []scriptedResp{
		{err: status.Error(codes.Unavailable, "down")},
		{err: status.Error(codes.Unavailable, "down")},
		{outcome: s3_lifecycle_pb.LifecycleDeleteOutcome_DONE},
	}}
	_, err := dispatchWithRetry(context.Background(), c, lim, sampleMatch(), 0)
	require.NoError(t, err)
	assert.Equal(t, int32(transportRetryAttempts), c.calls)
	assert.Less(t, lim.Tokens(), 0.5, "each attempt must take a token from the shared budget")
}

func TestProcessMatches_RetryExhaustionHoldsCursorBeforeTheMatch(t *testing.T) {
	runNow := time.Now()
	first, second := sampleMatch(), sampleMatch()
	first.DueTime, second.DueTime = runNow.Add(-time.Hour), runNow.Add(-time.Hour)
	second.ObjectKey = "obj2"
	c := &fakeLifecycleClient{scripted: []scriptedResp{
		{outcome: s3_lifecycle_pb.LifecycleDeleteOutcome_DONE},
		{err: status.Error(codes.Unavailable, "down")},
	}}
	_, halted, err := processMatches(context.Background(), Config{Client: c}, runNow, &reader.Event{TsNs: runNow.UnixNano()}, []router.Match{first, second})
	require.Error(t, err)
	assert.True(t, halted)
	assert.Equal(t, int32(1+transportRetryAttempts), c.calls)
}

func TestWalkerDispatcher_TransientErrorIsRetried(t *testing.T) {
	c := &fakeLifecycleClient{scripted: []scriptedResp{
		{err: status.Error(codes.Unavailable, "connection reset")},
		{outcome: s3_lifecycle_pb.LifecycleDeleteOutcome_DONE},
	}}
	d := &WalkerDispatcher{Client: c}
	err := d.Delete(context.Background(), sampleAction(t, s3lifecycle.ActionKindExpirationDays), &bootstrap.Entry{Path: "obj"})
	require.NoError(t, err)
	assert.Equal(t, int32(2), c.calls)
}

func TestWalkerDispatcher_AuthErrorSurfacesAtOnce(t *testing.T) {
	c := &fakeLifecycleClient{scripted: []scriptedResp{{err: status.Error(codes.Unauthenticated, "invalid admin token")}}}
	d := &WalkerDispatcher{Client: c}
	err := d.Delete(context.Background(), sampleAction(t, s3lifecycle.ActionKindExpirationDays), &bootstrap.Entry{Path: "obj"})
	require.Error(t, err)
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
	assert.Equal(t, int32(1), c.calls)
}

func TestWalkerDispatcher_ParentCancelDuringBackoffStops(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	c := &fakeLifecycleClient{scripted: []scriptedResp{{err: errors.New("transport: connection refused")}}}
	time.AfterFunc(10*time.Millisecond, cancel)
	d := &WalkerDispatcher{Client: c}
	err := d.Delete(ctx, sampleAction(t, s3lifecycle.ActionKindExpirationDays), &bootstrap.Entry{Path: "obj"})
	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, int32(1), c.calls)
}
