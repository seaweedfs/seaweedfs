package dailyrun

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/s3_lifecycle_pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDispatch_UnavailableThenSuccess(t *testing.T) {
	c := &fakeLifecycleClient{scripted: []scriptedResp{
		{err: status.Error(codes.Unavailable, "connection error")},
		{outcome: s3_lifecycle_pb.LifecycleDeleteOutcome_DONE},
	}}
	out, err := dispatchWithRetry(context.Background(), c, sampleMatch(), 0)
	require.NoError(t, err)
	assert.Equal(t, s3_lifecycle_pb.LifecycleDeleteOutcome_DONE, out)
	assert.Equal(t, int32(2), c.calls)
}

func TestDispatch_PermanentErrorsAreNotRetried(t *testing.T) {
	for _, code := range []codes.Code{codes.Unauthenticated, codes.PermissionDenied, codes.InvalidArgument, codes.Unimplemented} {
		t.Run(code.String(), func(t *testing.T) {
			c := &fakeLifecycleClient{scripted: []scriptedResp{{err: status.Error(code, "rejected")}}}
			_, err := dispatchWithRetry(context.Background(), c, sampleMatch(), 0)
			require.Error(t, err)
			assert.Equal(t, code, status.Code(err))
			assert.Equal(t, int32(1), c.calls)
		})
	}
}

func TestDispatch_NilResponseIsAnErrorNotAPanic(t *testing.T) {
	c := &walkerStubClient{nilResp: true}
	_, err := dispatchWithRetry(context.Background(), c, sampleMatch(), 0)
	require.ErrorIs(t, err, errNilDeleteResponse)
}
