package dailyrun

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/engine"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/reader"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type eofSubscribeClient struct {
	filer_pb.SeaweedFilerClient
	request *filer_pb.SubscribeMetadataRequest
}

func (c *eofSubscribeClient) SubscribeMetadata(_ context.Context, req *filer_pb.SubscribeMetadataRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[filer_pb.SubscribeMetadataResponse], error) {
	c.request = req
	return &eofSubscribeStream{}, nil
}

type eofSubscribeStream struct {
	grpc.ServerStreamingClient[filer_pb.SubscribeMetadataResponse]
}

func (*eofSubscribeStream) Recv() (*filer_pb.SubscribeMetadataResponse, error) {
	return nil, io.EOF
}

func TestSharedSubscriptionEOFStopsFanout(t *testing.T) {
	client := &eofSubscribeClient{}
	runNow := time.Unix(0, 987654321)
	_, readerDone, fanoutDone, cancel := startSharedSubscription(context.Background(), Config{
		Shards:                     []int{0},
		BucketsPath:                "/buckets",
		FilerClient:                client,
		SubscriptionReceiveTimeout: time.Second,
	}, runNow, 123)
	defer cancel()

	select {
	case <-fanoutDone:
	case <-time.After(time.Second):
		t.Fatal("fan-out stayed blocked after the metadata reader reached EOF")
	}
	require.NoError(t, <-readerDone)
	require.NotNil(t, client.request)
	assert.Equal(t, runNow.UnixNano(), client.request.UntilNs)
	assert.Equal(t, int64(123), client.request.SinceNs)
	assert.True(t, client.request.ClientSupportsIdleHeartbeat)
}

func TestSharedSubscriptionReceiveTimeoutStopsFanout(t *testing.T) {
	client := &blockingDailySubscribeClient{}
	_, readerDone, fanoutDone, cancel := startSharedSubscription(context.Background(), Config{
		Shards:                     []int{0},
		BucketsPath:                "/buckets",
		FilerClient:                client,
		SubscriptionReceiveTimeout: 50 * time.Millisecond,
	}, time.Now(), 123)
	defer cancel()

	select {
	case <-fanoutDone:
	case <-time.After(time.Second):
		t.Fatal("fan-out stayed blocked after the metadata receive timeout")
	}
	require.ErrorIs(t, <-readerDone, reader.ErrReceiveTimeout)
}

func TestRunReturnsSharedSubscriptionReceiveTimeout(t *testing.T) {
	eng := engine.New()
	eng.Compile([]engine.CompileInput{{
		Bucket: "bucket",
		Rules: []*s3lifecycle.Rule{{
			ID: "expire", Status: s3lifecycle.StatusEnabled, ExpirationDays: 1,
		}},
	}}, engine.CompileOptions{})
	for _, action := range eng.Snapshot().AllActions() {
		eng.Snapshot().MarkActive(action.Key)
	}

	cfg := validatableConfig()
	cfg.Shards = []int{0}
	cfg.Engine = eng
	cfg.FilerClient = &blockingDailySubscribeClient{}
	cfg.SubscriptionReceiveTimeout = 50 * time.Millisecond

	err := Run(context.Background(), cfg)
	require.ErrorIs(t, err, reader.ErrReceiveTimeout)
}

type blockingDailySubscribeClient struct {
	filer_pb.SeaweedFilerClient
}

func (*blockingDailySubscribeClient) SubscribeMetadata(ctx context.Context, _ *filer_pb.SubscribeMetadataRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[filer_pb.SubscribeMetadataResponse], error) {
	return &blockingDailySubscribeStream{ctx: ctx}, nil
}

type blockingDailySubscribeStream struct {
	grpc.ServerStreamingClient[filer_pb.SubscribeMetadataResponse]
	ctx context.Context
}

func (s *blockingDailySubscribeStream) Recv() (*filer_pb.SubscribeMetadataResponse, error) {
	<-s.ctx.Done()
	return nil, s.ctx.Err()
}
