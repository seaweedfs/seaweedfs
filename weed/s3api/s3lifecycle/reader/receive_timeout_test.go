package reader

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type blockingSubscribeClient struct {
	filer_pb.SeaweedFilerClient
	request *filer_pb.SubscribeMetadataRequest
}

func (c *blockingSubscribeClient) SubscribeMetadata(ctx context.Context, req *filer_pb.SubscribeMetadataRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[filer_pb.SubscribeMetadataResponse], error) {
	c.request = req
	return &blockingSubscribeStream{ctx: ctx}, nil
}

type blockingSubscribeStream struct {
	grpc.ServerStreamingClient[filer_pb.SubscribeMetadataResponse]
	ctx context.Context
}

func (s *blockingSubscribeStream) Recv() (*filer_pb.SubscribeMetadataResponse, error) {
	<-s.ctx.Done()
	return nil, s.ctx.Err()
}

func TestRunTimesOutHalfOpenSubscription(t *testing.T) {
	client := &blockingSubscribeClient{}
	r := &Reader{
		ShardID:        0,
		BucketsPath:    "/buckets",
		UntilTsNs:      12345,
		Events:         make(chan *Event),
		ReceiveTimeout: 50 * time.Millisecond,
	}

	started := time.Now()
	err := r.Run(context.Background(), client, "test-lifecycle", 7)
	require.ErrorIs(t, err, ErrReceiveTimeout)
	assert.Less(t, time.Since(started), time.Second)
	require.NotNil(t, client.request)
	assert.Equal(t, int64(12345), client.request.UntilNs)
	assert.True(t, client.request.ClientSupportsIdleHeartbeat)
}
