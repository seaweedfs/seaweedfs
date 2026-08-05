package reader

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// stalledStream stays open and never delivers — the filer answering
// keepalive pings while its handler has stopped producing.
type stalledStream struct {
	grpc.ClientStream
	ctx context.Context
}

func (s *stalledStream) Recv() (*filer_pb.SubscribeMetadataResponse, error) {
	<-s.ctx.Done()
	return nil, s.ctx.Err()
}

type stalledClient struct {
	filer_pb.SeaweedFilerClient
	req *filer_pb.SubscribeMetadataRequest
}

func (c *stalledClient) SubscribeMetadata(ctx context.Context, req *filer_pb.SubscribeMetadataRequest, _ ...grpc.CallOption) (filer_pb.SeaweedFiler_SubscribeMetadataClient, error) {
	c.req = req
	return &stalledStream{ctx: ctx}, nil
}

func TestRun_ReceiveTimeoutOnStalledStream(t *testing.T) {
	client := &stalledClient{}
	r := &Reader{
		ShardPredicate: func(int) bool { return true },
		BucketsPath:    "/buckets",
		Events:         make(chan *Event, 1),
		ReceiveTimeout: 50 * time.Millisecond,
	}

	done := make(chan error, 1)
	go func() { done <- r.Run(context.Background(), client, "test", 1) }()
	select {
	case err := <-done:
		require.ErrorIs(t, err, ErrReceiveTimeout)
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not time out on a stalled stream")
	}
	assert.True(t, client.req.ClientSupportsIdleHeartbeat,
		"a reader with a receive timeout must ask for heartbeats, or a caught-up stream looks stalled")
}

// heartbeatStream delivers only idle heartbeats: responses carrying a
// timestamp and no EventNotification.
type heartbeatStream struct {
	grpc.ClientStream
	every time.Duration
	sent  int
	max   int
}

func (s *heartbeatStream) Recv() (*filer_pb.SubscribeMetadataResponse, error) {
	if s.sent >= s.max {
		return nil, context.Canceled
	}
	time.Sleep(s.every)
	s.sent++
	return &filer_pb.SubscribeMetadataResponse{TsNs: int64(s.sent)}, nil
}

type heartbeatClient struct {
	filer_pb.SeaweedFilerClient
	stream *heartbeatStream
}

func (c *heartbeatClient) SubscribeMetadata(_ context.Context, _ *filer_pb.SubscribeMetadataRequest, _ ...grpc.CallOption) (filer_pb.SeaweedFiler_SubscribeMetadataClient, error) {
	return c.stream, nil
}

// TestRun_HeartbeatsHoldTheStreamOpen is the reason the timeout is safe
// to set at all: a caught-up stream keeps arriving as heartbeats, so the
// watchdog only fires on real silence, not on an idle cluster.
func TestRun_HeartbeatsHoldTheStreamOpen(t *testing.T) {
	stream := &heartbeatStream{every: 20 * time.Millisecond, max: 10}
	r := &Reader{
		ShardPredicate: func(int) bool { return true },
		BucketsPath:    "/buckets",
		Events:         make(chan *Event, 1),
		ReceiveTimeout: 200 * time.Millisecond,
	}

	done := make(chan error, 1)
	go func() { done <- r.Run(context.Background(), &heartbeatClient{stream: stream}, "test", 1) }()
	select {
	case err := <-done:
		assert.False(t, errors.Is(err, ErrReceiveTimeout),
			"heartbeats spanning more than one timeout window must keep the stream alive")
	case <-time.After(5 * time.Second):
		t.Fatal("Run never returned")
	}
	assert.Equal(t, 10, stream.sent, "every heartbeat should have been consumed")
}

func TestRun_RejectsNegativeReceiveTimeout(t *testing.T) {
	r := &Reader{
		ShardPredicate: func(int) bool { return true },
		BucketsPath:    "/buckets",
		Events:         make(chan *Event, 1),
		ReceiveTimeout: -time.Second,
	}
	err := r.Run(context.Background(), &stalledClient{}, "test", 1)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ReceiveTimeout")
}
