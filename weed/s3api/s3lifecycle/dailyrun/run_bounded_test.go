package dailyrun

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// idleSubscribeStream models the filer's SubscribeMetadata stream on a
// cluster with no writes past the pass boundary: everything up to
// UntilNs has been delivered (here: nothing), so a bounded subscription
// ends with io.EOF and an unbounded one blocks in Recv forever.
type idleSubscribeStream struct {
	grpc.ClientStream
	ctx     context.Context
	untilNs int64
}

func (s *idleSubscribeStream) Recv() (*filer_pb.SubscribeMetadataResponse, error) {
	if s.untilNs != 0 {
		return nil, io.EOF
	}
	<-s.ctx.Done()
	return nil, s.ctx.Err()
}

type idleSubscribeClient struct {
	filer_pb.SeaweedFilerClient
	gotReq chan *filer_pb.SubscribeMetadataRequest
}

func (c *idleSubscribeClient) SubscribeMetadata(ctx context.Context, req *filer_pb.SubscribeMetadataRequest, _ ...grpc.CallOption) (filer_pb.SeaweedFiler_SubscribeMetadataClient, error) {
	select {
	case c.gotReq <- req:
	default:
	}
	return &idleSubscribeStream{ctx: ctx, untilNs: req.UntilNs}, nil
}

// TestRun_EndsOnIdleSubscription is the regression for a daily-replay
// pass that never returned: the pass only ended when an unrelated write
// pushed a meta-log event past its boundary, so on a quiet cluster the
// reader parked in Recv, all 16 shard drains starved, and Run's
// WaitGroup never reached zero. Bounding the subscription at the pass
// boundary makes the end of the pass the filer's decision, not the
// cluster's write traffic.
func TestRun_EndsOnIdleSubscription(t *testing.T) {
	e := engine.New()
	e.Compile([]engine.CompileInput{
		{Bucket: "b1", Rules: []*s3lifecycle.Rule{
			{ID: "r", Status: s3lifecycle.StatusEnabled, ExpirationDays: 30},
		}},
	}, engine.CompileOptions{})
	require.NotEqual(t, [32]byte{}, engine.ReplayContentHash(e.Snapshot()),
		"rule must be replay-eligible or Run skips the subscription entirely")

	client := &idleSubscribeClient{gotReq: make(chan *filer_pb.SubscribeMetadataRequest, 1)}
	runNow := time.Now().UTC()
	cfg := Config{
		Shards:      []int{0, 1, 2},
		BucketsPath: "/buckets",
		Engine:      e,
		FilerClient: client,
		Client:      stubLifecycleClient{},
		Persister:   newMemPersister(),
		Lister:      stubSiblingLister{},
		Now:         func() time.Time { return runNow },
	}

	done := make(chan error, 1)
	go func() { done <- Run(context.Background(), cfg) }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return on an idle subscription")
	}

	req := <-client.gotReq
	assert.Equal(t, runNow.UnixNano(), req.UntilNs, "subscription must stop at the pass boundary")
}

// failingSubscribeClient fails the subscribe call outright.
type failingSubscribeClient struct {
	filer_pb.SeaweedFilerClient
	err error
}

func (c *failingSubscribeClient) SubscribeMetadata(_ context.Context, _ *filer_pb.SubscribeMetadataRequest, _ ...grpc.CallOption) (filer_pb.SeaweedFiler_SubscribeMetadataClient, error) {
	return nil, c.err
}

// TestRun_ReaderFailureFailsThePass guards the flip side of closing the
// event channel when the reader exits: every shard drain now ends
// cleanly on a dead subscription, so without this the pass would report
// success and the job would go green while lifecycle processed nothing.
func TestRun_ReaderFailureFailsThePass(t *testing.T) {
	e := engine.New()
	e.Compile([]engine.CompileInput{
		{Bucket: "b1", Rules: []*s3lifecycle.Rule{
			{ID: "r", Status: s3lifecycle.StatusEnabled, ExpirationDays: 30},
		}},
	}, engine.CompileOptions{})

	cfg := Config{
		Shards:      []int{0, 1},
		BucketsPath: "/buckets",
		Engine:      e,
		FilerClient: &failingSubscribeClient{err: status.Error(codes.Unavailable, "filer down")},
		Client:      stubLifecycleClient{},
		Persister:   newMemPersister(),
		Lister:      stubSiblingLister{},
	}

	done := make(chan error, 1)
	go func() { done <- Run(context.Background(), cfg) }()
	select {
	case err := <-done:
		require.Error(t, err, "a dead subscription must not report a successful pass")
		assert.Contains(t, err.Error(), "subscription")
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after the subscription failed")
	}
}

// blockingSubscribeStream never delivers, so the pass can only end by
// the caller's wall-clock cap. Models the shell driver's -runtime.
type blockingSubscribeStream struct {
	grpc.ClientStream
	ctx context.Context
}

func (s *blockingSubscribeStream) Recv() (*filer_pb.SubscribeMetadataResponse, error) {
	<-s.ctx.Done()
	// What gRPC actually returns for a canceled stream: a status code,
	// not a wrapped context.Canceled.
	return nil, status.Error(codes.Canceled, "context canceled")
}

type blockingSubscribeClient struct {
	filer_pb.SeaweedFilerClient
}

func (c *blockingSubscribeClient) SubscribeMetadata(ctx context.Context, _ *filer_pb.SubscribeMetadataRequest, _ ...grpc.CallOption) (filer_pb.SeaweedFiler_SubscribeMetadataClient, error) {
	return &blockingSubscribeStream{ctx: ctx}, nil
}

// TestRun_CappedPassIsNotAFailure keeps a wall-clock-capped pass (the
// shell driver's -runtime) reporting success. It is the counterweight
// to TestRun_ReaderFailureFailsThePass: both end with the reader
// returning codes.Canceled, and only the caller's intent separates the
// truncated-on-purpose pass from the broken one — which is why the
// decision reads ctx rather than the error.
func TestRun_CappedPassIsNotAFailure(t *testing.T) {
	e := engine.New()
	e.Compile([]engine.CompileInput{
		{Bucket: "b1", Rules: []*s3lifecycle.Rule{
			{ID: "r", Status: s3lifecycle.StatusEnabled, ExpirationDays: 30},
		}},
	}, engine.CompileOptions{})

	cfg := Config{
		Shards:      []int{0, 1},
		BucketsPath: "/buckets",
		Engine:      e,
		FilerClient: &blockingSubscribeClient{},
		Client:      stubLifecycleClient{},
		Persister:   newMemPersister(),
		Lister:      stubSiblingLister{},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfg) }()
	select {
	case err := <-done:
		require.NoError(t, err, "a pass truncated by its own wall-clock cap is not a failure")
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after the pass was capped")
	}
}

// TestRun_ServerSideCancelFailsThePass is the case status-code
// classification could not express: the filer cancels the stream while
// the caller's context is untouched. Indistinguishable from the capped
// pass above by error alone, so a codes.Canceled check would report a
// truncated pass as a success.
func TestRun_ServerSideCancelFailsThePass(t *testing.T) {
	e := engine.New()
	e.Compile([]engine.CompileInput{
		{Bucket: "b1", Rules: []*s3lifecycle.Rule{
			{ID: "r", Status: s3lifecycle.StatusEnabled, ExpirationDays: 30},
		}},
	}, engine.CompileOptions{})

	cfg := Config{
		Shards:      []int{0, 1},
		BucketsPath: "/buckets",
		Engine:      e,
		FilerClient: &failingSubscribeClient{err: status.Error(codes.Canceled, "context canceled")},
		Client:      stubLifecycleClient{},
		Persister:   newMemPersister(),
		Lister:      stubSiblingLister{},
	}

	done := make(chan error, 1)
	go func() { done <- Run(context.Background(), cfg) }()
	select {
	case err := <-done:
		require.Error(t, err, "a peer-canceled stream truncates the pass and must not report success")
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after the subscription was canceled")
	}
}
