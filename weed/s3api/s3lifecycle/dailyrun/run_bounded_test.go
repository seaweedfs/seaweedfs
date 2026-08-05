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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// idleSubscribeStream: nothing to deliver past the pass boundary. A
// bounded subscription gets EOF; an unbounded one blocks forever.
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

// The reported wedge: the pass ended only when an unrelated write pushed
// an event past its boundary, so a quiet cluster starved every drain and
// Run's WaitGroup never reached zero.
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

// Flip side of closing the event channel: the drains now end cleanly on
// a dead subscription, so the pass would otherwise go green having
// processed nothing.
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

// blockingSubscribeStream never delivers; only a cap ends the pass.
type blockingSubscribeStream struct {
	grpc.ClientStream
	ctx context.Context
}

func (s *blockingSubscribeStream) Recv() (*filer_pb.SubscribeMetadataResponse, error) {
	<-s.ctx.Done()
	// A canceled gRPC stream returns a status code, not context.Canceled.
	return nil, status.Error(codes.Canceled, "context canceled")
}

type blockingSubscribeClient struct {
	filer_pb.SeaweedFilerClient
}

func (c *blockingSubscribeClient) SubscribeMetadata(ctx context.Context, _ *filer_pb.SubscribeMetadataRequest, _ ...grpc.CallOption) (filer_pb.SeaweedFiler_SubscribeMetadataClient, error) {
	return &blockingSubscribeStream{ctx: ctx}, nil
}

// Counterweight to TestRun_ServerSideCancelFailsThePass: same
// codes.Canceled from the reader, opposite verdict. Only intent
// separates them, which is why the decision reads ctx not the error.
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

// Neither UntilNs nor keepalive reaches this: a healthy connection whose
// filer has stopped producing.
func TestRun_StalledSubscriptionTimesOutThePass(t *testing.T) {
	e := engine.New()
	e.Compile([]engine.CompileInput{
		{Bucket: "b1", Rules: []*s3lifecycle.Rule{
			{ID: "r", Status: s3lifecycle.StatusEnabled, ExpirationDays: 30},
		}},
	}, engine.CompileOptions{})

	cfg := Config{
		Shards:                     []int{0, 1},
		BucketsPath:                "/buckets",
		Engine:                     e,
		FilerClient:                &blockingSubscribeClient{},
		Client:                     stubLifecycleClient{},
		Persister:                  newMemPersister(),
		Lister:                     stubSiblingLister{},
		SubscriptionReceiveTimeout: 50 * time.Millisecond,
	}

	done := make(chan error, 1)
	go func() { done <- Run(context.Background(), cfg) }()
	select {
	case err := <-done:
		require.ErrorIs(t, err, reader.ErrReceiveTimeout)
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return on a stalled subscription")
	}
}

func TestValidate_RejectsNegativeSubscriptionReceiveTimeout(t *testing.T) {
	cfg := validatableConfig()
	cfg.SubscriptionReceiveTimeout = -time.Second
	require.ErrorContains(t, validate(cfg), "SubscriptionReceiveTimeout")
	cfg.SubscriptionReceiveTimeout = 0
	require.NoError(t, validate(cfg))
}

// slowSavePersister stretches teardown past the caller's deadline.
type slowSavePersister struct {
	*memPersister
	delay time.Duration
}

func (p *slowSavePersister) Save(ctx context.Context, shardID int, c Cursor) error {
	time.Sleep(p.delay)
	return p.memPersister.Save(ctx, shardID, c)
}

// A reader that fails while the caller's deadline is still live, on a
// pass whose teardown then outlives that deadline. Sampling ctx.Err()
// after the drains and cursor saves would read the late deadline as
// intent and report the truncated pass as a success.
func TestRun_LateDeadlineDoesNotMaskReaderFailure(t *testing.T) {
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
		Persister:   &slowSavePersister{memPersister: newMemPersister(), delay: 300 * time.Millisecond},
		Lister:      stubSiblingLister{},
	}

	// Deadline lands during the cursor saves, well after the reader failed.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfg) }()
	select {
	case err := <-done:
		require.Error(t, err, "a deadline expiring during teardown must not excuse an earlier reader failure")
		require.NotNil(t, ctx.Err(), "test is meaningless unless the deadline did expire")
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return")
	}
}

// The case status-code classification could not express: the filer
// cancels while the caller's context is untouched.
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
