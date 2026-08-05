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
