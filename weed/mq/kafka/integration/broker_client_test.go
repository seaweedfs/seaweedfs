package integration

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
)

type stubMessagingClient struct {
	mq_pb.SeaweedMessagingClient
	calls   int
	failFor int
	err     error
}

func (s *stubMessagingClient) FindBrokerLeader(ctx context.Context, in *mq_pb.FindBrokerLeaderRequest, opts ...grpc.CallOption) (*mq_pb.FindBrokerLeaderResponse, error) {
	s.calls++
	if s.calls <= s.failFor {
		return nil, s.err
	}
	return &mq_pb.FindBrokerLeaderResponse{}, nil
}

func TestHealthCheckRetriesPastTransientBrokerErrors(t *testing.T) {
	stub := &stubMessagingClient{failFor: 2, err: errors.New("lock broker_balancer not found")}
	bc := &BrokerClient{ctx: context.Background(), client: stub}
	if err := bc.HealthCheck(); err != nil || stub.calls != 3 {
		t.Fatalf("err %v after %d calls, want success on the 3rd", err, stub.calls)
	}
}

func TestHealthCheckGivesUpAtDeadline(t *testing.T) {
	defer func(orig time.Duration) { clusterNodeDiscoveryTimeout = orig }(clusterNodeDiscoveryTimeout)
	clusterNodeDiscoveryTimeout = 300 * time.Millisecond

	stub := &stubMessagingClient{failFor: 1 << 30, err: errors.New("lock broker_balancer not found")}
	bc := &BrokerClient{ctx: context.Background(), client: stub}
	if err := bc.HealthCheck(); err == nil || stub.calls < 2 {
		t.Fatalf("err %v after %d calls, want repeated retries then an error", err, stub.calls)
	}
}
