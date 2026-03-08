package operation

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// fakeAssignServer returns Unavailable for the first N calls, then succeeds.
type fakeAssignServer struct {
	master_pb.UnimplementedSeaweedServer
	unavailableCount int32
	callCount        atomic.Int32
}

func (s *fakeAssignServer) Assign(_ context.Context, _ *master_pb.AssignRequest) (*master_pb.AssignResponse, error) {
	n := s.callCount.Add(1)
	if n <= s.unavailableCount {
		return nil, status.Errorf(codes.Unavailable, "master is warming up")
	}
	return &master_pb.AssignResponse{
		Fid:   "1,abc",
		Count: 1,
		Location: &master_pb.Location{
			Url:       "127.0.0.1:8080",
			PublicUrl: "127.0.0.1:8080",
		},
	}, nil
}

func startFakeServer(t *testing.T, srv master_pb.SeaweedServer) pb.ServerAddress {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	master_pb.RegisterSeaweedServer(grpcServer, srv)
	go func() { _ = grpcServer.Serve(lis) }()
	t.Cleanup(grpcServer.GracefulStop)

	_, port, _ := net.SplitHostPort(lis.Addr().String())
	// Use "0.<grpcPort>" format so ToGrpcAddress() resolves to the actual port
	return pb.ServerAddress(fmt.Sprintf("127.0.0.1:0.%s", port))
}

func TestAssignRetriesOnUnavailable(t *testing.T) {
	srv := &fakeAssignServer{unavailableCount: 3}
	addr := startFakeServer(t, srv)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ret, err := Assign(ctx, func(_ context.Context) pb.ServerAddress {
		return addr
	}, grpc.WithTransportCredentials(insecure.NewCredentials()), &VolumeAssignRequest{Count: 1})

	if err != nil {
		t.Fatalf("expected success after retries, got error: %v", err)
	}
	if ret.Fid != "1,abc" {
		t.Errorf("expected fid '1,abc', got '%s'", ret.Fid)
	}
	if calls := srv.callCount.Load(); calls != 4 {
		t.Errorf("expected 4 calls (3 unavailable + 1 success), got %d", calls)
	}
}

func TestAssignStopsOnContextCancel(t *testing.T) {
	srv := &fakeAssignServer{unavailableCount: 1000} // never succeeds
	addr := startFakeServer(t, srv)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	start := time.Now()
	_, err := Assign(ctx, func(_ context.Context) pb.ServerAddress {
		return addr
	}, grpc.WithTransportCredentials(insecure.NewCredentials()), &VolumeAssignRequest{Count: 1})

	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected error from context cancellation")
	}
	// Should stop within a reasonable time after context deadline
	if elapsed > 5*time.Second {
		t.Errorf("took %v, expected to stop near context deadline of 2s", elapsed)
	}
}
