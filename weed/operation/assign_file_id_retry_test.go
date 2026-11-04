package operation

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
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
	// Verify the loop actually retried (not just an immediate failure)
	if calls := srv.callCount.Load(); calls <= 1 {
		t.Errorf("expected multiple retry attempts, got %d calls", calls)
	}
	// Verify the error is from context deadline
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected context.DeadlineExceeded, got: %v", err)
	}
}

// warmingAssignServer refuses Assign until warmEnd, like a master in warm-up,
// telling the client the remaining warm-up.
type warmingAssignServer struct {
	master_pb.UnimplementedSeaweedServer
	warmEnd   time.Time
	callCount atomic.Int32
}

func (s *warmingAssignServer) Assign(_ context.Context, _ *master_pb.AssignRequest) (*master_pb.AssignResponse, error) {
	s.callCount.Add(1)
	if remaining := time.Until(s.warmEnd); remaining > 0 {
		st, _ := status.New(codes.Unavailable, "master is warming up").WithDetails(&errdetails.RetryInfo{RetryDelay: durationpb.New(remaining)})
		return nil, st.Err()
	}
	return &master_pb.AssignResponse{Fid: "1,abc", Count: 1, Location: &master_pb.Location{Url: "127.0.0.1:8080", PublicUrl: "127.0.0.1:8080"}}, nil
}

func TestAssignRetryLandsAtWarmupEndWithHint(t *testing.T) {
	const warmup = 2600 * time.Millisecond
	srv := &warmingAssignServer{}
	addr := startFakeServer(t, srv)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	start := time.Now()
	srv.warmEnd = start.Add(warmup)
	if _, err := Assign(ctx, func(_ context.Context) pb.ServerAddress { return addr },
		grpc.WithTransportCredentials(insecure.NewCredentials()), &VolumeAssignRequest{Count: 1}); err != nil {
		t.Fatalf("Assign: %v", err)
	}
	// The hint lands the retry just after warm-up ends (2.6s), not at the
	// next backoff step (1s + 1.5s + 2.25s = 4.75s).
	if elapsed := time.Since(start); elapsed > 4500*time.Millisecond {
		t.Fatalf("assign took %v over %d calls, want about 2.75s (warm-up end)", elapsed, srv.callCount.Load())
	}
	if calls := srv.callCount.Load(); calls < 2 {
		t.Fatalf("got %d calls, want a refused call during warm-up before the retry", calls)
	}
}
