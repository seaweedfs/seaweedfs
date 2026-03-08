package wdclient

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

// fakeLookupServer returns Unavailable for the first N calls, then succeeds.
type fakeLookupServer struct {
	master_pb.UnimplementedSeaweedServer
	unavailableCount int32
	callCount        atomic.Int32
}

func (s *fakeLookupServer) LookupVolume(_ context.Context, req *master_pb.LookupVolumeRequest) (*master_pb.LookupVolumeResponse, error) {
	n := s.callCount.Add(1)
	if n <= s.unavailableCount {
		return nil, status.Errorf(codes.Unavailable, "master is warming up")
	}
	resp := &master_pb.LookupVolumeResponse{}
	for _, vid := range req.VolumeOrFileIds {
		resp.VolumeIdLocations = append(resp.VolumeIdLocations, &master_pb.LookupVolumeResponse_VolumeIdLocation{
			VolumeOrFileId: vid,
			Locations: []*master_pb.Location{
				{Url: "127.0.0.1:8080", PublicUrl: "127.0.0.1:8080"},
			},
		})
	}
	return resp, nil
}

func startFakeMasterServer(t *testing.T, srv master_pb.SeaweedServer) pb.ServerAddress {
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
	return pb.ServerAddress(fmt.Sprintf("127.0.0.1:0.%s", port))
}

func TestLookupVolumeIdsRetriesOnUnavailable(t *testing.T) {
	srv := &fakeLookupServer{unavailableCount: 3}
	addr := startFakeMasterServer(t, srv)

	mc := NewMasterClient(
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		"", "test", "", "", "",
		pb.ServerDiscovery{},
	)
	mc.setCurrentMaster(addr)
	mc.grpcTimeout = 5 * time.Second

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	provider := &masterVolumeProvider{masterClient: mc}
	result, err := provider.LookupVolumeIds(ctx, []string{"1"})

	if err != nil {
		t.Fatalf("expected success after retries, got error: %v", err)
	}
	if _, ok := result["1"]; !ok {
		t.Error("expected volume 1 in result")
	}
	if calls := srv.callCount.Load(); calls != 4 {
		t.Errorf("expected 4 calls (3 unavailable + 1 success), got %d", calls)
	}
}

func TestLookupVolumeIdsStopsOnContextCancel(t *testing.T) {
	srv := &fakeLookupServer{unavailableCount: 1000}
	addr := startFakeMasterServer(t, srv)

	mc := NewMasterClient(
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		"", "test", "", "", "",
		pb.ServerDiscovery{},
	)
	mc.setCurrentMaster(addr)
	mc.grpcTimeout = 5 * time.Second

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	provider := &masterVolumeProvider{masterClient: mc}
	start := time.Now()
	_, err := provider.LookupVolumeIds(ctx, []string{"1"})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected error from context cancellation")
	}
	if elapsed > 5*time.Second {
		t.Errorf("took %v, expected to stop near context deadline of 2s", elapsed)
	}
}
