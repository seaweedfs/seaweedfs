package shell

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type fakeDiscoveryServer struct {
	master_pb.UnimplementedSeaweedServer
	leader  string
	filers  atomic.Int32
	listErr atomic.Value
}

func (s *fakeDiscoveryServer) KeepConnected(stream master_pb.Seaweed_KeepConnectedServer) error {
	if _, err := stream.Recv(); err != nil {
		return err
	}
	if err := stream.Send(&master_pb.KeepConnectedResponse{
		VolumeLocation: &master_pb.VolumeLocation{Leader: s.leader},
	}); err != nil {
		return err
	}
	<-stream.Context().Done()
	return nil
}

func (s *fakeDiscoveryServer) ListClusterNodes(context.Context, *master_pb.ListClusterNodesRequest) (*master_pb.ListClusterNodesResponse, error) {
	if err, _ := s.listErr.Load().(error); err != nil {
		return nil, err
	}
	resp := &master_pb.ListClusterNodesResponse{}
	for i := int32(0); i < s.filers.Load(); i++ {
		resp.ClusterNodes = append(resp.ClusterNodes, &master_pb.ListClusterNodesResponse_ClusterNode{
			Address: fmt.Sprintf("127.0.0.1:88%02d", i),
		})
	}
	return resp, nil
}

func startFakeDiscoveryMaster(t *testing.T, srv *fakeDiscoveryServer) string {
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
	return fmt.Sprintf("127.0.0.1:0.%s", port)
}

func discoveryEnv(addr string, timeout time.Duration) (*CommandEnv, context.CancelFunc) {
	masters := addr
	filerGroup := ""
	options := &ShellOptions{
		Masters:        &masters,
		GrpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
		FilerGroup:     &filerGroup,
		Directory:      "/",
		ConnectTimeout: timeout,
	}
	ce := NewCommandEnv(options)
	ctx, cancel := context.WithCancel(context.Background())
	go ce.MasterClient.KeepConnectedToMaster(ctx)
	ce.MasterClient.WaitUntilConnected(ctx)
	return ce, cancel
}

func TestFilerDiscoveryRetriesUntilFilerAppears(t *testing.T) {
	srv := &fakeDiscoveryServer{}
	addr := startFakeDiscoveryMaster(t, srv)
	srv.leader = addr

	ce, cancel := discoveryEnv(addr, 10*time.Second)
	defer cancel()

	go func() {
		time.Sleep(1200 * time.Millisecond)
		srv.filers.Store(1)
	}()

	got := ce.FilerAddress()
	if got != "127.0.0.1:8800" {
		t.Fatalf("expected the late-registering filer, got %q", got)
	}
}

func TestFilerDiscoveryReturnsEmptyAtDeadline(t *testing.T) {
	srv := &fakeDiscoveryServer{}
	addr := startFakeDiscoveryMaster(t, srv)
	srv.leader = addr

	ce, cancel := discoveryEnv(addr, 1500*time.Millisecond)
	defer cancel()

	start := time.Now()
	if got := ce.FilerAddress(); got != "" {
		t.Fatalf("expected no filer, got %q", got)
	}
	if elapsed := time.Since(start); elapsed < time.Second {
		t.Fatalf("discovery returned after %v, expected it to poll until the deadline", elapsed)
	}
}

func TestFilerDiscoveryRetriesAfterEmptyResult(t *testing.T) {
	srv := &fakeDiscoveryServer{}
	addr := startFakeDiscoveryMaster(t, srv)
	srv.leader = addr

	ce, cancel := discoveryEnv(addr, 1500*time.Millisecond)
	defer cancel()

	if got := ce.FilerAddress(); got != "" {
		t.Fatalf("expected no filer, got %q", got)
	}
	srv.filers.Store(1)
	if got := ce.FilerAddress(); got != "127.0.0.1:8800" {
		t.Fatalf("expected the late-registered filer on retry, got %q", got)
	}
}

func TestFilerDiscoveryStopsOnNonTransientError(t *testing.T) {
	srv := &fakeDiscoveryServer{}
	addr := startFakeDiscoveryMaster(t, srv)
	srv.leader = addr
	srv.listErr.Store(status.Error(codes.PermissionDenied, "denied"))

	ce, cancel := discoveryEnv(addr, 30*time.Second)
	defer cancel()

	start := time.Now()
	if got := ce.FilerAddress(); got != "" {
		t.Fatalf("expected no filer, got %q", got)
	}
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Fatalf("discovery kept polling a non-transient error for %v", elapsed)
	}
}

func TestFilerAddressConfiguredSkipsDiscovery(t *testing.T) {
	ce := &CommandEnv{option: &ShellOptions{FilerAddress: "127.0.0.1:8888"}}
	if got := ce.FilerAddress(); got != "127.0.0.1:8888" {
		t.Fatalf("expected the configured filer, got %q", got)
	}
}
