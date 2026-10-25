package command

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/s3_pb"
)

type blockingIamCache struct {
	s3_pb.UnimplementedSeaweedS3IamCacheServer
	entered chan struct{}
	release chan struct{}
}

func (b *blockingIamCache) PutIdentity(ctx context.Context, _ *iam_pb.PutIdentityRequest) (*iam_pb.PutIdentityResponse, error) {
	close(b.entered)
	// Stop only cancels handler contexts; it cannot interrupt a handler that ignores them.
	select {
	case <-b.release:
		return &iam_pb.PutIdentityResponse{}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func startBlockingGrpc(t *testing.T) (grpcS *grpc.Server, release func(), rpcErr chan error) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	handler := &blockingIamCache{entered: make(chan struct{}), release: make(chan struct{})}
	grpcS = grpc.NewServer()
	s3_pb.RegisterSeaweedS3IamCacheServer(grpcS, handler)
	go grpcS.Serve(listener)
	t.Cleanup(grpcS.Stop)
	release = sync.OnceFunc(func() { close(handler.release) })
	t.Cleanup(release)

	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	rpcErr = make(chan error, 1)
	go func() {
		_, err := s3_pb.NewSeaweedS3IamCacheClient(conn).PutIdentity(context.Background(), &iam_pb.PutIdentityRequest{})
		rpcErr <- err
	}()
	<-handler.entered
	return grpcS, release, rpcErr
}

func TestGracefulStopGrpcWaitsForInFlightRPC(t *testing.T) {
	grpcS, release, rpcErr := startBlockingGrpc(t)

	stopped := make(chan struct{})
	go func() { gracefulStopGrpc(grpcS, 5*time.Second); close(stopped) }()
	select {
	case <-stopped:
		t.Fatal("gRPC stopped while an RPC was in flight")
	case <-time.After(100 * time.Millisecond):
	}
	release()
	if err := <-rpcErr; err != nil {
		t.Errorf("in-flight RPC failed: %v", err)
	}
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("gRPC graceful stop did not return after the RPC completed")
	}
}

func TestGracefulStopGrpcForcesStopAfterTimeout(t *testing.T) {
	grpcS, _, rpcErr := startBlockingGrpc(t)

	stopped := make(chan struct{})
	go func() { gracefulStopGrpc(grpcS, 50*time.Millisecond); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("gRPC graceful stop did not honour its timeout")
	}
	if err := <-rpcErr; err == nil {
		t.Error("in-flight RPC succeeded although the server was force-stopped")
	}
}
