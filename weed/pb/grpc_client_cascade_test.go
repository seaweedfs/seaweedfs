package pb

import (
	"context"
	"net"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// cascadeFilerServer answers a "slow" lookup only once its caller is known to be
// in flight, so a second caller can fail while the first still holds the shared
// cached ClientConn.
type cascadeFilerServer struct {
	filer_pb.UnimplementedSeaweedFilerServer
	inFlight  chan struct{}
	release   chan struct{}
	closeOnce sync.Once
}

func (s *cascadeFilerServer) LookupDirectoryEntry(ctx context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	if req.Name == "slow" {
		s.closeOnce.Do(func() { close(s.inFlight) })
		select {
		case <-s.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return &filer_pb.LookupDirectoryEntryResponse{}, nil
}

// startCascadeFiler serves a fake filer and resets the process-wide connection
// cache so the test owns the shared ClientConn under exercise.
func startCascadeFiler(t *testing.T) (*cascadeFilerServer, string, grpc.DialOption) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	fake := &cascadeFilerServer{inFlight: make(chan struct{}), release: make(chan struct{})}
	server := grpc.NewServer()
	filer_pb.RegisterSeaweedFilerServer(server, fake)
	go server.Serve(listener)

	grpcClientsLock.Lock()
	previous := grpcClients
	grpcClients = make(map[string]*versionedGrpcClient)
	grpcClientsLock.Unlock()
	t.Cleanup(func() {
		server.Stop()
		grpcClientsLock.Lock()
		grpcClients = previous
		grpcClientsLock.Unlock()
	})

	return fake, listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials())
}

// TestWithGrpcClient_AbandonedRequestKeepsSharedConnection reproduces
// seaweedfs#10947 end to end: an S3-style caller that hands WithGrpcClient a
// context.Background() while its RPC rides the (now abandoned) HTTP request
// context used to close the shared filer ClientConn, killing every concurrent
// upload on it with "the client connection is closing".
func TestWithGrpcClient_AbandonedRequestKeepsSharedConnection(t *testing.T) {
	fake, address, dialOption := startCascadeFiler(t)

	var concurrentErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		concurrentErr = WithGrpcClient(context.Background(), false, 0, func(connection *grpc.ClientConn) error {
			_, err := filer_pb.NewSeaweedFilerClient(connection).LookupDirectoryEntry(context.Background(),
				&filer_pb.LookupDirectoryEntryRequest{Directory: "/buckets/b", Name: "slow"})
			return err
		}, address, false, dialOption)
	}()
	<-fake.inFlight

	abandoned, cancel := context.WithCancel(context.Background())
	cancel()
	if err := WithGrpcClient(context.Background(), false, 0, func(connection *grpc.ClientConn) error {
		_, err := filer_pb.NewSeaweedFilerClient(connection).LookupDirectoryEntry(abandoned,
			&filer_pb.LookupDirectoryEntryRequest{Directory: "/buckets/b", Name: "fast"})
		return err
	}, address, false, dialOption); err == nil {
		t.Fatal("the abandoned request should have failed")
	}

	close(fake.release)
	wg.Wait()
	if concurrentErr != nil {
		t.Fatalf("concurrent caller must survive an unrelated abandoned request: %v", concurrentErr)
	}
}
