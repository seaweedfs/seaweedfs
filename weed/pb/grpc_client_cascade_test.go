package pb

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

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
		grpcClientsLock.Lock()
		cached := grpcClients
		grpcClients = previous
		grpcClientsLock.Unlock()
		for _, connection := range cached {
			connection.Close()
		}
		server.Stop()
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

func (s *cascadeFilerServer) SubscribeMetadata(req *filer_pb.SubscribeMetadataRequest, stream filer_pb.SeaweedFiler_SubscribeMetadataServer) error {
	if !req.ClientSupportsMetadataChunks {
		return nil
	}
	return stream.Send(&filer_pb.SubscribeMetadataResponse{
		LogFileRefs: []*filer_pb.LogFileChunkRef{{
			FilerId:  "5319c0e8",
			FileTsNs: time.Now().UnixNano(),
			Chunks:   []*filer_pb.FileChunk{{FileId: "4471,ce598bfff8416e", Size: 64}},
		}},
	})
}

// TestWithGrpcClient_EndedStreamKeepsSharedConnection covers the other half of
// seaweedfs#10947: the S3 gateway follows filer metadata on a long-lived
// SubscribeMetadata stream and reconnects forever. Every ordinary end of that
// stream used to drop the shared filer ClientConn the request handlers use,
// firing the same burst of "the client connection is closing" failures.
func TestWithGrpcClient_EndedStreamKeepsSharedConnection(t *testing.T) {
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

	if err := WithGrpcClient(context.Background(), true, 0, func(connection *grpc.ClientConn) error {
		stream, err := filer_pb.NewSeaweedFilerClient(connection).SubscribeMetadata(context.Background(), &filer_pb.SubscribeMetadataRequest{})
		if err != nil {
			return err
		}
		_, err = stream.Recv()
		return err
	}, address, false, dialOption); err == nil {
		t.Fatal("the ended stream should have reported io.EOF")
	}

	close(fake.release)
	wg.Wait()
	if concurrentErr != nil {
		t.Fatalf("concurrent caller must survive an unrelated stream ending: %v", concurrentErr)
	}
}

// TestWithGrpcClient_LogChunkReadFailureKeepsSharedConnection covers a
// subscriber replaying persisted log chunks over HTTP: an unreachable volume
// server reports "connection refused" as the subscription's error, which used
// to drop the shared filer ClientConn and cancel the assign and upload RPCs
// riding on it.
func TestWithGrpcClient_LogChunkReadFailureKeepsSharedConnection(t *testing.T) {
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

	option := &MetadataFollowOption{
		ClientName:     "mount",
		PathPrefix:     "/",
		EventErrorType: DontLogError,
		LogFileReaderFn: func(chunks []*filer_pb.FileChunk) (io.ReadCloser, error) {
			// An unreachable volume server, as a filerProxy mount sees it.
			response, err := http.Get("http://127.0.0.1:1/" + chunks[0].FileId)
			if err != nil {
				return nil, err
			}
			return response.Body, nil
		},
	}
	subscribe := makeSubscribeMetadataFunc(option, func(resp *filer_pb.SubscribeMetadataResponse) error { return nil })
	err := WithGrpcClient(context.Background(), true, 0, func(connection *grpc.ClientConn) error {
		return subscribe(filer_pb.NewSeaweedFilerClient(connection))
	}, address, false, dialOption)
	if err == nil || !errors.Is(err, ErrLogFileRead) {
		t.Fatalf("the replay should have failed reading the log chunk: %v", err)
	}

	close(fake.release)
	wg.Wait()
	if concurrentErr != nil {
		t.Fatalf("concurrent caller must survive a log chunk read failure: %v", concurrentErr)
	}
}
