package mount

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// wedgedFiler accepts connections but never answers: LookupDirectoryEntry and
// StreamMutateEntry block until the transport dies. It stands in for a filer
// whose TCP connection black-holes instead of refusing.
type wedgedFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
}

func (s *wedgedFiler) LookupDirectoryEntry(ctx context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (s *wedgedFiler) StreamMutateEntry(stream filer_pb.SeaweedFiler_StreamMutateEntryServer) error {
	<-stream.Context().Done()
	return stream.Context().Err()
}

func listenFiler(t *testing.T, fake filer_pb.SeaweedFilerServer) pb.ServerAddress {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	server := grpc.NewServer()
	filer_pb.RegisterSeaweedFilerServer(server, fake)
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	return pb.NewServerAddressWithGrpcPort("127.0.0.1:1", listener.Addr().(*net.TCPAddr).Port)
}

// A wedged filer must not hold the walk hostage: the call is bounded and the
// walk reaches the next address.
func TestWithFilerClientFailsOverPastWedgedFiler(t *testing.T) {
	wfs := newInvalidateTestWFS(t)
	wfs.filerCallTimeout = 200 * time.Millisecond

	wedged := listenFiler(t, &wedgedFiler{})
	live := listenFiler(t, &fakeFilerServer{lookupSize: 42, lookupLogTsNs: 1000})
	wfs.option.FilerAddresses = []pb.ServerAddress{wedged, live}
	atomic.StoreInt32(&wfs.option.filerIndex, 0)

	var entry *filer_pb.Entry
	err := wfs.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		resp, err := filer_pb.LookupEntry(context.Background(), client, &filer_pb.LookupDirectoryEntryRequest{
			Directory: "/dir",
			Name:      "file",
		})
		if err != nil {
			return err
		}
		entry = resp.Entry
		return nil
	})
	if err != nil {
		t.Fatalf("WithFilerClient: %v", err)
	}
	if entry == nil || entry.Attributes.FileSize != 42 {
		t.Fatalf("entry = %+v, want lookup served by the live filer", entry)
	}
	if got := atomic.LoadInt32(&wfs.option.filerIndex); got != 1 {
		t.Fatalf("filerIndex = %d, want 1 (the live filer)", got)
	}
}

// A mutation stream whose peer never answers must surface as a transport
// error instead of hanging, and the sticky index must move off the dead filer
// so the next stream opens elsewhere.
func TestStreamMutateTimesOutAndRotatesFiler(t *testing.T) {
	wfs := newInvalidateTestWFS(t)
	wfs.filerCallTimeout = 200 * time.Millisecond

	wedged := listenFiler(t, &wedgedFiler{})
	wfs.option.FilerAddresses = []pb.ServerAddress{
		wedged,
		pb.NewServerAddressWithGrpcPort("127.0.0.1:1", 1),
	}
	atomic.StoreInt32(&wfs.option.filerIndex, 0)

	mux := newStreamMutateMux(wfs)
	defer mux.Close()

	_, err := mux.CreateEntry(context.Background(), &filer_pb.CreateEntryRequest{
		Directory: "/dir",
		Entry:     &filer_pb.Entry{Name: "file"},
	})
	if !errors.Is(err, ErrStreamTransport) {
		t.Fatalf("err = %v, want ErrStreamTransport", err)
	}
	if got := atomic.LoadInt32(&wfs.option.filerIndex); got != 1 {
		t.Fatalf("filerIndex = %d, want 1 after teardown of the wedged stream", got)
	}
}
