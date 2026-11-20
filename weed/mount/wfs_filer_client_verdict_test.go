package mount

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// readOnlyCreateFiler refuses every create as a write to a read-only path.
type readOnlyCreateFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	creates atomic.Int32
}

func (f *readOnlyCreateFiler) CreateEntry(context.Context, *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	f.creates.Add(1)
	return &filer_pb.CreateEntryResponse{
		Error:     "read only: /buckets/b/ (e.g. bucket over quota)",
		ErrorCode: filer_pb.FilerError_READ_ONLY,
	}, nil
}

// The first filer's verdict is the answer even when the next filer is down. The
// walk must not trade it for that filer's transport error and retry.
func TestWithFilerClientKeepsTheFirstFilersVerdict(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	server := pb.NewGrpcServer()
	filer := &readOnlyCreateFiler{}
	filer_pb.RegisterSeaweedFilerServer(server, filer)
	go server.Serve(listener)
	t.Cleanup(server.Stop)

	// A port nothing listens on: dialing it fails as Unavailable.
	down, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	downPort := down.Addr().(*net.TCPAddr).Port
	_ = down.Close()

	wfs := &WFS{
		option: &Option{
			FilerAddresses: []pb.ServerAddress{
				pb.NewServerAddressWithGrpcPort("127.0.0.1:1", listener.Addr().(*net.TCPAddr).Port),
				pb.NewServerAddressWithGrpcPort("127.0.0.1:2", downPort),
			},
			GrpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	}

	start := time.Now()
	err = wfs.withFilerClient(context.Background(), false, time.Second, func(ctx context.Context, client filer_pb.SeaweedFilerClient) error {
		_, createErr := filer_pb.CreateEntryWithResponse(ctx, client, &filer_pb.CreateEntryRequest{
			Directory: "/buckets/b",
			Entry:     &filer_pb.Entry{Name: "f"},
		})
		return createErr
	})
	if !errors.Is(err, filer_pb.ErrReadOnly) {
		t.Fatalf("withFilerClient error = %v, want the first filer's ErrReadOnly", err)
	}
	if n := filer.creates.Load(); n != 1 {
		t.Fatalf("creates = %d, want 1: a verdict must not be retried", n)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("withFilerClient took %v for a verdict", elapsed)
	}
}
