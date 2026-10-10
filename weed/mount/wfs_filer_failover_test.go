package mount

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
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

func (s *wedgedFiler) StreamRenameEntry(req *filer_pb.StreamRenameEntryRequest, stream filer_pb.SeaweedFiler_StreamRenameEntryServer) error {
	<-stream.Context().Done()
	return stream.Context().Err()
}

// StreamRenameEntry ends cleanly with no events: enough for a failover test
// to confirm the walk reached this filer.
func (s *fakeFilerServer) StreamRenameEntry(req *filer_pb.StreamRenameEntryRequest, stream filer_pb.SeaweedFiler_StreamRenameEntryServer) error {
	return nil
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

// A timed-out attempt must unwind before the walk runs the next filer's
// attempt: every attempt runs the same closure, so two in flight at once
// race on the caller's captured result variables.
func TestWithFilerClientDoesNotOverlapAttempts(t *testing.T) {
	wfs := newInvalidateTestWFS(t)
	wfs.filerCallTimeout = 200 * time.Millisecond

	wedged := listenFiler(t, &wedgedFiler{})
	live := listenFiler(t, &fakeFilerServer{lookupSize: 42})
	wfs.option.FilerAddresses = []pb.ServerAddress{wedged, live}
	atomic.StoreInt32(&wfs.option.filerIndex, 0)

	var attempts, inside, overlap atomic.Int32
	release := make(chan struct{})

	done := make(chan error, 1)
	go func() {
		done <- wfs.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
			n := attempts.Add(1)
			if !inside.CompareAndSwap(0, 1) {
				overlap.Add(1)
			}
			defer inside.Store(0)
			if n == 1 {
				// Outlive the attempt deadline on purpose: the walk must
				// wait for this callback instead of racing ahead.
				<-release
				return errors.New("first attempt released")
			}
			return nil
		})
	}()

	deadline := time.Now().Add(10 * wfs.filerCallTimeout)
	for attempts.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if attempts.Load() == 0 {
		t.Fatal("first attempt never started")
	}

	// Past the deadline plus its unwind grace, the blocked callback must
	// still be the only attempt: no second run may start alongside it.
	time.Sleep(3 * wfs.filerCallTimeout)
	if got := attempts.Load(); got != 1 {
		t.Fatalf("attempts = %d, want 1 while the timed-out attempt is still unwinding", got)
	}

	close(release)
	if err := <-done; err != nil {
		t.Fatalf("WithFilerClient: %v", err)
	}
	if got := overlap.Load(); got != 0 {
		t.Fatalf("attempts overlapped %d times", got)
	}
	if got := attempts.Load(); got != 2 {
		t.Fatalf("attempts = %d, want 2", got)
	}
}

// The unary fallback for a streamed rename must bound silence too: a filer
// that accepts StreamRenameEntry then never answers cannot hold the walk.
func TestDoRenameFailsOverPastWedgedFiler(t *testing.T) {
	wfs := newInvalidateTestWFS(t)
	wfs.filerCallTimeout = 200 * time.Millisecond

	wedged := listenFiler(t, &wedgedFiler{})
	live := listenFiler(t, &fakeFilerServer{})
	wfs.option.FilerAddresses = []pb.ServerAddress{wedged, live}
	atomic.StoreInt32(&wfs.option.filerIndex, 0)

	var newPathLock *cluster.LiveLock
	start := time.Now()
	err := wfs.doRename(context.Background(), &filer_pb.StreamRenameEntryRequest{
		OldDirectory: "/a",
		OldName:      "f",
		NewDirectory: "/b",
		NewName:      "f",
	}, util.FullPath("/a/f"), util.FullPath("/b/f"), &newPathLock)
	if err != nil {
		t.Fatalf("doRename: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 10*wfs.filerCallTimeout {
		t.Fatalf("doRename took %v, wedged fallback stream was not bounded", elapsed)
	}
	if got := atomic.LoadInt32(&wfs.option.filerIndex); got != 1 {
		t.Fatalf("filerIndex = %d, want 1 (the live filer)", got)
	}
}
