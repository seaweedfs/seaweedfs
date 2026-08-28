package filer

import (
	"context"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/log_buffer"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

// collectionDeleteMaster is just enough of a master for a MasterClient to
// consider itself connected, and records every CollectionDelete it is asked for.
type collectionDeleteMaster struct {
	master_pb.UnimplementedSeaweedServer
	calls chan collectionDeleteCall
}

type collectionDeleteCall struct {
	name string
	// budget is the time the RPC arrived with, or 0 when it carried no deadline.
	budget time.Duration
}

// KeepConnected is what MasterClient waits on before it reports a master: one
// response is enough, then the stream idles until the server is torn down.
func (m *collectionDeleteMaster) KeepConnected(stream master_pb.Seaweed_KeepConnectedServer) error {
	if _, err := stream.Recv(); err != nil {
		return err
	}
	if err := stream.Send(&master_pb.KeepConnectedResponse{}); err != nil {
		return err
	}
	<-stream.Context().Done()
	return nil
}

func (m *collectionDeleteMaster) CollectionDelete(ctx context.Context, req *master_pb.CollectionDeleteRequest) (*master_pb.CollectionDeleteResponse, error) {
	call := collectionDeleteCall{name: req.Name}
	if deadline, ok := ctx.Deadline(); ok {
		call.budget = time.Until(deadline)
	}
	m.calls <- call
	return &master_pb.CollectionDeleteResponse{}, nil
}

// hookedStore is the stub store with one extra seam: a callback that runs once
// an entry has actually been removed, so a test can interleave an event -- a
// client hanging up, say -- between the store write and whatever follows it.
type hookedStore struct {
	*stubFilerStore
	onDeleteEntry func(util.FullPath)
}

func (s *hookedStore) DeleteEntry(ctx context.Context, p util.FullPath) error {
	if err := s.stubFilerStore.DeleteEntry(ctx, p); err != nil {
		return err
	}
	if s.onDeleteEntry != nil {
		s.onDeleteEntry(p)
	}
	return nil
}

// newFilerWithFakeMaster builds a filer backed by the stub store, with its
// MasterClient connected to a fake master the test can observe.
func newFilerWithFakeMaster(t *testing.T) (*Filer, *hookedStore, *collectionDeleteMaster) {
	t.Helper()

	master := &collectionDeleteMaster{calls: make(chan collectionDeleteCall, 4)}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	master_pb.RegisterSeaweedServer(grpcServer, master)
	go func() { _ = grpcServer.Serve(lis) }()
	t.Cleanup(grpcServer.Stop)

	_, port, err := net.SplitHostPort(lis.Addr().String())
	if err != nil {
		t.Fatalf("split listener address: %v", err)
	}
	masterAddress := pb.ServerAddress(fmt.Sprintf("127.0.0.1:0.%s", port))

	mc := wdclient.NewMasterClient(
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		"test", cluster.FilerType, pb.ServerAddress("localhost:0"), "", "",
		*pb.NewServiceDiscoveryFromMap(map[string]pb.ServerAddress{"m": masterAddress}),
	)

	connecting, stopConnecting := context.WithCancel(context.Background())
	t.Cleanup(stopConnecting)
	go mc.KeepConnectedToMaster(connecting)

	waiting, cancelWaiting := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelWaiting()
	mc.WaitUntilConnected(waiting)
	if waiting.Err() != nil {
		t.Fatal("the master client never connected to the fake master")
	}

	store := &hookedStore{stubFilerStore: newStubFilerStore()}
	f := &Filer{
		DirBucketsPath:      "/buckets",
		RemoteStorage:       NewFilerRemoteStorage(),
		Store:               NewFilerStoreWrapper(store),
		FilerConf:           NewFilerConf(),
		MaxFilenameLength:   255,
		MasterClient:        mc,
		FileIdDeletionQueue: util.NewUnboundedQueue(),
		deletionQuit:        make(chan struct{}),
		LocalMetaLogBuffer: log_buffer.NewLogBuffer("test", time.Minute,
			func(*log_buffer.LogBuffer, time.Time, time.Time, []byte, int64, int64) {}, nil, func() {}),
	}
	return f, store, master
}

// Deleting a bucket entry deletes its collection, and the request hanging up
// partway must not stop that: the bucket's metadata is already gone, so skipping
// it strands the bucket's volumes with nothing left to come back for them. The
// cleanup still has to carry a deadline of its own, or a master that is down
// parks this handler and every client retry behind it parks another.
//
// The cancellation lands between the store removing the entry and the collection
// delete, which is where a client disconnect actually bites: FilerStoreWrapper
// refuses a context that is already dead on entry, so an up-front cancellation
// fails the delete long before this point instead.
func TestDeleteEntryMetaAndDataDeletesCollectionWhenTheRequestIsCancelledMidDelete(t *testing.T) {
	f, store, master := newFilerWithFakeMaster(t)

	const bucket = "bucket-a"
	bucketPath := util.FullPath(f.DirBucketsPath + "/" + bucket)
	if err := store.InsertEntry(context.Background(), &Entry{
		FullPath: bucketPath,
		Attr:     Attr{Mode: os.ModeDir | 0755},
	}); err != nil {
		t.Fatalf("seed the bucket entry: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// The client hangs up just as the bucket entry comes out of the store.
	store.onDeleteEntry = func(util.FullPath) { cancel() }

	if err := f.DeleteEntryMetaAndData(ctx, bucketPath, true, false, true, false, nil, 0); err != nil {
		t.Fatalf("DeleteEntryMetaAndData: %v", err)
	}
	if ctx.Err() == nil {
		t.Fatal("test setup: the request was never cancelled, so nothing was exercised")
	}

	var call collectionDeleteCall
	select {
	case call = <-master.calls:
	case <-time.After(20 * time.Second):
		t.Fatal("CollectionDelete never reached the master")
	}
	if call.name != bucket {
		t.Errorf("master was asked to delete collection %q, want %q", call.name, bucket)
	}
	if call.budget <= 0 {
		t.Error("CollectionDelete arrived with no deadline; a master that stops answering would hold this handler open")
	}
	if call.budget > collectionDeleteTimeout {
		t.Errorf("CollectionDelete budget = %v, want at most collectionDeleteTimeout %v", call.budget, collectionDeleteTimeout)
	}

	if store.getEntry(string(bucketPath)) != nil {
		t.Error("the bucket entry survived the delete")
	}
}
