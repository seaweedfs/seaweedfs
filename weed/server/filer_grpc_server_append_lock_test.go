package weed_server

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const appendLockTestPath = "/data/app.log"

// appendHoldStore parks the first lookup of appendLockTestPath, which
// AppendToEntry makes while it holds the cluster lock, until released.
type appendHoldStore struct {
	*renameTestStore
	once    sync.Once
	held    chan struct{}
	release chan struct{}
}

func (s *appendHoldStore) FindEntry(ctx context.Context, p util.FullPath) (*filer.Entry, error) {
	if string(p) == appendLockTestPath {
		s.once.Do(func() {
			close(s.held)
			<-s.release
		})
	}
	return s.renameTestStore.FindEntry(ctx, p)
}

// newAppendLockServer serves the filer's lock RPCs on a real listener so
// AppendToEntry takes its cluster lock from itself, as a one-filer ring does.
func newAppendLockServer(t *testing.T) (*FilerServer, *appendHoldStore) {
	store := &appendHoldStore{
		renameTestStore: newRenameTestStore(),
		held:            make(chan struct{}),
		release:         make(chan struct{}),
	}
	f := newRenameTestFiler(t, store.renameTestStore)
	f.Store = filer.NewFilerStoreWrapper(store)
	fs := &FilerServer{
		filer:          f,
		option:         &FilerOption{},
		entryLockTable: util.NewLockTable[util.FullPath](),
		grpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := pb.NewServerAddressWithGrpcPort(lis.Addr().String(), lis.Addr().(*net.TCPAddr).Port)
	withRing(fs, addr, addr)
	srv := grpc.NewServer()
	filer_pb.RegisterSeaweedFilerServer(srv, fs)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	return fs, store
}

func startAppend(fs *FilerServer) chan error {
	done := make(chan error, 1)
	go func() {
		_, err := fs.AppendToEntry(context.Background(), &filer_pb.AppendToEntryRequest{
			Directory: "/data",
			EntryName: "app.log",
			Chunks:    []*filer_pb.FileChunk{{FileId: "3,01637037d6", Size: 10}},
		})
		done <- err
	}()
	return done
}

// An append that outlasts the lock's first lease keeps the lock: another
// filer must not be able to take it and append concurrently.
func TestAppendToEntryKeepsLockThroughSlowAppend(t *testing.T) {
	t.Parallel()
	fs, store := newAppendLockServer(t)
	done := startAppend(fs)
	<-store.held

	time.Sleep(6 * time.Second)
	resp, err := fs.DistributedLock(context.Background(), &filer_pb.LockRequest{
		Name:          appendLockTestPath,
		SecondsToLock: 5,
		Owner:         "other-filer",
	})
	require.NoError(t, err)
	require.Contains(t, resp.Error, "lock already owned", "another filer took the lock of a running append")

	close(store.release)
	require.NoError(t, <-done)
	require.Len(t, store.entries[appendLockTestPath].GetChunks(), 1)
}
