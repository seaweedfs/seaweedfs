package weed_server

import (
	"context"
	"fmt"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/seaweedfs/seaweedfs/weed/cluster/lock_manager"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// createRouteServer builds a filer whose ring holds two unreachable peers, so a
// forwarded create fails at the dial and is distinguishable from a local apply.
func createRouteServer(t *testing.T) (*FilerServer, *renameTestStore) {
	t.Helper()
	const self = pb.ServerAddress("127.0.0.1:18888")
	store := newRenameTestStore()
	f := newRenameTestFiler(t, store)
	f.DirBucketsPath = "/buckets"
	f.Dlm = lock_manager.NewDistributedLockManager(self)
	f.Dlm.LockRing.SetSnapshot([]pb.ServerAddress{self, "127.0.0.1:18889"}, 1)
	return &FilerServer{
		filer:          f,
		option:         &FilerOption{Host: self},
		grpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
		entryLockTable: util.NewLockTable[util.FullPath](),
	}, store
}

// peerOwnedName returns a child name under /test whose entry the ring assigns to
// the peer, so a routed create must leave this filer.
func peerOwnedName(t *testing.T, fs *FilerServer) string {
	t.Helper()
	for i := 0; i < 4000; i++ {
		name := fmt.Sprintf("obj-%d", i)
		key := entryRouteKey(util.NewFullPath("/test", name))
		if fs.filer.Dlm.LockRing.GetPrimary(key) != fs.option.Host {
			return name
		}
	}
	t.Skip("no name owned by the peer")
	return ""
}

func createReq(name string, oExcl bool) *filer_pb.CreateEntryRequest {
	return &filer_pb.CreateEntryRequest{
		Directory:                "/test",
		OExcl:                    oExcl,
		SkipCheckParentDirectory: true,
		Entry: &filer_pb.Entry{
			Name:       name,
			Attributes: &filer_pb.FuseAttributes{Mtime: 1700000000, FileMode: 0644, Inode: 1},
		},
	}
}

// An exclusive create for a peer-owned path must leave this filer rather than
// take the local lock, whose arbitration would not bind the other filers.
func TestCreateEntryExclusiveLeavesNonOwner(t *testing.T) {
	fs, store := createRouteServer(t)
	name := peerOwnedName(t, fs)

	_, err := fs.CreateEntry(context.Background(), createReq(name, true))
	if err == nil {
		t.Fatal("exclusive create on a non-owner must not be applied locally")
	}
	if _, found := store.entries[string(util.NewFullPath("/test", name))]; found {
		t.Fatal("forwarded create must not also write locally")
	}
}

// Plain creates are upserts whichever filer applies them, so routing them would
// buy nothing and cost a hop.
func TestCreateEntryPlainStaysLocal(t *testing.T) {
	fs, store := createRouteServer(t)
	name := peerOwnedName(t, fs)

	resp, err := fs.CreateEntry(context.Background(), createReq(name, false))
	if err != nil || resp.Error != "" {
		t.Fatalf("plain create must be applied locally, got err=%v resp=%v", err, resp.Error)
	}
	if _, found := store.entries[string(util.NewFullPath("/test", name))]; !found {
		t.Fatal("plain create must reach the local store")
	}
}

// is_moved bounds forwarding to one hop: the owner applies it even if its own
// ring view says someone else owns the key.
func TestCreateEntryMovedAppliesLocally(t *testing.T) {
	fs, store := createRouteServer(t)
	name := peerOwnedName(t, fs)

	req := createReq(name, true)
	req.IsMoved = true
	resp, err := fs.CreateEntry(context.Background(), req)
	if err != nil || resp.Error != "" {
		t.Fatalf("forwarded create must be applied locally, got err=%v resp=%v", err, resp.Error)
	}
	if _, found := store.entries[string(util.NewFullPath("/test", name))]; !found {
		t.Fatal("forwarded create must reach the local store")
	}
}

// An exclusive create this filer owns is applied locally, under its per-path lock.
func TestCreateEntryExclusiveAppliedByOwner(t *testing.T) {
	fs, store := createRouteServer(t)
	var name string
	for i := 0; i < 4000 && name == ""; i++ {
		candidate := fmt.Sprintf("obj-%d", i)
		key := entryRouteKey(util.NewFullPath("/test", candidate))
		if fs.filer.Dlm.LockRing.GetPrimary(key) == fs.option.Host {
			name = candidate
		}
	}
	if name == "" {
		t.Skip("no name owned by this filer")
	}

	resp, err := fs.CreateEntry(context.Background(), createReq(name, true))
	if err != nil || resp.Error != "" {
		t.Fatalf("owner must apply the create, got err=%v resp=%v", err, resp.Error)
	}
	if _, found := store.entries[string(util.NewFullPath("/test", name))]; !found {
		t.Fatal("owner's create must reach the local store")
	}
}

// A key still inside the cooling-off window belongs to its prior owner. If that
// filer cannot be reached the create fails: it may be partitioned rather than
// down, and applying here would be the second serialization point on the key.
func TestCreateEntryFailsWhenPriorOwnerIsUnreachable(t *testing.T) {
	const self = pb.ServerAddress("127.0.0.1:28888")
	const peer = pb.ServerAddress("127.0.0.1:28889")
	store := newRenameTestStore()
	f := newRenameTestFiler(t, store)
	f.DirBucketsPath = "/buckets"
	f.Dlm = lock_manager.NewDistributedLockManager(self)
	// Dropping the peer moves every key it owned to this filer, with the peer as
	// their prior owner — and nothing is listening on it.
	f.Dlm.LockRing.SetSnapshot([]pb.ServerAddress{self, peer}, 1)
	f.Dlm.LockRing.SetSnapshot([]pb.ServerAddress{self}, 2)
	fs := &FilerServer{
		filer:          f,
		option:         &FilerOption{Host: self},
		grpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
		entryLockTable: util.NewLockTable[util.FullPath](),
	}

	var name string
	for i := 0; i < 4000 && name == ""; i++ {
		candidate := fmt.Sprintf("obj-%d", i)
		if fs.filer.Dlm.LockRing.PriorOwner(entryRouteKey(util.NewFullPath("/test", candidate))) == peer {
			name = candidate
		}
	}
	if name == "" {
		t.Skip("no key whose prior owner is the departed peer")
	}

	resp, err := fs.CreateEntry(context.Background(), createReq(name, true))
	if err == nil {
		t.Fatalf("unreachable prior owner must fail the create, got resp=%v", resp)
	}
	if resp == nil {
		t.Fatal("a failed create must still answer with a response, not nil")
	}
	if _, found := store.entries[string(util.NewFullPath("/test", name))]; found {
		t.Fatal("a create routed away must not also write locally")
	}
}
