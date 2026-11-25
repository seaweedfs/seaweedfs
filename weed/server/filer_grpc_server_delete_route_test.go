package weed_server

import (
	"context"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

// separateStores gives B a store of its own. Nothing replicates between the
// two, as if A had not yet replayed what B stored.
func separateStores(c *twoFilers) *renameTestStore {
	storeB := newRenameTestStore()
	storeB.entries["/"] = newDirectoryEntry("/", 1)
	c.fsB.filer.Store = filer.NewFilerStoreWrapper(storeB)
	return storeB
}

// Creates that must be exclusive land on the write owner. An unlink or rmdir
// sent to another filer right after must delete them there, even though that
// filer's own store has not seen them yet.
func TestDeleteEntryMissReachesOwnerStore(t *testing.T) {
	for _, tc := range []struct {
		name string
		req  func(dir, name string) *filer_pb.CreateEntryRequest
	}{
		{"exclusive file", func(dir, name string) *filer_pb.CreateEntryRequest {
			return &filer_pb.CreateEntryRequest{Directory: dir, OExcl: true, Entry: &filer_pb.Entry{
				Name: name, Attributes: &filer_pb.FuseAttributes{Mtime: 1700000000, FileMode: 0644},
			}}
		}},
		{"exclusive directory", func(dir, name string) *filer_pb.CreateEntryRequest {
			return &filer_pb.CreateEntryRequest{Directory: dir, OExcl: true, Entry: &filer_pb.Entry{
				Name: name, IsDirectory: true, Attributes: &filer_pb.FuseAttributes{Mtime: 1700000000, FileMode: uint32(os.ModeDir | 0755)},
			}}
		}},
		{"create if not exists", func(dir, name string) *filer_pb.CreateEntryRequest {
			return &filer_pb.CreateEntryRequest{Directory: dir, Entry: &filer_pb.Entry{
				Name: name, Attributes: &filer_pb.FuseAttributes{Mtime: 1700000000, FileMode: 0644},
			}, Condition: &filer_pb.WriteCondition{Clauses: []*filer_pb.WriteCondition_Clause{{Kind: filer_pb.WriteCondition_IF_NOT_EXISTS}}}}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newTwoFilers(t)
			storeB := separateStores(c)
			sharedWriteRing(c)
			p := c.keyOwnedBy(t, c.addrB, c.addrB)
			dir, name := p.DirAndName()
			ctx := context.Background()

			if resp, err := c.fsA.CreateEntry(ctx, tc.req(dir, name)); err != nil || resp.GetError() != "" {
				t.Fatalf("create via A: resp=%v err=%v", resp, err)
			}
			if _, err := storeB.FindEntry(ctx, p); err != nil {
				t.Fatalf("the owner did not store the create: %v", err)
			}
			if _, err := c.store.FindEntry(ctx, p); err != filer_pb.ErrNotFound {
				t.Fatalf("A's store must not have the entry yet: %v", err)
			}

			req := &filer_pb.DeleteEntryRequest{Directory: dir, Name: name, IsDeleteData: true}
			if resp, err := c.fsA.DeleteEntry(ctx, req); err != nil || resp.GetError() != "" {
				t.Fatalf("delete via A: resp=%v err=%v", resp, err)
			}
			if _, err := storeB.FindEntry(ctx, p); err != filer_pb.ErrNotFound {
				t.Fatalf("the owner kept the entry after a successful delete: %v", err)
			}
		})
	}
}

// fakeOwner answers the signature read of A's aggregator and counts the
// deletes A forwards to it.
type fakeOwner struct {
	filer_pb.UnimplementedSeaweedFilerServer
	signature int32
	deletes   atomic.Int32
}

func (o *fakeOwner) GetFilerConfiguration(context.Context, *filer_pb.GetFilerConfigurationRequest) (*filer_pb.GetFilerConfigurationResponse, error) {
	return &filer_pb.GetFilerConfigurationResponse{Signature: o.signature}, nil
}

func (o *fakeOwner) DeleteEntry(_ context.Context, req *filer_pb.DeleteEntryRequest) (*filer_pb.DeleteEntryResponse, error) {
	if req.IsMoved {
		o.deletes.Add(1)
	}
	return &filer_pb.DeleteEntryResponse{}, nil
}

// On a store the owner shares, the miss is the owner's answer too, so a
// delete of an absent entry costs no extra hop. On a store of its own, the
// owner is asked.
func TestDeleteEntryMissAsksOwnerOnlyOnSeparateStore(t *testing.T) {
	for _, tc := range []struct {
		name        string
		sharesStore bool
		forwards    int32
	}{
		{"shared store", true, 0},
		{"separate store", false, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs, _ := createRouteServer(t)
			fs.filer.Signature = 41
			owner := &fakeOwner{signature: 42}
			if tc.sharesStore {
				owner.signature = fs.filer.Signature
			}
			lis, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatalf("listen: %v", err)
			}
			srv := grpc.NewServer()
			filer_pb.RegisterSeaweedFilerServer(srv, owner)
			go func() { _ = srv.Serve(lis) }()
			ownerAddr := pb.NewServerAddressWithGrpcPort(lis.Addr().String(), lis.Addr().(*net.TCPAddr).Port)
			t.Cleanup(func() {
				srv.Stop()
				pb.InvalidateGrpcConnection(ownerAddr.ToGrpcAddress())
			})
			setRing(fs, 0, []pb.ServerAddress{fs.option.Host, ownerAddr})

			ma := filer.NewMetaAggregator(fs.filer, fs.option.Host, fs.grpcDialOption)
			fs.filer.MetaAggregator = ma
			t.Cleanup(ma.Shutdown)
			ma.OnPeerUpdate(&master_pb.ClusterNodeUpdate{Address: string(ownerAddr), IsAdd: true}, time.Now())
			if tc.sharesStore {
				deadline := time.Now().Add(10 * time.Second)
				for !ma.SharesStore(ownerAddr) {
					if time.Now().After(deadline) {
						t.Fatal("the aggregator never read the owner's store signature")
					}
					time.Sleep(10 * time.Millisecond)
				}
			}

			name := peerOwnedName(t, fs)
			resp, err := fs.DeleteEntry(context.Background(), &filer_pb.DeleteEntryRequest{Directory: "/test", Name: name})
			if err != nil || resp.GetError() != "" {
				t.Fatalf("delete of an absent entry: resp=%v err=%v", resp, err)
			}
			if got := owner.deletes.Load(); got != tc.forwards {
				t.Fatalf("owner received %d forwarded deletes, want %d", got, tc.forwards)
			}
		})
	}
}

// A scoped delete is checked against this filer only, so it is never
// forwarded: the owner would apply it without the scope's checks.
func TestScopedDeleteEntryMissStaysLocal(t *testing.T) {
	fs, _ := createRouteServer(t)
	name := peerOwnedName(t, fs)
	req := &filer_pb.DeleteEntryRequest{Directory: "/test", Name: name}
	resp, err := fs.DeleteEntry(withScopeKeepOnly(context.Background()), req)
	if err != nil || resp.GetError() != "" || req.IsMoved {
		t.Fatalf("scoped delete of an absent entry: resp=%v err=%v forwarded=%v", resp, err, req.IsMoved)
	}
}

// While B's ring still gives the path to A, B refuses the delete A forwards
// rather than answer for a store the creates may not have reached.
func TestForwardedDeleteEntryRefusedByNonOwner(t *testing.T) {
	slack := notWriteOwnerRetrySlack
	notWriteOwnerRetrySlack = 200 * time.Millisecond
	t.Cleanup(func() { notWriteOwnerRetrySlack = slack })

	c := newTwoFilers(t)
	storeB := separateStores(c)
	setRing(c.fsA, 0, []pb.ServerAddress{c.addrA, c.addrB})
	setRing(c.fsB, time.Minute, []pb.ServerAddress{c.addrA}, []pb.ServerAddress{c.addrA, c.addrB})
	p := c.keyOwnedBy(t, c.addrB, c.addrA)
	if err := storeB.InsertEntry(context.Background(), newFileEntry(string(p), 7)); err != nil {
		t.Fatalf("seed B: %v", err)
	}

	dir, name := p.DirAndName()
	_, err := c.fsA.DeleteEntry(context.Background(), &filer_pb.DeleteEntryRequest{Directory: dir, Name: name})
	if !isNotWriteOwner(err) {
		t.Fatalf("B applied or failed otherwise a delete its ring gives to A: %v", err)
	}
	if _, err := storeB.FindEntry(context.Background(), p); err != nil {
		t.Fatalf("a refused forward deleted the entry: %v", err)
	}
}
