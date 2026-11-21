package weed_server

import (
	"context"
	"fmt"
	"net"
	"slices"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/cluster/lock_manager"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// receivedUpdates counts the UpdateEntry requests a filer receives over gRPC,
// and how many of them another filer forwarded (is_moved).
type receivedUpdates struct {
	mu             sync.Mutex
	updates, moved int
}

func (c *receivedUpdates) intercept(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if r, ok := req.(*filer_pb.UpdateEntryRequest); ok {
		c.mu.Lock()
		c.updates++
		if r.IsMoved {
			c.moved++
		}
		c.mu.Unlock()
	}
	return handler(ctx, req)
}

func (c *receivedUpdates) counts() (updates, moved int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.updates, c.moved
}

// twoFilers is filers A and B on one shared store, each serving gRPC with its
// own ring view, so either can forward a routed write to the other.
type twoFilers struct {
	store          *renameTestStore
	dial           grpc.DialOption
	addrA, addrB   pb.ServerAddress
	fsA, fsB       *FilerServer
	callsA, callsB *receivedUpdates
}

func newTwoFilers(t *testing.T) *twoFilers {
	t.Helper()
	c := &twoFilers{
		store:  newRenameTestStore(),
		dial:   grpc.WithTransportCredentials(insecure.NewCredentials()),
		callsA: &receivedUpdates{},
		callsB: &receivedUpdates{},
	}
	c.store.entries["/"] = newDirectoryEntry("/", 1)
	serve := func(calls *receivedUpdates) (*FilerServer, pb.ServerAddress) {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		host := pb.NewServerAddressWithGrpcPort(lis.Addr().String(), lis.Addr().(*net.TCPAddr).Port)
		f := newRenameTestFiler(t, c.store)
		f.Dlm = lock_manager.NewDistributedLockManager(host)
		fs := &FilerServer{
			filer:          f,
			option:         &FilerOption{Host: host},
			grpcDialOption: c.dial,
			entryLockTable: util.NewLockTable[util.FullPath](),
		}
		srv := grpc.NewServer(grpc.UnaryInterceptor(calls.intercept))
		filer_pb.RegisterSeaweedFilerServer(srv, fs)
		go func() { _ = srv.Serve(lis) }()
		t.Cleanup(func() {
			srv.Stop()
			pb.InvalidateGrpcConnection(host.ToGrpcAddress())
		})
		return fs, host
	}
	c.fsA, c.addrA = serve(c.callsA)
	c.fsB, c.addrB = serve(c.callsB)
	return c
}

// setRing gives fs a ring built from the given snapshots, oldest first. Inside
// the cooling-off window of the last change a moved key keeps its prior owner.
func setRing(fs *FilerServer, window time.Duration, snapshots ...[]pb.ServerAddress) {
	fs.filer.Dlm.LockRing = lock_manager.NewLockRing(window)
	for i, servers := range snapshots {
		fs.filer.Dlm.LockRing.SetSnapshot(slices.Clone(servers), int64(i+1))
	}
}

// keyOwnedBy returns a test path whose write owner is ownerInA in A's ring and
// ownerInB in B's.
func (c *twoFilers) keyOwnedBy(t *testing.T, ownerInA, ownerInB pb.ServerAddress) util.FullPath {
	t.Helper()
	for i := 0; i < 4000; i++ {
		p := util.FullPath(fmt.Sprintf("/forward-%d", i))
		key := entryRouteKey(p)
		if c.fsA.filer.Dlm.LockRing.WriteOwner(key) == ownerInA && c.fsB.filer.Dlm.LockRing.WriteOwner(key) == ownerInB {
			return p
		}
	}
	t.Fatalf("no path that A's ring gives to %s and B's ring gives to %s", ownerInA, ownerInB)
	return ""
}

// seed stores p with the extended attribute v=seed and returns it as read.
func (c *twoFilers) seed(t *testing.T, p util.FullPath) *filer_pb.Entry {
	t.Helper()
	e := newFileEntry(string(p), 7)
	e.Extended = map[string][]byte{"v": []byte("seed")}
	if err := c.store.InsertEntry(context.Background(), e); err != nil {
		t.Fatalf("seed %s: %v", p, err)
	}
	return e.ToProtoEntry()
}

// update sends addr an UpdateEntry that sets v to value only while v is still
// expected's.
func (c *twoFilers) update(addr pb.ServerAddress, p util.FullPath, expected *filer_pb.Entry, value string) error {
	dir, _ := p.DirAndName()
	next := proto.Clone(expected).(*filer_pb.Entry)
	next.Extended = map[string][]byte{"v": []byte(value)}
	return pb.WithFilerClient(false, 0, addr, c.dial, func(client filer_pb.SeaweedFilerClient) error {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, err := client.UpdateEntry(ctx, &filer_pb.UpdateEntryRequest{
			Directory:        dir,
			Entry:            next,
			ExpectedExtended: map[string][]byte{"v": expected.Extended["v"]},
		})
		return err
	})
}

func (c *twoFilers) stored(t *testing.T, p util.FullPath) string {
	t.Helper()
	e, err := c.store.FindEntry(context.Background(), p)
	if err != nil {
		t.Fatalf("read %s: %v", p, err)
	}
	return string(e.Extended["v"])
}

// A's ring names B as the write owner of the entry, while B, inside the
// cooling-off window of the change from {A} to {A, B}, names A. B must refuse
// A's forwarded conditional update rather than check it under its own lock,
// which another writer routed by B's ring would not take. A's ring has no
// window left to close, so A surfaces the refusal after its retries.
func TestForwardedConditionalUpdateRefusedByNonOwner(t *testing.T) {
	slack := notWriteOwnerRetrySlack
	notWriteOwnerRetrySlack = 200 * time.Millisecond
	t.Cleanup(func() { notWriteOwnerRetrySlack = slack })

	c := newTwoFilers(t)
	setRing(c.fsA, 0, []pb.ServerAddress{c.addrA, c.addrB})
	setRing(c.fsB, time.Minute, []pb.ServerAddress{c.addrA}, []pb.ServerAddress{c.addrA, c.addrB})
	p := c.keyOwnedBy(t, c.addrB, c.addrA)
	expected := c.seed(t, p)

	err := c.update(c.addrA, p, expected, "via-A")
	if !isNotWriteOwner(err) {
		t.Fatalf("B applied or failed otherwise a forward its ring gives to A: %v", err)
	}
	if got := c.stored(t, p); got != "seed" {
		t.Fatalf("a refused forward changed the entry to %q", got)
	}
	ua, ma := c.callsA.counts()
	ub, mb := c.callsB.counts()
	if ua != 1 || ma != 0 || ub < 2 || mb != ub {
		t.Fatalf("updates received: A %d (forwarded %d), B %d (forwarded %d); want one on A, retried forwards on B", ua, ma, ub, mb)
	}
}

// The same disagreement ends when B's window closes and B's ring names B too:
// A keeps re-routing the refused forward, B applies it once, and a second
// update with the same expectation fails its condition.
func TestForwardedConditionalUpdateAppliedOnceRingsAgree(t *testing.T) {
	const window = time.Second
	c := newTwoFilers(t)
	setRing(c.fsA, window, []pb.ServerAddress{c.addrA, c.addrB})
	setRing(c.fsB, window, []pb.ServerAddress{c.addrA}, []pb.ServerAddress{c.addrA, c.addrB})
	p := c.keyOwnedBy(t, c.addrB, c.addrA)
	expected := c.seed(t, p)

	if err := c.update(c.addrA, p, expected, "via-A"); err != nil {
		t.Fatalf("update via A once the rings agree: %v", err)
	}
	if got := c.stored(t, p); got != "via-A" {
		t.Fatalf("stored %q, want via-A", got)
	}
	if ub, mb := c.callsB.counts(); ub < 2 || mb != ub {
		t.Fatalf("B received %d updates (%d forwarded), want a refused forward before the applied one", ub, mb)
	}

	err := c.update(c.addrB, p, expected, "via-B")
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("the second update with the same expectation must fail its condition, got %v", err)
	}
	if got := c.stored(t, p); got != "via-A" {
		t.Fatalf("stored %q after the losing update, want via-A", got)
	}
}
