package weed_server

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer/posixlock"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func peerClient(t *testing.T, c *twoFilers, addr pb.ServerAddress, token string) filer_pb.SeaweedFilerClient {
	t.Helper()
	conn, err := grpc.NewClient(
		"passthrough:///"+addr.ToGrpcAddress(),
		c.dial,
		grpc.WithPerRPCCredentials(scopeToken(token)),
	)
	if err != nil {
		t.Fatalf("dial filer %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return filer_pb.NewSeaweedFilerClient(conn)
}

func peerAdminToken(key security.SigningKey) string {
	return string(security.GenJwtForFilerServer(key, 60))
}

func peerScopedWriteToken(key security.SigningKey) string {
	return string(security.GenJwtForFilerScope(key, time.Minute, "peer-forward", []string{"/"}, nil))
}

func peerPosixKey(c *twoFilers, owner pb.ServerAddress) string {
	for i := 0; i < 4000; i++ {
		key := fmt.Sprintf("s3.fuse.lock:/peer-%d", i)
		if c.fsA.filer.Dlm.LockRing.GetPrimary(key) == owner && c.fsB.filer.Dlm.LockRing.GetPrimary(key) == owner {
			return key
		}
	}
	return ""
}

func peerLockKey(c *twoFilers, owner pb.ServerAddress) string {
	for i := 0; i < 4000; i++ {
		key := fmt.Sprintf("peer-lock-%d", i)
		if c.fsA.filer.Dlm.LockRing.GetPrimary(key) == owner && c.fsB.filer.Dlm.LockRing.GetPrimary(key) == owner {
			return key
		}
	}
	return ""
}

func TestPeerRequireCreateDeleteForwarding(t *testing.T) {
	for _, operation := range []string{"create", "delete"} {
		for _, tc := range []struct {
			name           string
			scoped, remote bool
		}{
			{"scoped remote", true, true},
			{"scoped local", true, false},
			{"admin remote", false, true},
		} {
			t.Run(operation+"/"+tc.name, func(t *testing.T) {
				c, key := newPeerFilers(t)
				owner := c.addrA
				if tc.remote {
					owner = c.addrB
				}
				p := c.keyOwnedBy(t, owner, owner)
				dir, name := p.DirAndName()
				token := peerAdminToken(key)
				if tc.scoped {
					token = string(security.GenJwtForFilerScope(key, time.Minute, "test-caller", []string{"/"}, nil))
				}
				client := peerClient(t, c, c.addrA, token)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				var err error
				var responseError string
				if operation == "create" {
					var resp *filer_pb.CreateEntryResponse
					resp, err = client.CreateEntry(ctx, &filer_pb.CreateEntryRequest{
						Directory: dir, OExcl: true,
						Entry: &filer_pb.Entry{Name: name, Attributes: &filer_pb.FuseAttributes{FileMode: 0644}, Extended: map[string][]byte{"v": []byte("created")}},
					})
					responseError = resp.GetError()
				} else {
					c.seed(t, p)
					var resp *filer_pb.DeleteEntryResponse
					resp, err = client.DeleteEntry(ctx, &filer_pb.DeleteEntryRequest{Directory: dir, Name: name, ExpectedExtended: map[string][]byte{"v": []byte("seed")}})
					responseError = resp.GetError()
				}
				denied := tc.scoped && tc.remote
				want := codes.OK
				if denied {
					want = codes.PermissionDenied
				}
				if status.Code(err) != want || (!denied && responseError != "") {
					t.Fatalf("call code=%v response=%q, want %v", status.Code(err), responseError, want)
				}
				if operation == "create" && !denied {
					if c.stored(t, p) != "created" {
						t.Fatal("allowed create was not stored")
					}
				} else if operation == "delete" && denied {
					if c.stored(t, p) != "seed" {
						t.Fatal("refused delete changed the entry")
					}
				} else if _, findErr := c.store.FindEntry(ctx, p); findErr != filer_pb.ErrNotFound {
					t.Fatalf("entry lookup error=%v, want absent", findErr)
				}
			})
		}
	}
}

func TestRequireModeUnscopedConditionalUpdateForwardsCaller(t *testing.T) {
	c, key := newPeerFilers(t)
	p := c.keyOwnedBy(t, c.addrB, c.addrB)
	expected := c.seed(t, p)
	dir, _ := p.DirAndName()
	entry := proto.Clone(expected).(*filer_pb.Entry)
	entry.Extended = map[string][]byte{"v": []byte("forwarded")}

	client := peerClient(t, c, c.addrA, peerAdminToken(key))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := client.UpdateEntry(ctx, &filer_pb.UpdateEntryRequest{
		Directory:        dir,
		Entry:            entry,
		ExpectedExtended: map[string][]byte{"v": []byte("seed")},
	})
	if err != nil {
		t.Fatalf("unscoped conditional update: %v", err)
	}
	if got := c.stored(t, p); got != "forwarded" {
		t.Fatalf("forwarded update stored %q, want forwarded", got)
	}
}

func TestRequireModeScopedConditionalUpdateRefusesRemoteOwner(t *testing.T) {
	c, key := newPeerFilers(t)
	p := c.keyOwnedBy(t, c.addrB, c.addrB)
	expected := c.seed(t, p)
	dir, _ := p.DirAndName()
	entry := proto.Clone(expected).(*filer_pb.Entry)
	entry.Extended = map[string][]byte{"v": []byte("must-refuse")}

	client := peerClient(t, c, c.addrA, peerScopedWriteToken(key))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := client.UpdateEntry(ctx, &filer_pb.UpdateEntryRequest{
		Directory:        dir,
		Entry:            entry,
		ExpectedExtended: map[string][]byte{"v": []byte("seed")},
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("scoped remote update error=%v, want PermissionDenied", err)
	}
	if got := c.stored(t, p); got != "seed" {
		t.Fatalf("refused update changed the entry to %q", got)
	}
	if updates, _ := c.callsB.counts(); updates != 0 {
		t.Fatalf("refused update reached the owner %d time(s)", updates)
	}
}

func TestRequireModeScopedConditionalUpdateAllowsLocalOwner(t *testing.T) {
	c, key := newPeerFilers(t)
	p := c.keyOwnedBy(t, c.addrA, c.addrA)
	expected := c.seed(t, p)
	dir, _ := p.DirAndName()
	entry := proto.Clone(expected).(*filer_pb.Entry)
	entry.Extended = map[string][]byte{"v": []byte("local")}

	client := peerClient(t, c, c.addrA, peerScopedWriteToken(key))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := client.UpdateEntry(ctx, &filer_pb.UpdateEntryRequest{
		Directory:        dir,
		Entry:            entry,
		ExpectedExtended: map[string][]byte{"v": []byte("seed")},
	}); err != nil {
		t.Fatalf("scoped local update: %v", err)
	}
	if got := c.stored(t, p); got != "local" {
		t.Fatalf("local update stored %q, want local", got)
	}
}

func TestRequireModeUnscopedPosixForwardCarriesCaller(t *testing.T) {
	c, key := newPeerFilers(t)
	c.fsA.posixLocks = posixlock.NewManager()
	c.fsB.posixLocks = posixlock.NewManager()
	lockKey := peerPosixKey(c, c.addrB)
	if lockKey == "" {
		t.Fatal("no POSIX key owned by B")
	}

	client := peerClient(t, c, c.addrA, peerAdminToken(key))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := client.PosixLock(ctx, &filer_pb.PosixLockRequest{
		Key:  lockKey,
		Op:   filer_pb.PosixLockOp_TRY_LOCK,
		Lock: &filer_pb.PosixLockRange{Start: 0, End: 99, Type: posixlock.Write, Sid: 1, Owner: 1, Pid: 1},
	})
	if err != nil || !resp.GetGranted() {
		t.Fatalf("unscoped forwarded POSIX lock: err=%v granted=%v", err, resp.GetGranted())
	}
}

func TestRequireModeScopedPosixForwardRefusesBeforeOwner(t *testing.T) {
	c, key := newPeerFilers(t)
	c.fsA.posixLocks = posixlock.NewManager()
	c.fsB.posixLocks = posixlock.NewManager()
	lockKey := peerPosixKey(c, c.addrB)
	if lockKey == "" {
		t.Fatal("no POSIX key owned by B")
	}

	client := peerClient(t, c, c.addrA, peerScopedWriteToken(key))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := client.PosixLock(ctx, &filer_pb.PosixLockRequest{
		Key:  lockKey,
		Op:   filer_pb.PosixLockOp_TRY_LOCK,
		Lock: &filer_pb.PosixLockRange{Start: 0, End: 99, Type: posixlock.Write, Sid: 2, Owner: 2, Pid: 2},
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("scoped forwarded POSIX lock error=%v response=%v, want PermissionDenied", err, resp)
	}
	if locks := c.fsB.posixLocks.Snapshot()[lockKey]; len(locks) != 0 {
		t.Fatal("scoped POSIX refusal reached the owner")
	}
}

func TestRequireModeUnscopedDistributedLockAndUnlockForwardCaller(t *testing.T) {
	c, key := newPeerFilers(t)
	lockKey := peerLockKey(c, c.addrB)
	if lockKey == "" {
		t.Fatal("no DLM key owned by B")
	}

	client := peerClient(t, c, c.addrA, peerAdminToken(key))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	locked, err := client.DistributedLock(ctx, &filer_pb.LockRequest{
		Name:          lockKey,
		SecondsToLock: 5,
		Owner:         string(c.addrA),
	})
	if err != nil || locked.GetError() != "" || locked.GetRenewToken() == "" {
		t.Fatalf("unscoped forwarded DLM lock: err=%v response=%v", err, locked)
	}
	if _, err := client.DistributedUnlock(ctx, &filer_pb.UnlockRequest{Name: lockKey, RenewToken: locked.GetRenewToken()}); err != nil {
		t.Fatalf("unscoped forwarded DLM unlock: %v", err)
	}
}

func TestWithCallerAuthContextLeavesTokenlessCallsTokenless(t *testing.T) {
	if got := withCallerAuthContext(context.Background()); got != context.Background() {
		t.Fatal("tokenless context was replaced")
	}
	if got := withHTTPBearerContext(context.Background(), ""); got != context.Background() {
		t.Fatal("empty HTTP bearer context was replaced")
	}
}
