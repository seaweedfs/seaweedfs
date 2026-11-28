package weed_server

import (
	"context"
	"crypto/rand"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func newPeerFilers(t *testing.T) (*twoFilers, security.SigningKey) {
	t.Helper()
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	c := newTwoFilers(t, func(fs *FilerServer) {
		fs.filerGuard = security.NewGuard(nil, string(key), 10, "", 0, "", "")
		fs.filer.PeerContext = fs.peerContext
		fs.grpcScope = newFilerGrpcScope(scopeConfig{
			signingKey:       fs.filerGuard.SigningKey,
			verificationKeys: func() (security.SigningKey, security.SigningKey) { return fs.filerGuard.VerificationKeys(true) },
			requireToken:     func() bool { return true },
		}, fs)
	})
	sharedWriteRing(c)
	return c, key
}

// A pooled connection is shared by address, so each caller must supply its own
// authority even when the previous caller reached the same filer as an admin.
func TestPeerAdminTokenConnectionReuse(t *testing.T) {
	for _, adminFirst := range []bool{false, true} {
		name := "tokenless first"
		if adminFirst {
			name = "admin first"
		}
		t.Run(name, func(t *testing.T) {
			c, _ := newPeerFilers(t)
			c.seed(t, util.FullPath("/cached"))
			var firstConn *grpc.ClientConn
			for _, authenticated := range []bool{adminFirst, !adminFirst, adminFirst} {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				err := pb.WithGrpcClient(ctx, false, 0, func(conn *grpc.ClientConn) error {
					if firstConn == nil {
						firstConn = conn
					} else if firstConn != conn {
						t.Error("test did not reuse the real cached connection")
					}
					callCtx := ctx
					if authenticated {
						callCtx = c.fsA.peerContext(callCtx)
					}
					_, err := filer_pb.NewSeaweedFilerClient(conn).LookupDirectoryEntry(callCtx, &filer_pb.LookupDirectoryEntryRequest{Directory: "/", Name: "cached"})
					return err
				}, c.addrB.ToGrpcAddress(), false, c.fsA.grpcDialOption)
				cancel()
				want := codes.Unauthenticated
				if authenticated {
					want = codes.OK
				}
				if status.Code(err) != want {
					t.Fatalf("authenticated=%v: code=%v, want %v", authenticated, status.Code(err), want)
				}
			}
		})
	}
}

func TestPeerAdminTokenLockAndReplication(t *testing.T) {
	c, _ := newPeerFilers(t)
	// Route every internal acquisition to B, including the client's seed A.
	setRing(c.fsA, 0, []pb.ServerAddress{c.addrB})
	setRing(c.fsB, 0, []pb.ServerAddress{c.addrB})
	client := c.fsA.newLockClient()
	client.SetRing([]pb.ServerAddress{c.addrB}, 1)
	lock := client.NewBlockingLongLivedLock("internal-lock", string(c.addrA), 0)
	if lock == nil {
		t.Fatal("filer-owned lock acquisition failed under require-mode")
	}
	if err := lock.AttemptToLock(5 * time.Second); err != nil {
		t.Fatalf("filer-owned lock renewal failed: %v", err)
	}
	if err := lock.Stop(); err != nil {
		t.Fatalf("filer-owned unlock failed: %v", err)
	}
	if _, found := c.fsB.filer.Dlm.GetLock("internal-lock"); found {
		t.Fatal("unlock left a lock")
	}
	c.fsA.SetupDlmReplication()
	c.fsA.filer.Dlm.ReplicateFn(c.addrB, "replicated", time.Now().Add(time.Minute).UnixNano(), "test-lease", "", "test-owner", 7, 1, false)
	backup, found := c.fsB.filer.Dlm.GetLock("replicated")
	if !found || backup.Owner != "test-owner" || !backup.IsBackup || backup.Generation != 7 || backup.Seq != 1 {
		t.Fatal("replication did not store the owner's backup lock")
	}
}

func TestPeerAdminTokenReload(t *testing.T) {
	c, oldKey := newPeerFilers(t)
	c.seed(t, util.FullPath("/reload"))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	oldCtx := c.fsA.peerContext(ctx)
	var firstConn *grpc.ClientConn
	lookup := func(callCtx context.Context, want codes.Code) {
		t.Helper()
		err := pb.WithGrpcClient(callCtx, false, 0, func(conn *grpc.ClientConn) error {
			if firstConn == nil {
				firstConn = conn
			} else if firstConn != conn {
				t.Fatal("key reload replaced the pooled connection")
			}
			_, err := filer_pb.NewSeaweedFilerClient(conn).LookupDirectoryEntry(callCtx, &filer_pb.LookupDirectoryEntryRequest{Directory: "/", Name: "reload"})
			return err
		}, c.addrB.ToGrpcAddress(), false, c.fsA.grpcDialOption)
		if status.Code(err) != want {
			t.Fatalf("lookup code=%v, want %v", status.Code(err), want)
		}
	}
	lookup(oldCtx, codes.OK)
	newKey := make([]byte, 32)
	if _, err := rand.Read(newKey); err != nil {
		t.Fatal(err)
	}
	c.fsB.filerGuard.UpdateSigningKeys(string(newKey), 10, "", 0, string(oldKey), "")
	lookup(oldCtx, codes.OK)
	c.fsA.filerGuard.UpdateSigningKeys(string(newKey), 10, "", 0, "", "")
	lookup(c.fsA.peerContext(ctx), codes.OK)
	c.fsB.filerGuard.UpdateSigningKeys(string(newKey), 10, "", 0, "", "")
	lookup(oldCtx, codes.Unauthenticated)
	lookup(c.fsA.peerContext(ctx), codes.OK)
	c.fsA.filerGuard.UpdateSigningKeys("", 10, "", 0, "", "")
	cleared := c.fsA.peerContext(oldCtx)
	md, _ := metadata.FromOutgoingContext(cleared)
	if len(md.Get("authorization")) != 0 {
		t.Fatal("key removal retained stale peer authorization")
	}
	lookup(cleared, codes.Unauthenticated)
}

func TestPeerAdminTokenLockTransferAndLookup(t *testing.T) {
	c, key := newPeerFilers(t)
	setRing(c.fsA, 0, []pb.ServerAddress{c.addrA})
	c.fsA.filer.Dlm.InsertLock("moved-lock", time.Now().Add(time.Minute).UnixNano(), "test-lease", "", "test-owner", 4, 2)
	c.fsA.SetupDlmReplication()
	t.Cleanup(c.fsA.dlmHandoffCancel)
	setRing(c.fsA, 0, []pb.ServerAddress{c.addrB})
	setRing(c.fsB, 0, []pb.ServerAddress{c.addrB})
	c.fsA.OnDlmChangeSnapshot([]pb.ServerAddress{c.addrB})
	lock, found := c.fsB.filer.Dlm.GetLock("moved-lock")
	if !found || lock.Owner != "test-owner" || lock.IsBackup || lock.Generation != 4 || lock.Seq != 2 {
		t.Fatal("transfer did not preserve the primary lock")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// The owner trusts the caller's key, not A's newly rotated peer key. A
	// caller-owned lookup must retain that caller's authority across the hop.
	c.fsA.grpcScope.cfg.verificationKeys = func() (security.SigningKey, security.SigningKey) { return key, nil }
	otherKey := make([]byte, 32)
	if _, err := rand.Read(otherKey); err != nil {
		t.Fatal(err)
	}
	c.fsA.filerGuard.UpdateSigningKeys(string(otherKey), 10, "", 0, "", "")
	client := peerClient(t, c, c.addrA, peerAdminToken(key))
	response, err := client.FindLockOwner(ctx, &filer_pb.FindLockOwnerRequest{Name: "moved-lock"})
	if err != nil || response.GetOwner() != "test-owner" {
		t.Fatalf("peer owner lookup failed: %v", err)
	}
}

func TestPeerAdminTokenWithDedicatedGrpcKey(t *testing.T) {
	c, httpKey := newPeerFilers(t)
	c.seed(t, util.FullPath("/realm"))
	grpcKey := make([]byte, 32)
	if _, err := rand.Read(grpcKey); err != nil {
		t.Fatal(err)
	}
	c.fsB.grpcScope.cfg.verificationKeys = func() (security.SigningKey, security.SigningKey) { return grpcKey, nil }
	c.fsB.grpcScope.cfg.adminKeys = func() (security.SigningKey, security.SigningKey) { return c.fsB.filerGuard.VerificationKeys(true) }
	for _, tc := range []struct {
		name  string
		key   security.SigningKey
		admin bool
		want  codes.Code
	}{
		{"HTTP admin", httpKey, true, codes.OK},
		{"HTTP scoped", httpKey, false, codes.Unauthenticated},
		{"gRPC scoped", grpcKey, false, codes.OK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if tc.admin {
				ctx = c.fsA.peerContext(ctx)
			} else {
				token := security.GenJwtForFilerScope(tc.key, time.Minute, "test-caller", []string{"/"}, nil)
				ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer "+string(token)))
			}
			err := pb.WithFilerClient(false, 0, c.addrB, c.fsA.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
				_, err := client.LookupDirectoryEntry(ctx, &filer_pb.LookupDirectoryEntryRequest{Directory: "/", Name: "realm"})
				return err
			})
			if status.Code(err) != tc.want {
				t.Fatalf("lookup code=%v, want %v", status.Code(err), tc.want)
			}
		})
	}
}

// Filer-owned calls sign with the key filesystem gRPC verifies: a dedicated
// grpc_key alone must authenticate them in require-mode, and a peer without the
// shared-key admin fallback must accept them when both keys are configured.
func TestPeerAdminTokenSignedWithGrpcVerificationKey(t *testing.T) {
	for _, tc := range []struct {
		name        string
		sharedKey   bool
		requireMode bool
	}{
		{"gRPC key only in require-mode", false, true},
		{"peer without admin fallback", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, httpKey := newPeerFilers(t)
			c.seed(t, util.FullPath("/realm"))
			grpcKey := make([]byte, 32)
			if _, err := rand.Read(grpcKey); err != nil {
				t.Fatal(err)
			}
			for _, fs := range []*FilerServer{c.fsA, c.fsB} {
				if !tc.sharedKey {
					fs.filerGuard.UpdateSigningKeys("", 10, "", 0, "", "")
				}
				fs.grpcScopeSettings.Store(&grpcScopeSettings{key: grpcKey, requireToken: tc.requireMode})
				fs.grpcScope.cfg.verificationKeys = fs.grpcScopeVerificationKeys
				fs.grpcScope.cfg.requireToken = func() bool { return tc.requireMode }
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			err := pb.WithFilerClient(false, 0, c.addrB, c.fsA.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
				_, err := client.LookupDirectoryEntry(c.fsA.peerContext(ctx), &filer_pb.LookupDirectoryEntryRequest{Directory: "/", Name: "realm"})
				return err
			})
			if err != nil {
				t.Fatalf("filer-owned lookup: %v", err)
			}
			md, _ := metadata.FromOutgoingContext(c.fsA.peerSubscriptionContext(ctx))
			auth := md.Get("authorization")
			if len(auth) != 1 {
				t.Fatalf("subscription authorization=%q, want one bearer", auth)
			}
			subscriptionKey := grpcKey
			if tc.sharedKey {
				subscriptionKey = httpKey
			}
			if !security.IsFilerAdminToken(subscriptionKey, security.EncodedJwt(strings.TrimPrefix(auth[0], security.BearerPrefix))) {
				t.Fatal("subscription bearer is not an admin token for the key its verifier accepts")
			}
			v := util.GetViper()
			v.Set("jwt.filer_signing.subscribe.key", subscriptionTokenKey)
			t.Cleanup(func() { v.Set("jwt.filer_signing.subscribe.key", "") })
			if scope, err := c.fsB.metadataSubscriptionScope(metadata.NewIncomingContext(ctx, md), security.MethodSubscribeLocalMetadata); err != nil || scope != nil {
				t.Fatalf("peer subscription with a subscribe key set: scope=%+v err=%v, want the full stream", scope, err)
			}
		})
	}
}
