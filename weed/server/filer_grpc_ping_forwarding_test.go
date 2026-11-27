package weed_server

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const pingForwardingKey = "ping-forwarding-test-key"

func newPingFilers(t *testing.T, requireToken bool) (*twoFilers, security.SigningKey) {
	t.Helper()
	c := newTwoFilers(t, func(fs *FilerServer) {
		fs.filerGuard = security.NewGuard(nil, pingForwardingKey, 60, "", 0, "", "")
		fs.grpcScope = newFilerGrpcScope(scopeConfig{
			signingKey:       fs.filerGuard.SigningKey,
			verificationKeys: func() (security.SigningKey, security.SigningKey) { return fs.filerGuard.VerificationKeys(true) },
			requireToken:     func() bool { return requireToken },
		}, fs)
	})

	// The fixture does not run the normal master peer subscription. Treat B as
	// a known peer so A's real target gate admits the probe without starting a
	// subscription goroutine solely for this test.
	ma := filer.NewMetaAggregator(c.fsA.filer, c.addrB, nil)
	c.fsA.filer.MetaAggregator = ma
	t.Cleanup(ma.Shutdown)
	return c, security.SigningKey(pingForwardingKey)
}

func TestFilerPingForwardsCallerBearerToRequireModePeer(t *testing.T) {
	c, key := newPingFilers(t, true)
	caller := peerAdminToken(key)
	client := peerClient(t, c, c.addrA, caller)

	_, err := client.Ping(context.Background(), &filer_pb.PingRequest{Target: string(c.addrB), TargetType: cluster.FilerType})
	if err != nil {
		t.Fatalf("target filer Ping: %v", err)
	}
	assertForwardedPingBearer(t, c.callsB, caller)
}

func assertForwardedPingBearer(t *testing.T, calls *receivedUpdates, caller string) {
	t.Helper()
	calls.mu.Lock()
	defer calls.mu.Unlock()
	if len(calls.pingAuthorizations) != 1 {
		t.Fatalf("receiving filer saw %d Ping calls, want one", len(calls.pingAuthorizations))
	}
	if !slices.Equal(calls.pingAuthorizations[0], []string{"Bearer " + caller}) {
		t.Fatal("receiving filer did not get exactly the caller's bearer")
	}
}

func TestFilerPingRejectsTokenlessCallerInRequireMode(t *testing.T) {
	c, _ := newPingFilers(t, true)
	client := peerClient(t, c, c.addrA, "")

	_, err := client.Ping(context.Background(), &filer_pb.PingRequest{Target: string(c.addrB), TargetType: cluster.FilerType})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("tokenless target Ping error=%v, want Unauthenticated", err)
	}
}

func TestFilerPingDoesNotMintPeerAdminForTokenlessCaller(t *testing.T) {
	c, _ := newPingFilers(t, false)
	c.fsB.grpcScope.cfg.requireToken = func() bool { return true }
	client := peerClient(t, c, c.addrA, "")

	_, err := client.Ping(context.Background(), &filer_pb.PingRequest{Target: string(c.addrB), TargetType: cluster.FilerType})
	if err == nil || !strings.Contains(err.Error(), "filer: gRPC token authentication is required") {
		t.Fatalf("tokenless target Ping against require-mode peer error=%v, want remote Unauthenticated", err)
	}
}

func TestFilerPingPreservesTokenlessCompatibilityWhenRequireModeOff(t *testing.T) {
	c, _ := newPingFilers(t, false)
	client := peerClient(t, c, c.addrA, "")

	if _, err := client.Ping(context.Background(), &filer_pb.PingRequest{Target: string(c.addrB), TargetType: cluster.FilerType}); err != nil {
		t.Fatalf("tokenless target Ping in compatibility mode: %v", err)
	}
}

func TestFilerPingReplacesStaleOutgoingAuthorization(t *testing.T) {
	c, key := newPingFilers(t, true)
	caller := peerAdminToken(key)
	stale := peerAdminToken(security.SigningKey("stale-ping-key"))
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+caller))
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer "+stale))

	if _, err := c.fsA.Ping(ctx, &filer_pb.PingRequest{Target: string(c.addrB), TargetType: cluster.FilerType}); err != nil {
		t.Fatalf("target filer Ping with stale outgoing authorization: %v", err)
	}
	assertForwardedPingBearer(t, c.callsB, caller)
}
