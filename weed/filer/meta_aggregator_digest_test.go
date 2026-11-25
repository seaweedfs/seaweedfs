package filer

import (
	"context"
	"crypto/fips140"
	"fmt"
	"net"
	"strconv"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func TestCheckPeerDigest(t *testing.T) {
	defer util.SetDigest("")
	cases := []struct {
		own, peer string
		shared    bool
		refuse    bool
	}{
		{util.DigestMd5, util.DigestMd5, false, false},
		{util.DigestSha256, util.DigestSha256, false, false},
		{util.DigestMd5, "", false, false},
		{util.DigestMd5, util.DigestSha256, false, true},
		{util.DigestSha256, util.DigestMd5, false, true},
		{util.DigestMd5, util.DigestSha256, true, false},
	}
	for _, c := range cases {
		if c.own == util.DigestMd5 && fips140.Enforced() {
			continue
		}
		if err := util.SetDigest(c.own); err != nil {
			t.Fatal(err)
		}
		err := checkPeerDigest("peer:8888", c.peer, c.shared)
		if (err != nil) != c.refuse {
			t.Errorf("own %s, peer %q, shared store %v: checkPeerDigest = %v, want refused %v", c.own, c.peer, c.shared, err, c.refuse)
		}
	}
}

// otherDigestFiler is a peer filer with a store of its own that reports a
// digest of the other kind.
type otherDigestFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	digest string
}

func (f *otherDigestFiler) GetFilerConfiguration(context.Context, *filer_pb.GetFilerConfigurationRequest) (*filer_pb.GetFilerConfigurationResponse, error) {
	return &filer_pb.GetFilerConfigurationResponse{Signature: 7, Digest: f.digest}, nil
}

// TestRefusedPeerIsRetiredLikeAnUnreachableOne checks that a peer refused for
// its digest starts the unreachable clock, so the subscribe loop retires it
// from the low watermarks rather than keeping it at zero for every
// aggregated subscriber.
func TestRefusedPeerIsRetiredLikeAnUnreachableOne(t *testing.T) {
	t.Cleanup(func() { util.SetDigest("") })
	if err := util.SetDigest(util.DigestSha256); err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	filer_pb.RegisterSeaweedFilerServer(server, &otherDigestFiler{digest: util.DigestMd5})
	go server.Serve(ln)
	defer server.Stop()
	_, portStr, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	grpcPort, err := strconv.Atoi(portStr)
	if err != nil || grpcPort <= 10000 {
		t.Skipf("listener port %q cannot carry the filer's gRPC offset", portStr)
	}
	peer := pb.ServerAddress(fmt.Sprintf("127.0.0.1:%d", grpcPort-10000))
	ma := NewMetaAggregator(&Filer{Signature: 1}, pb.ServerAddress("127.0.0.1:2"), grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer ma.Shutdown()

	ma.OnPeerUpdate(&master_pb.ClusterNodeUpdate{Address: string(peer), IsAdd: true}, time.Now())
	deadline := time.Now().Add(10 * time.Second)
	for {
		ma.peerChansLock.Lock()
		_, noted := ma.peerUnreachableSinceNs[peer]
		ma.peerChansLock.Unlock()
		if noted {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("a peer refused for its digest never started the unreachable clock")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// silentFiler is a peer filer that accepts the connection but never answers
// GetFilerConfiguration.
type silentFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
}

func (silentFiler) GetFilerConfiguration(ctx context.Context, _ *filer_pb.GetFilerConfigurationRequest) (*filer_pb.GetFilerConfigurationResponse, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

// TestCheckPeerDigestsSkipsASilentPeer checks that a peer that never answers
// its configuration read is skipped after peerDigestReadTimeout, so it cannot
// hold the filer's startup.
func TestCheckPeerDigestsSkipsASilentPeer(t *testing.T) {
	saved := peerDigestReadTimeout
	t.Cleanup(func() { peerDigestReadTimeout = saved })
	peerDigestReadTimeout = 300 * time.Millisecond

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	filer_pb.RegisterSeaweedFilerServer(server, silentFiler{})
	go server.Serve(ln)
	defer server.Stop()
	_, portStr, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	grpcPort, err := strconv.Atoi(portStr)
	if err != nil || grpcPort <= 10000 {
		t.Skipf("listener port %q cannot carry the filer's gRPC offset", portStr)
	}
	peer := pb.ServerAddress(fmt.Sprintf("127.0.0.1:%d", grpcPort-10000))
	f := &Filer{Signature: 1, GrpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials())}

	done := make(chan error, 1)
	go func() {
		done <- f.CheckPeerDigests(context.Background(), "127.0.0.1:2", []*master_pb.ClusterNodeUpdate{{Address: string(peer)}})
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("a silent peer should be skipped, got %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("CheckPeerDigests waited on a peer that never answers")
	}
}
