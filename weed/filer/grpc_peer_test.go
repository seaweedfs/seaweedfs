package filer

import (
	"context"
	"crypto/rand"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type authenticatedPeerServer struct {
	filer_pb.UnimplementedSeaweedFilerServer
	key   atomic.Value
	mu    sync.Mutex
	calls map[string]int
}

func (s *authenticatedPeerServer) authenticate(ctx context.Context, method string) error {
	if err := security.CheckGrpcAdminToken(ctx, s.key.Load().(security.SigningKey)); err != nil {
		return err
	}
	s.mu.Lock()
	s.calls[method]++
	s.mu.Unlock()
	return nil
}

func (s *authenticatedPeerServer) GetFilerConfiguration(context.Context, *filer_pb.GetFilerConfigurationRequest) (*filer_pb.GetFilerConfigurationResponse, error) {
	return &filer_pb.GetFilerConfigurationResponse{Signature: 7}, nil
}

func (s *authenticatedPeerServer) TraverseBfsMetadata(*filer_pb.TraverseBfsMetadataRequest, filer_pb.SeaweedFiler_TraverseBfsMetadataServer) error {
	return nil
}

func (s *authenticatedPeerServer) SubscribeLocalMetadata(*filer_pb.SubscribeMetadataRequest, filer_pb.SeaweedFiler_SubscribeLocalMetadataServer) error {
	return nil
}

func (s *authenticatedPeerServer) LookupVolume(context.Context, *filer_pb.LookupVolumeRequest) (*filer_pb.LookupVolumeResponse, error) {
	return &filer_pb.LookupVolumeResponse{}, nil
}

func TestPeerMetadataAuthenticationOnEachAttempt(t *testing.T) {
	peer := &authenticatedPeerServer{calls: make(map[string]int)}
	rotate := func() {
		key := make([]byte, 32)
		if _, err := rand.Read(key); err != nil {
			t.Fatal(err)
		}
		peer.key.Store(security.SigningKey(key))
	}
	rotate()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := pb.NewServerAddressWithGrpcPort(lis.Addr().String(), lis.Addr().(*net.TCPAddr).Port)
	server := grpc.NewServer(
		grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			if err := peer.authenticate(ctx, info.FullMethod); err != nil {
				return nil, err
			}
			return handler(ctx, req)
		}),
		grpc.StreamInterceptor(func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			if err := peer.authenticate(stream.Context(), info.FullMethod); err != nil {
				return err
			}
			return handler(srv, stream)
		}),
	)
	filer_pb.RegisterSeaweedFilerServer(server, peer)
	go server.Serve(lis)
	t.Cleanup(func() { server.Stop(); pb.InvalidateGrpcConnection(address.ToGrpcAddress()) })
	dial := grpc.WithTransportCredentials(insecure.NewCredentials())
	var resolved atomic.Int32
	f := &Filer{Signature: 7, GrpcDialOption: dial, PeerContext: func(ctx context.Context) context.Context {
		resolved.Add(1)
		return security.WithFilerPeerCredentials(ctx, peer.key.Load().(security.SigningKey))
	}}
	ma := NewMetaAggregator(f, "127.0.0.1:1", dial)
	t.Cleanup(ma.Shutdown)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for round := 0; round < 2; round++ {
		rotate()
		if err := f.MaybeBootstrapFromOnePeer(ctx, "127.0.0.1:1", []*master_pb.ClusterNodeUpdate{{Address: string(address)}}, time.Now()); err != nil {
			t.Fatal(err)
		}
		if err := ma.traversePeerMetadata(ctx, f, address); err != nil {
			t.Fatal(err)
		}
		if sig, err := ma.readFilerStoreSignature(ctx, address); err != nil || sig != 7 {
			t.Fatalf("configuration lookup: signature=%d err=%v", sig, err)
		}
		stop := make(chan struct{})
		_, err := ma.doSubscribeToOneFiler(f, "127.0.0.1:1", address, 1, stop)
		close(stop)
		if err != nil {
			t.Fatal(err)
		}
		err = pb.WithFilerClient(false, 0, address, dial, func(client filer_pb.SeaweedFilerClient) error {
			wrapped := peerLookupClient{client, f.peerCallContext}
			_, err := wrapped.LookupVolume(ctx, &filer_pb.LookupVolumeRequest{})
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := resolved.Load(); got != 12 {
		t.Fatalf("resolved authentication %d times, want once for each of 12 RPC attempts", got)
	}
	peer.mu.Lock()
	defer peer.mu.Unlock()
	for method, want := range map[string]int{
		filer_pb.SeaweedFiler_GetFilerConfiguration_FullMethodName:  4,
		filer_pb.SeaweedFiler_TraverseBfsMetadata_FullMethodName:    4,
		filer_pb.SeaweedFiler_SubscribeLocalMetadata_FullMethodName: 2,
		filer_pb.SeaweedFiler_LookupVolume_FullMethodName:           2,
	} {
		if got := peer.calls[method]; got != want {
			t.Errorf("%s authenticated calls=%d, want %d", method, got, want)
		}
	}
}
