package filersink

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/replication/source"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type adapterAuthFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	key              security.SigningKey
	allowAnonymous   bool
	failFirstStream  bool
	subscribeCalls   atomic.Int32
	createEntryCalls atomic.Int32
	configCalls      atomic.Int32
}

func (f *adapterAuthFiler) authenticate(ctx context.Context) error {
	md, _ := metadata.FromIncomingContext(ctx)
	values := md.Get("authorization")
	if len(values) == 0 && f.allowAnonymous {
		return nil
	}
	if len(values) != 1 || !strings.HasPrefix(values[0], security.BearerPrefix) {
		return status.Error(codes.Unauthenticated, "missing filer authorization")
	}
	if !security.IsFilerAdminToken(f.key, security.EncodedJwt(strings.TrimPrefix(values[0], security.BearerPrefix))) {
		return status.Error(codes.Unauthenticated, "invalid filer authorization")
	}
	return nil
}

func (f *adapterAuthFiler) SubscribeMetadata(_ *filer_pb.SubscribeMetadataRequest, stream filer_pb.SeaweedFiler_SubscribeMetadataServer) error {
	if f.subscribeCalls.Add(1) == 1 && f.failFirstStream {
		return status.Error(codes.Unavailable, "subscription dropped")
	}
	return stream.Send(&filer_pb.SubscribeMetadataResponse{TsNs: 1})
}

func (f *adapterAuthFiler) GetFilerConfiguration(context.Context, *filer_pb.GetFilerConfigurationRequest) (*filer_pb.GetFilerConfigurationResponse, error) {
	f.configCalls.Add(1)
	return &filer_pb.GetFilerConfigurationResponse{Signature: 1}, nil
}

func (f *adapterAuthFiler) CreateEntry(_ context.Context, _ *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	f.createEntryCalls.Add(1)
	return &filer_pb.CreateEntryResponse{}, nil
}

func adapterTestKey(t *testing.T) security.SigningKey {
	t.Helper()
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		t.Fatal(err)
	}
	return security.SigningKey(hex.EncodeToString(buf))
}

func startAdapterAuthFiler(t *testing.T, filer *adapterAuthFiler) pb.ServerAddress {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(
		grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			if err := filer.authenticate(ctx); err != nil {
				return nil, err
			}
			return handler(ctx, req)
		}),
		grpc.StreamInterceptor(func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			if err := filer.authenticate(stream.Context()); err != nil {
				return err
			}
			return handler(srv, stream)
		}),
	)
	filer_pb.RegisterSeaweedFilerServer(server, filer)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})
	port := listener.Addr().(*net.TCPAddr).Port
	return pb.NewServerAddressWithGrpcPort(listener.Addr().String(), port)
}

func TestFilerSyncAdaptersUseIndependentEndpointTokens(t *testing.T) {
	sourceKey := adapterTestKey(t)
	destinationKey := adapterTestKey(t)
	if string(sourceKey) == string(destinationKey) {
		t.Fatal("test endpoint keys unexpectedly match")
	}
	sourceFiler := &adapterAuthFiler{key: sourceKey, failFirstStream: true}
	destinationFiler := &adapterAuthFiler{key: destinationKey}
	sourceAddress := startAdapterAuthFiler(t, sourceFiler)
	destinationAddress := startAdapterAuthFiler(t, destinationFiler)
	dial := grpc.WithTransportCredentials(insecure.NewCredentials())

	sourceClient := &source.FilerSource{}
	if err := sourceClient.DoInitialize(sourceAddress.String(), sourceAddress.ToGrpcAddress(), "/", false); err != nil {
		t.Fatal(err)
	}
	sourceClient.SetGrpcDialOption(dial)
	sourceCalls := atomic.Int32{}
	sourceClient.SetFilerTokenSource(func(context.Context) (security.EncodedJwt, error) {
		sourceCalls.Add(1)
		return security.GenJwtForFilerAdmin(sourceKey, 60), nil
	})
	for attempt := 0; attempt < 2; attempt++ {
		err := sourceClient.WithFilerClient(true, func(client filer_pb.SeaweedFilerClient) error {
			stream, err := client.SubscribeMetadata(context.Background(), &filer_pb.SubscribeMetadataRequest{PathPrefix: "/"})
			if err != nil {
				return err
			}
			_, err = stream.Recv()
			return err
		})
		if attempt == 0 && status.Code(err) != codes.Unavailable {
			t.Fatalf("first source metadata attempt error = %v, want Unavailable", err)
		}
		if attempt == 1 && err != nil {
			t.Fatalf("reconnected source metadata subscription: %v", err)
		}
	}
	if sourceCalls.Load() < 2 || sourceFiler.subscribeCalls.Load() != 2 {
		t.Fatalf("source token/subscription calls = %d/%d, want at least 2/2", sourceCalls.Load(), sourceFiler.subscribeCalls.Load())
	}

	destinationClient := &FilerSink{}
	if err := destinationClient.DoInitialize(destinationAddress.String(), destinationAddress.ToGrpcAddress(), "/", "", "", 0, "", dial, false); err != nil {
		t.Fatal(err)
	}
	destinationCalls := atomic.Int32{}
	destinationClient.SetFilerTokenSource(func(context.Context) (security.EncodedJwt, error) {
		destinationCalls.Add(1)
		return security.GenJwtForFilerAdmin(destinationKey, 60), nil
	})
	if err := destinationClient.CreateEntry("/file", &filer_pb.Entry{Name: "file"}, nil); err != nil {
		t.Fatalf("destination create: %v", err)
	}
	if destinationCalls.Load() < 1 || destinationFiler.createEntryCalls.Load() != 1 {
		t.Fatalf("destination token/create calls = %d/%d, want at least 1/1", destinationCalls.Load(), destinationFiler.createEntryCalls.Load())
	}
}

func TestFilerSourceTokenResolutionFailureIsFailClosedAndNilIsLegacy(t *testing.T) {
	key := adapterTestKey(t)
	filer := &adapterAuthFiler{key: key, allowAnonymous: true}
	address := startAdapterAuthFiler(t, filer)
	dial := grpc.WithTransportCredentials(insecure.NewCredentials())
	client := &source.FilerSource{}
	if err := client.DoInitialize(address.String(), address.ToGrpcAddress(), "/", false); err != nil {
		t.Fatal(err)
	}
	client.SetGrpcDialOption(dial)
	client.SetFilerTokenSource(func(context.Context) (security.EncodedJwt, error) {
		return "", status.Error(codes.Unauthenticated, "token source unavailable")
	})
	if err := client.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		_, err := client.GetFilerConfiguration(context.Background(), &filer_pb.GetFilerConfigurationRequest{})
		return err
	}); err == nil {
		t.Fatal("token source failure unexpectedly reached the filer")
	}
	if filer.configCalls.Load() != 0 || filer.subscribeCalls.Load() != 0 {
		t.Fatalf("token source failure unexpectedly reached filer: configuration=%d subscriptions=%d", filer.configCalls.Load(), filer.subscribeCalls.Load())
	}

	client.SetFilerTokenSource(nil)
	if err := client.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		_, err := client.GetFilerConfiguration(context.Background(), &filer_pb.GetFilerConfigurationRequest{})
		return err
	}); err != nil {
		t.Fatalf("nil token source did not preserve the legacy call: %v", err)
	}
	if filer.configCalls.Load() != 1 {
		t.Fatalf("legacy call count = %d, want 1", filer.configCalls.Load())
	}
}
