package s3api

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/s3_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

type filerAuthTestServer struct {
	filer_pb.UnimplementedSeaweedFilerServer

	mu             sync.Mutex
	authorizations [][]string
	allowAnonymous bool
	lookupContent  []byte
}

func (s *filerAuthTestServer) recordAuthorization(ctx context.Context) error {
	md, _ := metadata.FromIncomingContext(ctx)
	values := append([]string(nil), md.Get("authorization")...)
	s.mu.Lock()
	s.authorizations = append(s.authorizations, values)
	allowAnonymous := s.allowAnonymous
	s.mu.Unlock()
	if allowAnonymous {
		return nil
	}
	if len(values) != 1 || !strings.HasPrefix(values[0], security.BearerPrefix) {
		return status.Error(codes.Unauthenticated, "missing filer admin token")
	}
	token := security.EncodedJwt(strings.TrimPrefix(values[0], security.BearerPrefix))
	if !security.IsFilerAdminToken(testFilerAuthKey, token) {
		return status.Error(codes.Unauthenticated, "invalid filer admin token")
	}
	return nil
}

func (s *filerAuthTestServer) CollectionList(ctx context.Context, _ *filer_pb.CollectionListRequest) (*filer_pb.CollectionListResponse, error) {
	return &filer_pb.CollectionListResponse{}, nil
}

func (s *filerAuthTestServer) LookupDirectoryEntry(_ context.Context, _ *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	return &filer_pb.LookupDirectoryEntryResponse{Entry: &filer_pb.Entry{Content: s.lookupContent}}, nil
}

func (s *filerAuthTestServer) LookupVolume(_ context.Context, req *filer_pb.LookupVolumeRequest) (*filer_pb.LookupVolumeResponse, error) {
	locations := make(map[string]*filer_pb.Locations, len(req.GetVolumeIds()))
	for _, volumeID := range req.GetVolumeIds() {
		locations[volumeID] = &filer_pb.Locations{}
	}
	return &filer_pb.LookupVolumeResponse{LocationsMap: locations}, nil
}

func (s *filerAuthTestServer) ListEntries(_ *filer_pb.ListEntriesRequest, stream filer_pb.SeaweedFiler_ListEntriesServer) error {
	return stream.Send(&filer_pb.ListEntriesResponse{Entry: &filer_pb.Entry{Name: "entry"}})
}

func (s *filerAuthTestServer) authorizationValues() [][]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	values := make([][]string, len(s.authorizations))
	for i, auth := range s.authorizations {
		values[i] = append([]string(nil), auth...)
	}
	return values
}

var testFilerAuthKey = security.SigningKey("s3-api-filer-auth-test-key-32-bytes-long")

func startFilerAuthTestServer(t *testing.T, serverImpl *filerAuthTestServer) pb.ServerAddress {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			if err := serverImpl.recordAuthorization(ctx); err != nil {
				return nil, err
			}
			return handler(ctx, req)
		}),
		grpc.StreamInterceptor(func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			if err := serverImpl.recordAuthorization(stream.Context()); err != nil {
				return err
			}
			return handler(srv, stream)
		}),
	)
	filer_pb.RegisterSeaweedFilerServer(grpcServer, serverImpl)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
	})
	address := pb.ServerAddress(fmt.Sprintf("127.0.0.1:1.%d", listener.Addr().(*net.TCPAddr).Port))
	pb.InvalidateGrpcConnection(address.ToGrpcAddress())
	return address
}

func setFilerAuthTestKey(t *testing.T, key string) {
	t.Helper()
	t.Setenv("WEED_JWT_FILER_SIGNING_GRPC_KEY", "")
	t.Setenv("WEED_JWT_FILER_SIGNING_KEY", key)
}

func TestS3FilerCallsCarryFreshAdminAuthForUnaryAndStreaming(t *testing.T) {
	setFilerAuthTestKey(t, string(testFilerAuthKey))
	server := &filerAuthTestServer{}
	address := startFilerAuthTestServer(t, server)
	s3a := &S3ApiServer{
		option: &S3ApiServerOption{
			Filers:         []pb.ServerAddress{address},
			GrpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	}

	if err := s3a.withFilerClient(context.Background(), false, func(client filer_pb.SeaweedFilerClient) error {
		_, err := client.CollectionList(context.Background(), &filer_pb.CollectionListRequest{})
		return err
	}); err != nil {
		t.Fatalf("unary filer call: %v", err)
	}
	if err := s3a.withFilerClient(context.Background(), true, func(client filer_pb.SeaweedFilerClient) error {
		stream, err := client.ListEntries(context.Background(), &filer_pb.ListEntriesRequest{Directory: "/"})
		if err != nil {
			return err
		}
		_, err = stream.Recv()
		return err
	}); err != nil {
		t.Fatalf("streaming filer call: %v", err)
	}

	for i, values := range server.authorizationValues() {
		if len(values) != 1 || !strings.HasPrefix(values[0], security.BearerPrefix) {
			t.Fatalf("call %d did not receive one bearer authorization", i)
		}
	}
}

func TestS3FilerCallsRemainAnonymousWithoutAKey(t *testing.T) {
	setFilerAuthTestKey(t, "")
	server := &filerAuthTestServer{allowAnonymous: true}
	address := startFilerAuthTestServer(t, server)
	s3a := &S3ApiServer{
		option: &S3ApiServerOption{
			Filers:         []pb.ServerAddress{address},
			GrpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	}
	if err := s3a.withFilerClient(context.Background(), false, func(client filer_pb.SeaweedFilerClient) error {
		_, err := client.CollectionList(context.Background(), &filer_pb.CollectionListRequest{})
		return err
	}); err != nil {
		t.Fatalf("anonymous filer call: %v", err)
	}
	values := server.authorizationValues()
	if len(values) != 1 || len(values[0]) != 0 {
		t.Fatalf("filer call carried authorization, want anonymous")
	}
}

func TestInitializedS3FilerClientCarriesAdminAuth(t *testing.T) {
	setFilerAuthTestKey(t, string(testFilerAuthKey))
	server := &filerAuthTestServer{}
	address := startFilerAuthTestServer(t, server)
	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	filerClient := wdclient.NewFilerClient([]pb.ServerAddress{address}, dialOption, "", &wdclient.FilerClientOption{
		FilerTokenSource: security.FilerAdminToken,
	})
	if _, err := filerClient.LookupVolumeIdsAuthoritative(context.Background(), []string{"7"}); err != nil {
		t.Fatalf("initialized filer client volume lookup: %v", err)
	}
	if values := server.authorizationValues(); len(values) != 1 || len(values[0]) != 1 {
		t.Fatalf("initialized filer client did not send one authorization")
	}
}

func TestCircuitBreakerBootstrapCarriesAdminAuth(t *testing.T) {
	setFilerAuthTestKey(t, string(testFilerAuthKey))
	config, err := protojson.Marshal(&s3_pb.S3CircuitBreakerConfig{
		Global: &s3_pb.S3CircuitBreakerOptions{
			Enabled: true,
			Actions: map[string]int64{s3_constants.ACTION_WRITE: 1},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	server := &filerAuthTestServer{lookupContent: config}
	address := startFilerAuthTestServer(t, server)
	cb := NewCircuitBreaker(&S3ApiServerOption{
		Filers:         []pb.ServerAddress{address},
		GrpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
	})
	if !cb.Enabled {
		t.Fatal("circuit breaker configuration was not loaded")
	}
	values := server.authorizationValues()
	if len(values) != 1 || len(values[0]) != 1 || !strings.HasPrefix(values[0][0], security.BearerPrefix) {
		t.Fatal("circuit breaker bootstrap did not carry one bearer authorization")
	}
}

func TestS3FilerConfReadCarriesAdminAuth(t *testing.T) {
	setFilerAuthTestKey(t, string(testFilerAuthKey))
	content, err := protojson.Marshal(&filer_pb.FilerConf{
		Version:   filer.FilerConfVersion,
		Locations: []*filer_pb.FilerConf_PathConf{{LocationPrefix: "/buckets/", Collection: "buckets"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	server := &filerAuthTestServer{lookupContent: content}
	address := startFilerAuthTestServer(t, server)
	fc, err := filer.ReadFilerConfFromFilers([]pb.ServerAddress{address}, grpc.WithTransportCredentials(insecure.NewCredentials()), nil, security.FilerAdminToken)
	if err != nil {
		t.Fatalf("read filer.conf: %v", err)
	}
	if got := fc.MatchStorageRule("/buckets/example").GetCollection(); got != "buckets" {
		t.Fatalf("collection = %q, want buckets", got)
	}
	values := server.authorizationValues()
	if len(values) != 1 || len(values[0]) != 1 || !strings.HasPrefix(values[0][0], security.BearerPrefix) {
		t.Fatal("filer.conf read did not carry one bearer authorization")
	}
}
