package iamapi

import (
	"context"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type iamFilerAuthTestServer struct {
	filer_pb.UnimplementedSeaweedFilerServer

	mu             sync.Mutex
	authorizations [][]string
	created        bool
}

func (s *iamFilerAuthTestServer) recordAuthorization(ctx context.Context) error {
	md, _ := metadata.FromIncomingContext(ctx)
	values := append([]string(nil), md.Get("authorization")...)
	s.mu.Lock()
	s.authorizations = append(s.authorizations, values)
	s.mu.Unlock()
	if len(values) != 1 || !strings.HasPrefix(values[0], security.BearerPrefix) {
		return status.Error(codes.Unauthenticated, "missing filer admin token")
	}
	token := security.EncodedJwt(strings.TrimPrefix(values[0], security.BearerPrefix))
	if !security.IsFilerAdminToken(iamFilerAuthTestKey, token) {
		return status.Error(codes.Unauthenticated, "invalid filer admin token")
	}
	return nil
}

func (s *iamFilerAuthTestServer) LookupDirectoryEntry(context.Context, *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	return &filer_pb.LookupDirectoryEntryResponse{}, nil
}

func (s *iamFilerAuthTestServer) CreateEntry(_ context.Context, _ *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	s.mu.Lock()
	s.created = true
	s.mu.Unlock()
	return &filer_pb.CreateEntryResponse{}, nil
}

func (s *iamFilerAuthTestServer) authorizationValues() [][]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	values := make([][]string, len(s.authorizations))
	for i, auth := range s.authorizations {
		values[i] = append([]string(nil), auth...)
	}
	return values
}

var iamFilerAuthTestKey = security.SigningKey("iam-api-filer-auth-test-key-32-bytes-long")

func startIamFilerAuthTestServer(t *testing.T, serverImpl *iamFilerAuthTestServer) pb.ServerAddress {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if err := serverImpl.recordAuthorization(ctx); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}))
	filer_pb.RegisterSeaweedFilerServer(grpcServer, serverImpl)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
	})
	_, portText, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	grpcPort, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatal(err)
	}
	return pb.NewServerAddressWithGrpcPort(listener.Addr().String(), grpcPort)
}

func TestPutS3ApiConfigurationToFilerCarriesAdminAuth(t *testing.T) {
	t.Setenv("WEED_JWT_FILER_SIGNING_GRPC_KEY", "")
	t.Setenv("WEED_JWT_FILER_SIGNING_KEY", string(iamFilerAuthTestKey))
	server := &iamFilerAuthTestServer{}
	address := startIamFilerAuthTestServer(t, server)
	configure := &IamS3ApiConfigure{
		option: &IamServerOption{
			Filers:         []pb.ServerAddress{address},
			GrpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	}
	if err := configure.PutS3ApiConfigurationToFiler(&iam_pb.S3ApiConfiguration{}); err != nil {
		t.Fatalf("put IAM configuration: %v", err)
	}
	s := server.authorizationValues()
	if len(s) != 2 || !server.created {
		t.Fatalf("persistence made %d authenticated calls, created=%v; want lookup plus create", len(s), server.created)
	}
	for i, values := range s {
		if len(values) != 1 || !strings.HasPrefix(values[0], security.BearerPrefix) {
			t.Fatalf("persistence call %d did not carry one bearer authorization", i)
		}
	}
}
