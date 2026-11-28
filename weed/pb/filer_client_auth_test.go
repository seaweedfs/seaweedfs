package pb

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

func filerAuthTestKey(t *testing.T) security.SigningKey {
	t.Helper()
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		t.Fatal(err)
	}
	return security.SigningKey(hex.EncodeToString(bytes))
}

type authRecordingFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer

	mu             sync.Mutex
	authorizations [][]string
}

func (s *authRecordingFiler) record(ctx context.Context) {
	md, _ := metadata.FromIncomingContext(ctx)
	values := append([]string(nil), md.Get("authorization")...)
	s.mu.Lock()
	s.authorizations = append(s.authorizations, values)
	s.mu.Unlock()
}

func (s *authRecordingFiler) LookupDirectoryEntry(ctx context.Context, _ *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	s.record(ctx)
	return &filer_pb.LookupDirectoryEntryResponse{}, nil
}

func (s *authRecordingFiler) ListEntries(_ *filer_pb.ListEntriesRequest, stream filer_pb.SeaweedFiler_ListEntriesServer) error {
	s.record(stream.Context())
	return stream.Send(&filer_pb.ListEntriesResponse{Entry: &filer_pb.Entry{Name: "entry"}})
}

func (s *authRecordingFiler) SubscribeMetadata(_ *filer_pb.SubscribeMetadataRequest, stream filer_pb.SeaweedFiler_SubscribeMetadataServer) error {
	s.record(stream.Context())
	return nil
}

func (s *authRecordingFiler) authorizationValues() [][]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	values := make([][]string, len(s.authorizations))
	for i, auth := range s.authorizations {
		values[i] = append([]string(nil), auth...)
	}
	return values
}

func validAdminAuthorization(values []string, key security.SigningKey) bool {
	if len(values) != 1 || !strings.HasPrefix(values[0], security.BearerPrefix) {
		return false
	}
	return security.IsFilerAdminToken(key, security.EncodedJwt(strings.TrimPrefix(values[0], security.BearerPrefix)))
}

func startAuthRecordingFiler(t *testing.T) (*authRecordingFiler, ServerAddress) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	filer := &authRecordingFiler{}
	filer_pb.RegisterSeaweedFilerServer(server, filer)
	go func() {
		_ = server.Serve(listener)
	}()
	t.Cleanup(func() {
		server.Stop()
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
	return filer, NewServerAddressWithGrpcPort(listener.Addr().String(), grpcPort)
}

func callLookupWithAuth(t *testing.T, address ServerAddress, source security.FilerTokenSource) error {
	t.Helper()
	return WithGrpcFilerClient(false, 0, address, grpc.WithTransportCredentials(insecure.NewCredentials()), source, func(client filer_pb.SeaweedFilerClient) error {
		_, err := client.LookupDirectoryEntry(context.Background(), &filer_pb.LookupDirectoryEntryRequest{Directory: "/", Name: "entry"})
		return err
	})
}

func pooledFilerConnection(t *testing.T, address ServerAddress) *grpc.ClientConn {
	t.Helper()
	grpcClientsLock.Lock()
	defer grpcClientsLock.Unlock()
	client, ok := grpcClients[address.ToGrpcAddress()]
	if !ok {
		t.Fatal("pooled filer connection was not created")
	}
	return client.ClientConn
}

func TestWithGrpcFilerClientPooledUnaryAuthAnonymousBothOrders(t *testing.T) {
	for _, firstAuth := range []bool{false, true} {
		t.Run(map[bool]string{false: "anonymous-first", true: "authenticated-first"}[firstAuth], func(t *testing.T) {
			server, address := startAuthRecordingFiler(t)
			t.Cleanup(func() { InvalidateGrpcConnection(address.ToGrpcAddress()) })
			key := filerAuthTestKey(t)
			token := security.GenJwtForFilerAdmin(key, 60)
			source := func(context.Context) (security.EncodedJwt, error) { return token, nil }

			var pooledConn *grpc.ClientConn
			if firstAuth {
				if err := callLookupWithAuth(t, address, source); err != nil {
					t.Fatalf("authenticated first call: %v", err)
				}
				pooledConn = pooledFilerConnection(t, address)
				if err := callLookupWithAuth(t, address, nil); err != nil {
					t.Fatalf("anonymous second call: %v", err)
				}
			} else {
				if err := callLookupWithAuth(t, address, nil); err != nil {
					t.Fatalf("anonymous first call: %v", err)
				}
				pooledConn = pooledFilerConnection(t, address)
				if err := callLookupWithAuth(t, address, source); err != nil {
					t.Fatalf("authenticated second call: %v", err)
				}
			}
			if pooledConn != pooledFilerConnection(t, address) {
				t.Fatal("authenticated and anonymous calls did not reuse the same pooled client connection")
			}

			auth := server.authorizationValues()
			if len(auth) != 2 {
				t.Fatalf("server saw %d calls, want two", len(auth))
			}
			for i, values := range auth {
				wantAuth := (i == 0) == firstAuth
				if !wantAuth {
					if len(values) != 0 {
						t.Fatalf("call %d unexpectedly carried authorization metadata", i)
					}
					continue
				}
				if !validAdminAuthorization(values, key) {
					t.Fatalf("call %d did not receive a valid admin token", i)
				}
			}
		})
	}
}

func TestWithGrpcFilerClientResolvesPooledTokenOnEveryUnaryCall(t *testing.T) {
	server, address := startAuthRecordingFiler(t)
	t.Cleanup(func() { InvalidateGrpcConnection(address.ToGrpcAddress()) })
	keys := []security.SigningKey{
		filerAuthTestKey(t),
		filerAuthTestKey(t),
	}
	tokens := []security.EncodedJwt{
		security.GenJwtForFilerAdmin(keys[0], 60),
		security.GenJwtForFilerAdmin(keys[1], 60),
	}
	index := 0
	source := func(context.Context) (security.EncodedJwt, error) {
		token := tokens[index]
		index++
		return token, nil
	}
	for i := 0; i < len(tokens); i++ {
		if err := callLookupWithAuth(t, address, source); err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
	}
	if index != len(tokens) {
		t.Fatalf("token source called %d times, want %d", index, len(tokens))
	}
	auth := server.authorizationValues()
	if len(auth) != len(tokens) {
		t.Fatalf("server saw %d calls, want %d", len(auth), len(tokens))
	}
	for i, values := range auth {
		if !validAdminAuthorization(values, keys[i]) {
			t.Fatalf("call %d did not receive a valid admin token", i)
		}
		if values[0] != security.BearerPrefix+string(tokens[i]) {
			t.Fatalf("call %d did not receive its freshly resolved token", i)
		}
	}
}

func TestWithGrpcFilerClientStreamingReopensWithFreshToken(t *testing.T) {
	server, address := startAuthRecordingFiler(t)
	keys := []security.SigningKey{filerAuthTestKey(t), filerAuthTestKey(t)}
	tokens := []security.EncodedJwt{
		security.GenJwtForFilerAdmin(keys[0], 60),
		security.GenJwtForFilerAdmin(keys[1], 60),
	}
	call := 0
	source := func(context.Context) (security.EncodedJwt, error) {
		token := tokens[call]
		call++
		return token, nil
	}
	for i := range tokens {
		err := WithGrpcFilerClient(true, 0, address, grpc.WithTransportCredentials(insecure.NewCredentials()), source, func(client filer_pb.SeaweedFilerClient) error {
			stream, err := client.ListEntries(context.Background(), &filer_pb.ListEntriesRequest{Directory: "/"})
			if err != nil {
				return err
			}
			if _, err := stream.Recv(); err != nil {
				return err
			}
			_, err = stream.Recv()
			return err
		})
		if err != io.EOF {
			t.Fatalf("stream call %d returned an unexpected result", i)
		}
	}
	if call != len(tokens) {
		t.Fatalf("token source called %d times, want %d", call, len(tokens))
	}
	auth := server.authorizationValues()
	if len(auth) != len(tokens) {
		t.Fatalf("server saw %d stream calls, want %d", len(auth), len(tokens))
	}
	for i, values := range auth {
		if !validAdminAuthorization(values, keys[i]) || values[0] != security.BearerPrefix+string(tokens[i]) {
			t.Fatalf("stream call %d did not receive its fresh token", i)
		}
	}
}

func TestWithGrpcFilerClientStreamingListEntriesCarriesToken(t *testing.T) {
	server, address := startAuthRecordingFiler(t)
	key := filerAuthTestKey(t)
	token := security.GenJwtForFilerAdmin(key, 60)
	sourceCalls := 0
	source := func(context.Context) (security.EncodedJwt, error) {
		sourceCalls++
		return token, nil
	}
	err := WithGrpcFilerClient(true, 0, address, grpc.WithTransportCredentials(insecure.NewCredentials()), source, func(client filer_pb.SeaweedFilerClient) error {
		stream, err := client.ListEntries(context.Background(), &filer_pb.ListEntriesRequest{Directory: "/"})
		if err != nil {
			return err
		}
		response, err := stream.Recv()
		if err != nil {
			return err
		}
		if response.GetEntry().GetName() != "entry" {
			t.Fatalf("stream entry = %q, want entry", response.GetEntry().GetName())
		}
		_, err = stream.Recv()
		if err != io.EOF {
			t.Fatalf("second stream receive = %v, want EOF", err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("streaming call: %v", err)
	}
	if sourceCalls != 1 {
		t.Fatalf("token source called %d times, want one stream-open resolution", sourceCalls)
	}
	auth := server.authorizationValues()
	if len(auth) != 1 || !validAdminAuthorization(auth[0], key) {
		t.Fatal("ListEntries did not receive a valid admin token")
	}
}

func TestFollowMetadataUsesEndpointTokenSource(t *testing.T) {
	server, address := startAuthRecordingFiler(t)
	key := filerAuthTestKey(t)
	token := security.GenJwtForFilerAdmin(key, 60)
	sourceCalls := 0
	err := FollowMetadata(address, grpc.WithTransportCredentials(insecure.NewCredentials()), &MetadataFollowOption{
		ClientName:       "auth-test",
		EventErrorType:   DontLogError,
		FilerTokenSource: func(context.Context) (security.EncodedJwt, error) { sourceCalls++; return token, nil },
	}, func(*filer_pb.SubscribeMetadataResponse) error { return nil })
	if err != nil {
		t.Fatalf("metadata follow: %v", err)
	}
	if sourceCalls != 1 {
		t.Fatalf("token source called %d times, want one subscription attempt", sourceCalls)
	}
	auth := server.authorizationValues()
	if len(auth) != 1 || !validAdminAuthorization(auth[0], key) {
		t.Fatal("subscription did not receive a valid endpoint admin token")
	}
}
