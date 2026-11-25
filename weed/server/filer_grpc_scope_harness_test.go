package weed_server

import (
	"context"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	scopeTestKey = "scope-test-signing-key"
	aliceWS      = "/buckets/t1/users/alice/workspace"
	bobWS        = "/buckets/t1/users/bob/workspace"
	groupG1      = "/buckets/t1/groups/g1"
)

type scopeRecordedCall struct {
	method string
	req    proto.Message
}

type scopeFakeFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer

	mu      sync.Mutex
	calls   []scopeRecordedCall
	entries map[string]*filer_pb.Entry
	kv      map[string][]byte
	kvCalls int
	findErr error
}

func newScopeFakeFiler() *scopeFakeFiler {
	return &scopeFakeFiler{entries: make(map[string]*filer_pb.Entry), kv: make(map[string][]byte)}
}

func (f *scopeFakeFiler) record(method string, req proto.Message) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, scopeRecordedCall{method: method, req: proto.Clone(req)})
}

func (f *scopeFakeFiler) received(method string) []proto.Message {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []proto.Message
	for _, call := range f.calls {
		if call.method == method {
			out = append(out, call.req)
		}
	}
	return out
}

func (f *scopeFakeFiler) total() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func (f *scopeFakeFiler) put(path string, entry *filer_pb.Entry) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.entries[path] = proto.Clone(entry).(*filer_pb.Entry)
}

func (f *scopeFakeFiler) putDir(p string) {
	_, name := scopeSplitPath(p)
	f.put(p, &filer_pb.Entry{Name: name, IsDirectory: true})
}

func (f *scopeFakeFiler) setFindErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.findErr = err
}

func (f *scopeFakeFiler) putKV(key []byte, value []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.kv[string(key)] = append([]byte(nil), value...)
}

func (f *scopeFakeFiler) kvLookups() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.kvCalls
}

func scopeJoin(dir, name string) string {
	if dir == "/" {
		return "/" + name
	}
	return dir + "/" + name
}

func (f *scopeFakeFiler) scopeFindEntry(_ context.Context, path string) (*filer_pb.Entry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.findErr != nil {
		return nil, f.findErr
	}
	entry := f.entries[path]
	if entry == nil {
		return nil, nil
	}
	return proto.Clone(entry).(*filer_pb.Entry), nil
}

func (f *scopeFakeFiler) scopeCollection(context.Context, string) (string, error) {
	return "", nil
}

func (f *scopeFakeFiler) scopeKvGet(_ context.Context, key []byte) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.kvCalls++
	return append([]byte(nil), f.kv[string(key)]...), nil
}

func (f *scopeFakeFiler) LookupDirectoryEntry(_ context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	f.record("LookupDirectoryEntry", req)
	return &filer_pb.LookupDirectoryEntryResponse{}, nil
}

func (f *scopeFakeFiler) LookupDirectoryEntries(_ context.Context, req *filer_pb.LookupDirectoryEntriesRequest) (*filer_pb.LookupDirectoryEntriesResponse, error) {
	f.record("LookupDirectoryEntries", req)
	resp := &filer_pb.LookupDirectoryEntriesResponse{Results: make([]*filer_pb.LookupDirectoryEntryResult, 0, len(req.GetRequests()))}
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, lookup := range req.GetRequests() {
		entry := f.entries[scopeJoin(lookup.GetDirectory(), lookup.GetName())]
		result := &filer_pb.LookupDirectoryEntryResult{Found: entry != nil}
		if entry != nil {
			result.Entry = proto.Clone(entry).(*filer_pb.Entry)
		}
		resp.Results = append(resp.Results, result)
	}
	return resp, nil
}

func (f *scopeFakeFiler) ListEntries(req *filer_pb.ListEntriesRequest, stream grpc.ServerStreamingServer[filer_pb.ListEntriesResponse]) error {
	f.record("ListEntries", req)
	f.mu.Lock()
	entries := make([]*filer_pb.Entry, 0)
	for path, entry := range f.entries {
		lastSlash := strings.LastIndex(path, "/")
		if lastSlash >= 0 && path[:lastSlash] == req.GetDirectory() {
			entries = append(entries, proto.Clone(entry).(*filer_pb.Entry))
		}
	}
	f.mu.Unlock()
	for _, entry := range entries {
		if err := stream.Send(&filer_pb.ListEntriesResponse{Entry: entry}); err != nil {
			return err
		}
	}
	return nil
}

func (f *scopeFakeFiler) CreateEntry(_ context.Context, req *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	f.record("CreateEntry", req)
	return &filer_pb.CreateEntryResponse{}, nil
}

func (f *scopeFakeFiler) UpdateEntry(_ context.Context, req *filer_pb.UpdateEntryRequest) (*filer_pb.UpdateEntryResponse, error) {
	f.record("UpdateEntry", req)
	return &filer_pb.UpdateEntryResponse{}, nil
}

func (f *scopeFakeFiler) TouchAccessTime(_ context.Context, req *filer_pb.TouchAccessTimeRequest) (*filer_pb.TouchAccessTimeResponse, error) {
	f.record("TouchAccessTime", req)
	return &filer_pb.TouchAccessTimeResponse{}, nil
}

func (f *scopeFakeFiler) AppendToEntry(_ context.Context, req *filer_pb.AppendToEntryRequest) (*filer_pb.AppendToEntryResponse, error) {
	f.record("AppendToEntry", req)
	return &filer_pb.AppendToEntryResponse{}, nil
}

func (f *scopeFakeFiler) DeleteEntry(_ context.Context, req *filer_pb.DeleteEntryRequest) (*filer_pb.DeleteEntryResponse, error) {
	f.record("DeleteEntry", req)
	return &filer_pb.DeleteEntryResponse{}, nil
}

func (f *scopeFakeFiler) PosixLock(_ context.Context, req *filer_pb.PosixLockRequest) (*filer_pb.PosixLockResponse, error) {
	f.record("PosixLock", req)
	return &filer_pb.PosixLockResponse{}, nil
}

func (f *scopeFakeFiler) AtomicRenameEntry(_ context.Context, req *filer_pb.AtomicRenameEntryRequest) (*filer_pb.AtomicRenameEntryResponse, error) {
	f.record("AtomicRenameEntry", req)
	return &filer_pb.AtomicRenameEntryResponse{}, nil
}

func (f *scopeFakeFiler) StreamRenameEntry(req *filer_pb.StreamRenameEntryRequest, _ grpc.ServerStreamingServer[filer_pb.StreamRenameEntryResponse]) error {
	f.record("StreamRenameEntry", req)
	return nil
}

func (f *scopeFakeFiler) StreamMutateEntry(stream grpc.BidiStreamingServer[filer_pb.StreamMutateEntryRequest, filer_pb.StreamMutateEntryResponse]) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		f.record("StreamMutateEntry", req)
		if err := stream.Send(&filer_pb.StreamMutateEntryResponse{RequestId: req.GetRequestId(), IsLast: true}); err != nil {
			return err
		}
	}
}

func (f *scopeFakeFiler) AssignVolume(_ context.Context, req *filer_pb.AssignVolumeRequest) (*filer_pb.AssignVolumeResponse, error) {
	f.record("AssignVolume", req)
	return &filer_pb.AssignVolumeResponse{}, nil
}

func (f *scopeFakeFiler) LookupVolume(_ context.Context, req *filer_pb.LookupVolumeRequest) (*filer_pb.LookupVolumeResponse, error) {
	f.record("LookupVolume", req)
	return &filer_pb.LookupVolumeResponse{}, nil
}

func (f *scopeFakeFiler) Statistics(_ context.Context, req *filer_pb.StatisticsRequest) (*filer_pb.StatisticsResponse, error) {
	f.record("Statistics", req)
	return &filer_pb.StatisticsResponse{}, nil
}

func (f *scopeFakeFiler) Ping(_ context.Context, req *filer_pb.PingRequest) (*filer_pb.PingResponse, error) {
	f.record("Ping", req)
	return &filer_pb.PingResponse{}, nil
}

func (f *scopeFakeFiler) GetFilerConfiguration(_ context.Context, req *filer_pb.GetFilerConfigurationRequest) (*filer_pb.GetFilerConfigurationResponse, error) {
	f.record("GetFilerConfiguration", req)
	return &filer_pb.GetFilerConfigurationResponse{}, nil
}

func (f *scopeFakeFiler) MountRegister(_ context.Context, req *filer_pb.MountRegisterRequest) (*filer_pb.MountRegisterResponse, error) {
	f.record("MountRegister", req)
	return &filer_pb.MountRegisterResponse{}, nil
}

func (f *scopeFakeFiler) SubscribeMetadata(req *filer_pb.SubscribeMetadataRequest, _ grpc.ServerStreamingServer[filer_pb.SubscribeMetadataResponse]) error {
	f.record("SubscribeMetadata", req)
	return nil
}

func (f *scopeFakeFiler) SubscribeLocalMetadata(req *filer_pb.SubscribeMetadataRequest, _ grpc.ServerStreamingServer[filer_pb.SubscribeMetadataResponse]) error {
	f.record("SubscribeLocalMetadata", req)
	return nil
}

type scopeHarness struct {
	t     *testing.T
	fake  *scopeFakeFiler
	scope *filerGrpcScope
	key   atomic.Value
	addr  string
}

func newScopeHarness(t *testing.T) *scopeHarness {
	t.Helper()
	fake := newScopeFakeFiler()
	h := &scopeHarness{t: t, fake: fake}
	h.key.Store(scopeTestKey)
	h.scope = newFilerGrpcScope(scopeConfig{
		signingKey: func() security.SigningKey { return security.SigningKey(h.key.Load().(string)) },
	}, fake)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := grpc.NewServer(h.scope.ServerOptions()...)
	filer_pb.RegisterSeaweedFilerServer(server, fake)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})
	h.addr = listener.Addr().String()
	return h
}

type scopeToken string

func (s scopeToken) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	if s == "" {
		return nil, nil
	}
	return map[string]string{"authorization": "Bearer " + string(s)}, nil
}

func (scopeToken) RequireTransportSecurity() bool { return false }

func (h *scopeHarness) conn(token string) *grpc.ClientConn {
	h.t.Helper()
	conn, err := grpc.NewClient("passthrough:///"+h.addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithPerRPCCredentials(scopeToken(token)))
	if err != nil {
		h.t.Fatalf("dial: %v", err)
	}
	h.t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func (h *scopeHarness) client(token string) filer_pb.SeaweedFilerClient {
	return filer_pb.NewSeaweedFilerClient(h.conn(token))
}

func scopedToken(id string, lifetime time.Duration, readWrite, readOnly []string) string {
	return string(security.GenJwtForFilerScope(security.SigningKey(scopeTestKey), lifetime, id, readWrite, readOnly))
}

func methodScopedToken(t *testing.T, id string, prefixes, methods []string) string {
	t.Helper()
	claims := security.SeaweedFilerClaims{
		AllowedPrefixes: prefixes,
		AllowedMethods:  methods,
		RegisteredClaims: jwt.RegisteredClaims{
			ID:        id,
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
		},
	}
	return signedScopeClaims(t, scopeTestKey, claims)
}

func signedScopeClaims(t *testing.T, key string, claims security.SeaweedFilerClaims) string {
	t.Helper()
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(key))
	if err != nil {
		t.Fatalf("sign method token: %v", err)
	}
	return token
}

func unscopedToken() string {
	return string(security.GenJwtForFilerServer(security.SigningKey(scopeTestKey), 3600))
}

func scopeCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func entryWith(name string, fids ...string) *filer_pb.Entry {
	entry := &filer_pb.Entry{Name: name}
	for _, fid := range fids {
		entry.Chunks = append(entry.Chunks, &filer_pb.FileChunk{FileId: fid, Size: 10})
	}
	return entry
}

func expectScopeCode(t *testing.T, what string, err error, code codes.Code) {
	t.Helper()
	if got := status.Code(err); got != code {
		t.Fatalf("%s: got %v (%v), want %v", what, got, err, code)
	}
}

func expectScopeDenied(t *testing.T, h *scopeHarness, method, what string, call func() error) {
	t.Helper()
	before := len(h.fake.received(method))
	expectScopeCode(t, what, call(), codes.PermissionDenied)
	if got := len(h.fake.received(method)); got != before {
		t.Fatalf("%s: refused call reached the handler", what)
	}
}

func expectScopeForwarded(t *testing.T, h *scopeHarness, method, what string, call func() error) {
	t.Helper()
	before := len(h.fake.received(method))
	if err := call(); err != nil {
		t.Fatalf("%s: got %v", what, err)
	}
	if got := len(h.fake.received(method)); got != before+1 {
		t.Fatalf("%s: handler received %d calls, want %d", what, got, before+1)
	}
}
