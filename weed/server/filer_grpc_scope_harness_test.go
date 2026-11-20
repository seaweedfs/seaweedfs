package weed_server

// Harness for the scoped-token tests: a fake filesystem service behind the
// real scope interceptors on a loopback port, with clients that present
// tokens minted for the test. The fake records every request that reaches
// it, so a refused call can be proven to have reached nothing.

import (
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

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

	aliceWS = "/buckets/t1/users/alice/workspace"
	bobWS   = "/buckets/t1/users/bob/workspace"
	groupG1 = "/buckets/t1/groups/g1" // read-only for alice
)

// scopeFakeFiler stands in for the filer's filesystem service and for the
// scope's backend. Methods a scoped token must never reach stay Unimplemented
// through the embedded server.
type scopeFakeFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer

	mu      sync.Mutex
	calls   []scopeRecordedCall
	entries map[string]*filer_pb.Entry // full path -> entry
	kv      map[string][]byte
	nextFid int

	events chan *filer_pb.SubscribeMetadataResponse
	subs   int
}

type scopeRecordedCall struct {
	method string
	req    proto.Message
}

func newScopeFakeFiler() *scopeFakeFiler {
	return &scopeFakeFiler{
		entries: map[string]*filer_pb.Entry{},
		kv:      map[string][]byte{},
		events:  make(chan *filer_pb.SubscribeMetadataResponse, 16),
	}
}

func (f *scopeFakeFiler) record(method string, req proto.Message) {
	f.mu.Lock()
	f.calls = append(f.calls, scopeRecordedCall{method: method, req: proto.Clone(req)})
	f.mu.Unlock()
}

// received returns the requests the fake got for method, oldest first.
func (f *scopeFakeFiler) received(method string) []proto.Message {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []proto.Message
	for _, c := range f.calls {
		if c.method == method {
			out = append(out, c.req)
		}
	}
	return out
}

func (f *scopeFakeFiler) total() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func (f *scopeFakeFiler) put(p string, e *filer_pb.Entry) {
	f.mu.Lock()
	f.entries[p] = e
	f.mu.Unlock()
}

func scopeJoin(dir, name string) string {
	if dir == "/" {
		return "/" + name
	}
	return dir + "/" + name
}

// scopeBackend implementation.
func (f *scopeFakeFiler) scopeFindEntry(_ context.Context, p string) (*filer_pb.Entry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	e := f.entries[p]
	if e == nil {
		return nil, nil
	}
	return proto.Clone(e).(*filer_pb.Entry), nil
}

func (f *scopeFakeFiler) scopeKvGet(_ context.Context, key []byte) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.kv[string(key)], nil
}

func (f *scopeFakeFiler) LookupDirectoryEntry(_ context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	f.record("LookupDirectoryEntry", req)
	f.mu.Lock()
	e := f.entries[scopeJoin(req.GetDirectory(), req.GetName())]
	f.mu.Unlock()
	if e == nil {
		return &filer_pb.LookupDirectoryEntryResponse{}, filer_pb.ErrNotFound
	}
	return &filer_pb.LookupDirectoryEntryResponse{Entry: proto.Clone(e).(*filer_pb.Entry)}, nil
}

func (f *scopeFakeFiler) LookupDirectoryEntries(_ context.Context, req *filer_pb.LookupDirectoryEntriesRequest) (*filer_pb.LookupDirectoryEntriesResponse, error) {
	f.record("LookupDirectoryEntries", req)
	resp := &filer_pb.LookupDirectoryEntriesResponse{}
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, r := range req.GetRequests() {
		e := f.entries[scopeJoin(r.GetDirectory(), r.GetName())]
		res := &filer_pb.LookupDirectoryEntryResult{Found: e != nil}
		if e != nil {
			res.Entry = proto.Clone(e).(*filer_pb.Entry)
		}
		resp.Results = append(resp.Results, res)
	}
	return resp, nil
}

func (f *scopeFakeFiler) ListEntries(req *filer_pb.ListEntriesRequest, stream grpc.ServerStreamingServer[filer_pb.ListEntriesResponse]) error {
	f.record("ListEntries", req)
	f.mu.Lock()
	var children []*filer_pb.Entry
	for p, e := range f.entries {
		if i := strings.LastIndex(p, "/"); i >= 0 && (p[:i] == req.GetDirectory() || (i == 0 && req.GetDirectory() == "/")) {
			children = append(children, proto.Clone(e).(*filer_pb.Entry))
		}
	}
	f.mu.Unlock()
	for _, e := range children {
		if err := stream.Send(&filer_pb.ListEntriesResponse{Entry: e}); err != nil {
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
	f.mu.Lock()
	f.nextFid++
	n := f.nextFid
	f.mu.Unlock()
	count := req.GetCount()
	if count < 1 {
		count = 1
	}
	return &filer_pb.AssignVolumeResponse{FileId: fmt.Sprintf("3,%02x0000%04x", n, n), Count: count, Auth: "write-token"}, nil
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

func (f *scopeFakeFiler) KvGet(_ context.Context, req *filer_pb.KvGetRequest) (*filer_pb.KvGetResponse, error) {
	f.record("KvGet", req)
	f.mu.Lock()
	defer f.mu.Unlock()
	return &filer_pb.KvGetResponse{Value: f.kv[string(req.GetKey())]}, nil
}

// subscribers is how many subscriptions are open right now.
func (f *scopeFakeFiler) subscribers() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.subs
}

func (f *scopeFakeFiler) subscribe(method string, req *filer_pb.SubscribeMetadataRequest, stream grpc.ServerStreamingServer[filer_pb.SubscribeMetadataResponse]) error {
	f.record(method, req)
	f.mu.Lock()
	f.subs++
	f.mu.Unlock()
	defer func() { f.mu.Lock(); f.subs--; f.mu.Unlock() }()
	for {
		select {
		case ev := <-f.events:
			if err := stream.Send(ev); err != nil {
				return err
			}
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
}

func (f *scopeFakeFiler) SubscribeMetadata(req *filer_pb.SubscribeMetadataRequest, stream grpc.ServerStreamingServer[filer_pb.SubscribeMetadataResponse]) error {
	return f.subscribe("SubscribeMetadata", req, stream)
}

func (f *scopeFakeFiler) SubscribeLocalMetadata(req *filer_pb.SubscribeMetadataRequest, stream grpc.ServerStreamingServer[filer_pb.SubscribeMetadataResponse]) error {
	return f.subscribe("SubscribeLocalMetadata", req, stream)
}

// scopeHarness wires fake filer <- scope interceptors <- clients on loopback.
type scopeHarness struct {
	t       *testing.T
	fake    *scopeFakeFiler
	scope   *filerGrpcScope
	require bool
	// key, previousKey, leeway and assignLifetime are the harness's
	// jwt.filer_signing settings; tests change them between calls.
	key            string
	previousKey    string
	subscribeKey   string
	leeway         time.Duration
	assignLifetime time.Duration
	addr           string
}

func scopeServe(t *testing.T, srv *grpc.Server) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

func newScopeHarness(t *testing.T) *scopeHarness {
	t.Helper()
	return newScopeHarnessWith(t, newScopeFakeFiler())
}

// newScopeHarnessWith serves fake behind a new scope; two harnesses on one
// fake stand for two filer replicas over one store.
func newScopeHarnessWith(t *testing.T, fake *scopeFakeFiler) *scopeHarness {
	t.Helper()
	h := &scopeHarness{t: t, fake: fake, key: scopeTestKey}
	h.scope = newFilerGrpcScope(scopeConfig{
		signingKey:     func() security.SigningKey { return security.SigningKey(h.key) },
		previousKey:    func() security.SigningKey { return security.SigningKey(h.previousKey) },
		subscribeKey:   func() security.SigningKey { return security.SigningKey(h.subscribeKey) },
		adminKey:       func() security.SigningKey { return security.SigningKey(h.key) },
		require:        func() bool { return h.require },
		leeway:         func() time.Duration { return h.leeway },
		assignLifetime: func() time.Duration { return h.assignLifetime },
	}, fake)
	srv := grpc.NewServer(h.scope.ServerOptions()...)
	filer_pb.RegisterSeaweedFilerServer(srv, fake)
	h.addr = scopeServe(t, srv)
	return h
}

// staticToken presents one token on every call of a connection.
type staticToken string

func (s staticToken) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	if s == "" {
		return nil, nil
	}
	return map[string]string{"authorization": "Bearer " + string(s)}, nil
}

func (staticToken) RequireTransportSecurity() bool { return false }

// conn dials the server presenting token ("" for none).
func (h *scopeHarness) conn(token string) *grpc.ClientConn {
	h.t.Helper()
	c, err := grpc.NewClient("passthrough:///"+h.addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithPerRPCCredentials(staticToken(token)))
	if err != nil {
		h.t.Fatalf("client: %v", err)
	}
	h.t.Cleanup(func() { c.Close() })
	return c
}

func (h *scopeHarness) client(token string) filer_pb.SeaweedFilerClient {
	return filer_pb.NewSeaweedFilerClient(h.conn(token))
}

// scopedToken mints a token for id with the given grants and lifetime.
func scopedToken(id string, lifetime time.Duration, rw, ro []string) string {
	return string(security.GenJwtForFilerScope(security.SigningKey(scopeTestKey), lifetime, id, rw, ro))
}

func aliceToken() string {
	return scopedToken("alice", time.Hour, []string{aliceWS}, []string{groupG1})
}
func bobToken() string { return scopedToken("bob", time.Hour, []string{bobWS}, nil) }

// unscopedToken is what an in-tree client mints from the key.
func unscopedToken() string {
	return string(security.GenJwtForFilerServer(security.SigningKey(scopeTestKey), 3600))
}

func (h *scopeHarness) alice() filer_pb.SeaweedFilerClient { return h.client(aliceToken()) }
func (h *scopeHarness) bob() filer_pb.SeaweedFilerClient   { return h.client(bobToken()) }

func scopeCtx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func ctxBG() context.Context { return context.Background() }

// expectCode fails unless err carries the wanted gRPC code.
func expectCode(t *testing.T, what string, err error, want codes.Code) {
	t.Helper()
	if got := status.Code(err); got != want {
		t.Fatalf("%s: got %v (%v), want %v", what, got, err, want)
	}
}

func expectForwarded(t *testing.T, h *scopeHarness, method, what string, call func() error) {
	t.Helper()
	before := len(h.fake.received(method))
	if err := call(); err != nil {
		t.Fatalf("%s: want allowed, got %v", what, err)
	}
	if len(h.fake.received(method)) != before+1 {
		t.Fatalf("%s: allowed but not forwarded", what)
	}
}

func expectRefused(t *testing.T, h *scopeHarness, method, what string, call func() error) {
	t.Helper()
	before := len(h.fake.received(method))
	expectCode(t, what, call(), codes.PermissionDenied)
	if len(h.fake.received(method)) != before {
		t.Fatalf("%s: refused but forwarded", what)
	}
}

func chunk(fid string) *filer_pb.FileChunk { return &filer_pb.FileChunk{FileId: fid, Size: 10} }

// assign asks the filer for a chunk under p and returns its file id and
// assignment token.
func assign(t *testing.T, c filer_pb.SeaweedFilerClient, p string) (fid, token string) {
	t.Helper()
	return assignN(t, c, p, 1)
}

// assignN requests a count-n assignment, so the returned token covers fid and
// fid_1 .. fid_{n-1}.
func assignN(t *testing.T, c filer_pb.SeaweedFilerClient, p string, n int32) (fid, token string) {
	t.Helper()
	resp, err := c.AssignVolume(ctxBG(), &filer_pb.AssignVolumeRequest{Path: p, Count: n})
	if err != nil {
		t.Fatalf("assign %s: %v", p, err)
	}
	if resp.GetFileId() == "" || resp.GetAssignAuth() == "" {
		t.Fatalf("assign %s: no file id or assignment token in %v", p, resp)
	}
	return resp.GetFileId(), resp.GetAssignAuth()
}

// auth builds a chunk_auth map from fid, token pairs.
func auth(pairs ...string) map[string]string {
	m := map[string]string{}
	for i := 0; i+1 < len(pairs); i += 2 {
		m[pairs[i]] = pairs[i+1]
	}
	return m
}

func creatorAuth(c filer_pb.SeaweedFilerClient) func(dir string, e *filer_pb.Entry, chunkAuth map[string]string) func() error {
	return func(dir string, e *filer_pb.Entry, chunkAuth map[string]string) func() error {
		return func() error {
			_, err := c.CreateEntry(ctxBG(), &filer_pb.CreateEntryRequest{Directory: dir, Entry: e, ChunkAuth: chunkAuth})
			return err
		}
	}
}

func updaterAuth(c filer_pb.SeaweedFilerClient) func(dir string, e *filer_pb.Entry, chunkAuth map[string]string) func() error {
	return func(dir string, e *filer_pb.Entry, chunkAuth map[string]string) func() error {
		return func() error {
			_, err := c.UpdateEntry(ctxBG(), &filer_pb.UpdateEntryRequest{Directory: dir, Entry: e, ChunkAuth: chunkAuth})
			return err
		}
	}
}

func entryWith(name string, fids ...string) *filer_pb.Entry {
	e := &filer_pb.Entry{Name: name}
	for _, f := range fids {
		e.Chunks = append(e.Chunks, chunk(f))
	}
	return e
}

func creator(c filer_pb.SeaweedFilerClient) func(dir string, e *filer_pb.Entry) func() error {
	return func(dir string, e *filer_pb.Entry) func() error {
		return func() error {
			_, err := c.CreateEntry(ctxBG(), &filer_pb.CreateEntryRequest{Directory: dir, Entry: e})
			return err
		}
	}
}

func updater(c filer_pb.SeaweedFilerClient) func(dir string, e *filer_pb.Entry) func() error {
	return func(dir string, e *filer_pb.Entry) func() error {
		return func() error {
			_, err := c.UpdateEntry(ctxBG(), &filer_pb.UpdateEntryRequest{Directory: dir, Entry: e})
			return err
		}
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// fakeClock pins scopeNowFn for a test and returns a function that advances it.
func fakeClock(t *testing.T) func(d time.Duration) {
	t.Helper()
	now := time.Now()
	prev := scopeNowFn
	scopeNowFn = func() time.Time { return now }
	t.Cleanup(func() { scopeNowFn = prev })
	return func(d time.Duration) { now = now.Add(d) }
}
