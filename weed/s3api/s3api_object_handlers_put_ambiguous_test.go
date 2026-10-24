package s3api

import (
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

// fakeVolumeServer serves the two volume-server calls putToFiler makes: chunk
// uploads over HTTP and BatchDelete over gRPC. Deleted fids are recorded so a
// test can tell whether chunk cleanup ran.
type fakeVolumeServer struct {
	volume_server_pb.UnimplementedVolumeServerServer
	httpAddr string
	grpcPort uint32

	mu          sync.Mutex
	deletedFids []string
	stored      map[string][]byte
}

func (f *fakeVolumeServer) BatchDelete(_ context.Context, req *volume_server_pb.BatchDeleteRequest) (*volume_server_pb.BatchDeleteResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	resp := &volume_server_pb.BatchDeleteResponse{}
	for _, fid := range req.FileIds {
		f.deletedFids = append(f.deletedFids, fid)
		resp.Results = append(resp.Results, &volume_server_pb.DeleteResult{FileId: fid, Status: http.StatusAccepted})
	}
	return resp, nil
}

func (f *fakeVolumeServer) deleted() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.deletedFids...)
}

func startFakeVolumeServer(t *testing.T) *fakeVolumeServer {
	t.Helper()
	v := &fakeVolumeServer{stored: map[string][]byte{}}
	upload := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fid := strings.TrimPrefix(r.URL.Path, "/")
		if r.Method == http.MethodGet {
			v.mu.Lock()
			data, ok := v.stored[fid]
			v.mu.Unlock()
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			if rg := r.Header.Get("Range"); rg != "" {
				var start, end int
				fmt.Sscanf(rg, "bytes=%d-%d", &start, &end)
				if end >= len(data) {
					end = len(data) - 1
				}
				w.WriteHeader(http.StatusPartialContent)
				w.Write(data[start : end+1])
				return
			}
			w.Write(data)
			return
		}
		if r.Method == http.MethodHead {
			v.mu.Lock()
			_, ok := v.stored[fid]
			v.mu.Unlock()
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.WriteHeader(http.StatusOK)
			return
		}
		data, _ := io.ReadAll(r.Body)
		v.mu.Lock()
		v.stored[fid] = data
		v.mu.Unlock()
		w.Header().Set("Content-MD5", r.Header.Get("Content-MD5"))
		w.WriteHeader(http.StatusCreated)
		io.WriteString(w, `{"size":1}`)
	}))
	t.Cleanup(upload.Close)
	v.httpAddr = strings.TrimPrefix(upload.URL, "http://")

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcSrv := grpc.NewServer()
	volume_server_pb.RegisterVolumeServerServer(grpcSrv, v)
	go grpcSrv.Serve(lis)
	t.Cleanup(grpcSrv.Stop)
	v.grpcPort = uint32(lis.Addr().(*net.TCPAddr).Port)
	return v
}

// ambiguousPutFiler fakes the filer calls putToFiler makes. CreateEntry can
// apply the write and still return an error — the ambiguous outcome a
// restarting owner filer produces for issue 11366.
type ambiguousPutFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	volume *fakeVolumeServer

	mu            sync.Mutex
	entries       map[string]*filer_pb.Entry
	txnCount      int
	txnCheck      func(req *filer_pb.ObjectTransactionRequest)
	apply         bool
	ignoreAltKey  bool
	createErr     error
	respError     string
	lookupErr     error
	lookupFailKey string
	nextKey       uint64
	// supersede, once, swaps in a concurrent write's entry as the routed
	// transaction commits — issue 11631's client retry. txnNoApply models a
	// transaction that failed before applying (e.g. its forward was refused).
	supersede  *filer_pb.Entry
	txnErr     error
	txnNoApply bool
	// beforeCreate runs inside CreateEntry's lock, before the condition is
	// evaluated, letting a test land a racing write at the worst moment.
	// beforeLookup runs inside LookupDirectoryEntry, letting a test delete a
	// just-committed entry before recovery reads it.
	beforeCreate func()
	beforeLookup func()
	stores       int
	conds        []*filer_pb.WriteCondition
}

func (f *ambiguousPutFiler) AssignVolume(context.Context, *filer_pb.AssignVolumeRequest) (*filer_pb.AssignVolumeResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextKey++
	return &filer_pb.AssignVolumeResponse{
		FileId: fmt.Sprintf("3,%016x%08x", f.nextKey, uint32(f.nextKey)),
		Count:  1,
		Location: &filer_pb.Location{
			Url:       f.volume.httpAddr,
			PublicUrl: f.volume.httpAddr,
			GrpcPort:  f.volume.grpcPort,
		},
	}, nil
}

func (f *ambiguousPutFiler) CreateEntry(_ context.Context, req *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.beforeCreate != nil {
		f.beforeCreate()
	}
	f.conds = append(f.conds, req.Condition)
	key := req.Directory + "/" + req.Entry.Name
	if !putConditionSatisfied(req.Condition, f.entries[key]) {
		return &filer_pb.CreateEntryResponse{Error: "precondition failed", ErrorCode: filer_pb.FilerError_PRECONDITION_FAILED}, nil
	}
	if f.apply {
		entry := proto.Clone(req.Entry).(*filer_pb.Entry)
		filer_pb.BeforeEntrySerialization(entry.Chunks)
		f.entries[key] = entry
		f.stores++
	}
	if f.createErr != nil {
		return nil, f.createErr
	}
	return &filer_pb.CreateEntryResponse{Error: f.respError}, nil
}

// putConditionSatisfied mirrors the filer's clauseSatisfied for the condition
// kinds the ambiguous-route recovery emits.
func putConditionSatisfied(cond *filer_pb.WriteCondition, current *filer_pb.Entry) bool {
	for _, c := range cond.GetClauses() {
		switch c.Kind {
		case filer_pb.WriteCondition_IF_NOT_EXISTS:
			if current != nil {
				return false
			}
		case filer_pb.WriteCondition_IF_ENTRY_EQUAL:
			if current == nil || c.ExpectedEntry == nil {
				if current != nil || c.ExpectedEntry != nil {
					return false
				}
				continue
			}
			expected := proto.Clone(c.ExpectedEntry).(*filer_pb.Entry)
			filer_pb.BeforeEntrySerialization(expected.Chunks)
			actual := proto.Clone(current).(*filer_pb.Entry)
			filer_pb.BeforeEntrySerialization(actual.Chunks)
			if !proto.Equal(filer.FromPbEntry("", actual).ToProtoEntry(), filer.FromPbEntry("", expected).ToProtoEntry()) {
				return false
			}
		default:
			return false
		}
	}
	return true
}

func (f *ambiguousPutFiler) LookupDirectoryEntry(_ context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.beforeLookup != nil {
		f.beforeLookup()
	}
	if f.lookupErr != nil && req.Directory+"/"+req.Name == f.lookupFailKey {
		return nil, f.lookupErr
	}
	if entry, ok := f.entries[req.Directory+"/"+req.Name]; ok {
		out := proto.Clone(entry).(*filer_pb.Entry)
		filer_pb.AfterEntryDeserialization(out.Chunks)
		return &filer_pb.LookupDirectoryEntryResponse{Entry: out}, nil
	}
	return &filer_pb.LookupDirectoryEntryResponse{}, nil
}

// ObjectTransaction applies the WriteCondition against the entry the condition
// key names, the way a real filer evaluates it under the path lock, then
// applies PUT mutations. Routed-write tests use it to catch a condition sent
// for an entry the gateway's conditional check never looked at.
func (f *ambiguousPutFiler) ObjectTransaction(_ context.Context, req *filer_pb.ObjectTransactionRequest) (*filer_pb.ObjectTransactionResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.txnCount++
	if f.txnCheck != nil {
		f.txnCheck(req)
	}
	key := req.LockKey
	if req.ConditionKey != "" {
		key = req.ConditionKey
	}
	current := f.entries[key]
	if current == nil && req.ConditionAltKey != "" && !f.ignoreAltKey {
		// Mirrors the filer's fallback: the alt key is the object's .versions
		// directory and the entry under test is the one its latest pointer
		// names; a delete marker resolves to absent.
		if versionsEntry := f.entries[req.ConditionAltKey]; versionsEntry != nil {
			if latest := string(versionsEntry.Extended[s3_constants.ExtLatestVersionFileNameKey]); latest != "" {
				key = req.ConditionAltKey + "/" + latest
				current = f.entries[key]
				if current != nil && string(current.Extended[s3_constants.ExtDeleteMarkerKey]) == "true" {
					current = nil
				}
			}
		}
	}
	for _, clause := range req.GetCondition().GetClauses() {
		var md5Hex string
		if current != nil && current.Attributes != nil {
			md5Hex = fmt.Sprintf("%x", current.Attributes.Md5)
		}
		ok := true
		switch clause.Kind {
		case filer_pb.WriteCondition_IF_NOT_EXISTS:
			ok = current == nil
		case filer_pb.WriteCondition_IF_EXISTS:
			ok = current != nil
		case filer_pb.WriteCondition_IF_ETAG_MATCH:
			ok = current != nil && slices.Contains(clause.Etags, md5Hex)
		case filer_pb.WriteCondition_IF_ETAG_NOT_MATCH:
			ok = current == nil || !slices.Contains(clause.Etags, md5Hex)
		}
		if !ok {
			return &filer_pb.ObjectTransactionResponse{ErrorCode: filer_pb.FilerError_PRECONDITION_FAILED}, nil
		}
	}
	if !f.txnNoApply {
		for _, m := range req.Mutations {
			if m.Type == filer_pb.ObjectMutation_PUT {
				entry := proto.Clone(m.Entry).(*filer_pb.Entry)
				filer_pb.BeforeEntrySerialization(entry.Chunks)
				f.entries[m.Directory+"/"+m.Entry.Name] = entry
			}
		}
	}
	if f.supersede != nil {
		f.entries[req.LockKey] = f.supersede
		f.supersede = nil
	}
	return &filer_pb.ObjectTransactionResponse{}, f.txnErr
}

func (f *ambiguousPutFiler) ListEntries(req *filer_pb.ListEntriesRequest, stream filer_pb.SeaweedFiler_ListEntriesServer) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for fullPath, entry := range f.entries {
		if path.Dir(fullPath) != req.Directory {
			continue
		}
		if req.Prefix != "" && !strings.HasPrefix(entry.Name, req.Prefix) {
			continue
		}
		if req.StartFromFileName != "" && (entry.Name < req.StartFromFileName ||
			(entry.Name == req.StartFromFileName && !req.InclusiveStartFrom)) {
			continue
		}
		if err := stream.Send(&filer_pb.ListEntriesResponse{Entry: proto.Clone(entry).(*filer_pb.Entry)}); err != nil {
			return err
		}
	}
	return nil
}

func (f *ambiguousPutFiler) UpdateEntry(_ context.Context, req *filer_pb.UpdateEntryRequest) (*filer_pb.UpdateEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.entries[req.Directory+"/"+req.Entry.Name] = proto.Clone(req.Entry).(*filer_pb.Entry)
	return &filer_pb.UpdateEntryResponse{}, nil
}

func (f *ambiguousPutFiler) LookupVolume(_ context.Context, req *filer_pb.LookupVolumeRequest) (*filer_pb.LookupVolumeResponse, error) {
	resp := &filer_pb.LookupVolumeResponse{LocationsMap: map[string]*filer_pb.Locations{}}
	for _, vid := range req.VolumeIds {
		resp.LocationsMap[vid] = &filer_pb.Locations{Locations: []*filer_pb.Location{{
			Url:       f.volume.httpAddr,
			PublicUrl: f.volume.httpAddr,
			GrpcPort:  f.volume.grpcPort,
		}}}
	}
	return resp, nil
}

func newPutTestServer(t *testing.T, filerAddrs ...pb.ServerAddress) *S3ApiServer {
	t.Helper()
	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	return &S3ApiServer{
		option: &S3ApiServerOption{
			Filers:         filerAddrs,
			GrpcDialOption: dialOption,
			BucketsPath:    "/buckets",
		},
		filerClient: wdclient.NewFilerClient(filerAddrs, dialOption, ""),
	}
}

func putTestObject(t *testing.T, s3a *S3ApiServer) (string, s3err.ErrorCode) {
	t.Helper()
	r := httptest.NewRequest(http.MethodPut, "/b/o", nil)
	etag, code, _ := s3a.putToFiler(r, "/buckets/b/o", strings.NewReader("hello world"), "b", "o", 1, 0, nil, false, "")
	return etag, code
}

// Issue 11366: CreateEntry applied on the filer but the response was lost
// (owner restarting). Once the entry is confirmed, the write is successful —
// deleting the chunks would leave the entry pointing at tombstoned needles.
func TestPutToFilerAmbiguousCreateKeepsChunks(t *testing.T) {
	volume := startFakeVolumeServer(t)
	filerImpl := &ambiguousPutFiler{
		volume:    volume,
		entries:   map[string]*filer_pb.Entry{},
		apply:     true,
		createErr: status.Error(codes.Unavailable, "connect: connection refused"),
	}
	s3a := newPutTestServer(t, startFakeFiler(t, filerImpl))

	etag, code := putTestObject(t, s3a)
	if code != s3err.ErrNone {
		t.Fatalf("putToFiler returned %v, want success once the entry is confirmed on the filer", code)
	}
	if etag == "" {
		t.Fatal("expected an etag")
	}
	if deleted := volume.deleted(); len(deleted) != 0 {
		t.Fatalf("chunks under a live entry were deleted: %v", deleted)
	}
}

// Issue 11387: the filer can report a create failure after inserting the entry
// (e.g. a parent-directory creation failing post-insert). The failure arrives
// in the response rather than as a transport status, so it maps to a
// definitive error — but the entry exists and deleting its chunks would
// tombstone live needles.
func TestPutToFilerPostCommitErrorKeepsChunks(t *testing.T) {
	volume := startFakeVolumeServer(t)
	filerImpl := &ambiguousPutFiler{
		volume:    volume,
		entries:   map[string]*filer_pb.Entry{},
		apply:     true,
		respError: "create parent directories of /buckets/b: i/o timeout",
	}
	s3a := newPutTestServer(t, startFakeFiler(t, filerImpl))

	etag, code := putTestObject(t, s3a)
	if code != s3err.ErrNone {
		t.Fatalf("putToFiler returned %v, want success once the entry is confirmed on the filer", code)
	}
	if etag == "" {
		t.Fatal("expected an etag")
	}
	if deleted := volume.deleted(); len(deleted) != 0 {
		t.Fatalf("chunks under a live entry were deleted: %v", deleted)
	}
}

// Issue 11387, multi-filer: a create that fails over mid-flight can commit on
// a filer the confirmation does not ask first. A not-found from one replica
// does not authorize deleting chunks an entry on another filer references.
func TestPutToFilerPostCommitErrorOnFailoverFilerKeepsChunks(t *testing.T) {
	volume := startFakeVolumeServer(t)
	filerA := &ambiguousPutFiler{
		volume:    volume,
		entries:   map[string]*filer_pb.Entry{},
		apply:     false,
		createErr: status.Error(codes.Unavailable, "connect: connection refused"),
	}
	filerB := &ambiguousPutFiler{
		volume:    volume,
		entries:   map[string]*filer_pb.Entry{},
		apply:     true,
		respError: "create parent directories of /buckets/b: i/o timeout",
	}
	s3a := newPutTestServer(t, startFakeFiler(t, filerA), startFakeFiler(t, filerB))

	etag, code := putTestObject(t, s3a)
	if code != s3err.ErrNone {
		t.Fatalf("putToFiler returned %v, want success once the entry is confirmed on the filer", code)
	}
	if etag == "" {
		t.Fatal("expected an etag")
	}
	if deleted := volume.deleted(); len(deleted) != 0 {
		t.Fatalf("chunks under a live entry were deleted: %v", deleted)
	}
}

// A create the filer definitively refused still cleans up the uploaded chunks.
func TestPutToFilerConfirmedFailureDeletesOrphans(t *testing.T) {
	volume := startFakeVolumeServer(t)
	filerImpl := &ambiguousPutFiler{
		volume:    volume,
		entries:   map[string]*filer_pb.Entry{},
		apply:     false,
		createErr: status.Error(codes.Unknown, "create refused"),
	}
	s3a := newPutTestServer(t, startFakeFiler(t, filerImpl))

	_, code := putTestObject(t, s3a)
	if code == s3err.ErrNone {
		t.Fatal("expected an error when the entry was not created")
	}
	if deleted := volume.deleted(); len(deleted) == 0 {
		t.Fatal("orphaned chunks were not deleted")
	}
}

// A stale entry from an earlier object does not prove this PUT landed: the
// outcome stays unknown, so the new chunks are kept and an error returned.
func TestPutToFilerAmbiguousCreateWithStaleEntryKeepsChunks(t *testing.T) {
	volume := startFakeVolumeServer(t)
	stale := &filer_pb.Entry{
		Name:       "o",
		Attributes: &filer_pb.FuseAttributes{FileSize: 5},
		Chunks:     []*filer_pb.FileChunk{{FileId: "3,000000000000009900000099", Size: 5}},
	}
	filerImpl := &ambiguousPutFiler{
		volume:    volume,
		entries:   map[string]*filer_pb.Entry{"/buckets/b/o": stale},
		apply:     false,
		createErr: status.Error(codes.Unavailable, "connect: connection refused"),
	}
	s3a := newPutTestServer(t, startFakeFiler(t, filerImpl))

	_, code := putTestObject(t, s3a)
	if code == s3err.ErrNone {
		t.Fatal("expected an error when the create outcome is unknown")
	}
	if deleted := volume.deleted(); len(deleted) != 0 {
		t.Fatalf("chunks were deleted while the create outcome was unverifiable: %v", deleted)
	}
}

// When neither the create nor the lookup can be answered, the outcome stays
// unknown: keep the chunks (vacuum reclaims orphans) rather than risk deleting
// chunks a live entry references.
func TestPutToFilerUnverifiableCreateKeepsChunks(t *testing.T) {
	volume := startFakeVolumeServer(t)
	unavailable := status.Error(codes.Unavailable, "connect: connection refused")
	filerImpl := &ambiguousPutFiler{
		volume:        volume,
		entries:       map[string]*filer_pb.Entry{},
		apply:         false,
		createErr:     unavailable,
		lookupErr:     unavailable,
		lookupFailKey: "/buckets/b/o",
	}
	s3a := newPutTestServer(t, startFakeFiler(t, filerImpl))

	_, code := putTestObject(t, s3a)
	if code == s3err.ErrNone {
		t.Fatal("expected an error when the create outcome is unknown")
	}
	if deleted := volume.deleted(); len(deleted) != 0 {
		t.Fatalf("chunks were deleted while the create outcome was unverifiable: %v", deleted)
	}
}

// Issue 11631: a routed PUT committed on the owner but its response was lost;
// a client retry then superseded the entry and the retry's old-chunk cleanup
// deleted this PUT's chunks. Re-committing the stale entry would resurrect a
// pointer to dead needles — the recovery must leave the newer write alone.
func TestPutToFilerRoutedAmbiguousKeepsNewerWrite(t *testing.T) {
	volume := startFakeVolumeServer(t)
	newer := &filer_pb.Entry{
		Name:       "o",
		Attributes: &filer_pb.FuseAttributes{FileSize: 11},
		Chunks:     []*filer_pb.FileChunk{{FileId: "3,00000000000000bb0000000b", Size: 11}},
	}
	filerImpl := &ambiguousPutFiler{
		volume:    volume,
		entries:   map[string]*filer_pb.Entry{},
		supersede: newer,
		txnErr:    status.Error(codes.Unavailable, "owner restarted mid-write"),
	}
	filerAddr := startFakeFiler(t, filerImpl)
	s3a := newPutTestServer(t, filerAddr)
	s3a.objectWriteLockClient = cluster.NewLockClient(s3a.option.GrpcDialOption, filerAddr)
	s3a.objectWriteLockClient.SetRing([]pb.ServerAddress{filerAddr}, 1)

	_, code := putTestObject(t, s3a)
	if code != s3err.ErrServiceUnavailable {
		t.Fatalf("putToFiler returned %v, want refusal once a newer write is found", code)
	}
	if filerImpl.stores != 0 {
		t.Fatalf("recovery re-committed the stale entry %d times", filerImpl.stores)
	}
	if got := filerImpl.entries["/buckets/b/o"]; got == nil || len(got.Chunks) == 0 || got.Chunks[0].FileId != newer.Chunks[0].FileId {
		t.Fatalf("the newer write's entry was clobbered: %+v", got)
	}
	if deleted := volume.deleted(); len(deleted) != 0 {
		t.Fatalf("chunks were deleted during recovery: %v", deleted)
	}
}

// Issue 11631, raced variant: the routed PUT never committed and the recovery
// lookup finds the entry absent, but a concurrent write lands before the
// fallback create. The create's precondition — evaluated under the owner's
// path lock — refuses instead of overwriting the racer.
func TestPutToFilerRoutedRecoveryLosesRace(t *testing.T) {
	volume := startFakeVolumeServer(t)
	newer := &filer_pb.Entry{
		Name:       "o",
		Attributes: &filer_pb.FuseAttributes{FileSize: 11},
		Chunks:     []*filer_pb.FileChunk{{FileId: "3,00000000000000bb0000000b", Size: 11}},
	}
	filerImpl := &ambiguousPutFiler{
		volume:     volume,
		entries:    map[string]*filer_pb.Entry{},
		apply:      true,
		txnNoApply: true,
		txnErr:     status.Error(codes.Unavailable, "response lost"),
	}
	filerImpl.beforeCreate = func() {
		filerImpl.entries["/buckets/b/o"] = newer
		filerImpl.beforeCreate = nil
	}
	filerAddr := startFakeFiler(t, filerImpl)
	s3a := newPutTestServer(t, filerAddr)
	s3a.objectWriteLockClient = cluster.NewLockClient(s3a.option.GrpcDialOption, filerAddr)
	s3a.objectWriteLockClient.SetRing([]pb.ServerAddress{filerAddr}, 1)

	_, code := putTestObject(t, s3a)
	if code != s3err.ErrServiceUnavailable {
		t.Fatalf("putToFiler returned %v, want refusal after losing the create race", code)
	}
	if filerImpl.stores != 0 {
		t.Fatalf("the raced recovery create stored %d entries", filerImpl.stores)
	}
	if len(filerImpl.conds) == 0 || filerImpl.conds[0].GetClauses()[0].Kind != filer_pb.WriteCondition_IF_NOT_EXISTS {
		t.Fatalf("recovery create carried no IF_NOT_EXISTS precondition: %+v", filerImpl.conds)
	}
	if got := filerImpl.entries["/buckets/b/o"]; got == nil || len(got.Chunks) == 0 || got.Chunks[0].FileId != newer.Chunks[0].FileId {
		t.Fatalf("the racing write's entry was clobbered: %+v", got)
	}
}

// Same incident, but the ambiguous PUT wrote a zero-byte object: recovery must
// still run — an unconditional fallback would overwrite the newer write even
// though there were no chunks to verify.
func TestPutToFilerRoutedAmbiguousEmptyPutKeepsNewerWrite(t *testing.T) {
	volume := startFakeVolumeServer(t)
	newer := &filer_pb.Entry{
		Name:       "o",
		Attributes: &filer_pb.FuseAttributes{FileSize: 11},
		Chunks:     []*filer_pb.FileChunk{{FileId: "3,00000000000000bb0000000b", Size: 11}},
	}
	filerImpl := &ambiguousPutFiler{
		volume:    volume,
		entries:   map[string]*filer_pb.Entry{},
		supersede: newer,
		txnErr:    status.Error(codes.Unavailable, "owner restarted mid-write"),
	}
	filerAddr := startFakeFiler(t, filerImpl)
	s3a := newPutTestServer(t, filerAddr)
	s3a.objectWriteLockClient = cluster.NewLockClient(s3a.option.GrpcDialOption, filerAddr)
	s3a.objectWriteLockClient.SetRing([]pb.ServerAddress{filerAddr}, 1)

	r := httptest.NewRequest(http.MethodPut, "/b/o", nil)
	_, code, _ := s3a.putToFiler(r, "/buckets/b/o", strings.NewReader(""), "b", "o", 1, 0, nil, false, "")
	if code != s3err.ErrServiceUnavailable {
		t.Fatalf("putToFiler returned %v, want refusal once a newer write is found", code)
	}
	if filerImpl.stores != 0 {
		t.Fatalf("recovery re-committed the stale entry %d times", filerImpl.stores)
	}
	if got := filerImpl.entries["/buckets/b/o"]; got == nil || len(got.Chunks) == 0 || got.Chunks[0].FileId != newer.Chunks[0].FileId {
		t.Fatalf("the newer write's entry was clobbered: %+v", got)
	}
}

// Chunk lists cannot prove a zero-byte PUT landed — an older empty object at
// the same path compares equal too. A stored entry whose metadata differs from
// this write's must not be claimed as its commit: it belongs to a different
// request, so the outcome stays uncertain for the client to retry.
func TestPutToFilerRoutedAmbiguousEmptyPutStaleMetadata(t *testing.T) {
	volume := startFakeVolumeServer(t)
	filerImpl := &ambiguousPutFiler{
		volume:     volume,
		txnNoApply: true,
		txnErr:     status.Error(codes.Unavailable, "response lost before commit"),
		entries: map[string]*filer_pb.Entry{
			"/buckets/b/o": {
				Name:       "o",
				Attributes: &filer_pb.FuseAttributes{Mime: "text/plain"},
				Extended:   map[string][]byte{s3_constants.ExtETagKey: []byte("\"d41d8cd98f00b204e9800998ecf8427e\"")},
			},
		},
	}
	filerAddr := startFakeFiler(t, filerImpl)
	s3a := newPutTestServer(t, filerAddr)
	s3a.objectWriteLockClient = cluster.NewLockClient(s3a.option.GrpcDialOption, filerAddr)
	s3a.objectWriteLockClient.SetRing([]pb.ServerAddress{filerAddr}, 1)

	r := httptest.NewRequest(http.MethodPut, "/b/o", nil)
	_, code, _ := s3a.putToFiler(r, "/buckets/b/o", strings.NewReader(""), "b", "o", 1, 0, nil, false, "")
	if code != s3err.ErrServiceUnavailable {
		t.Fatalf("putToFiler returned %v, want refusal when the stored empty entry cannot be this write's", code)
	}
	if filerImpl.stores != 0 {
		t.Fatalf("recovery committed over an unverifiable empty entry %d times", filerImpl.stores)
	}
	if got := filerImpl.entries["/buckets/b/o"]; got.GetAttributes().GetMime() != "text/plain" {
		t.Fatalf("the older object's entry was replaced: %+v", got)
	}
}

// The counterpart: when the routed zero-byte PUT did commit and only its
// response was lost, the stored entry is this write's own — it carries this
// write's stamped id — and recovery must still claim it rather than fail a
// successful write.
func TestPutToFilerRoutedAmbiguousEmptyPutLanded(t *testing.T) {
	volume := startFakeVolumeServer(t)
	filerImpl := &ambiguousPutFiler{
		volume:  volume,
		entries: map[string]*filer_pb.Entry{},
		txnErr:  status.Error(codes.Unavailable, "response lost after commit"),
	}
	filerAddr := startFakeFiler(t, filerImpl)
	s3a := newPutTestServer(t, filerAddr)
	s3a.objectWriteLockClient = cluster.NewLockClient(s3a.option.GrpcDialOption, filerAddr)
	s3a.objectWriteLockClient.SetRing([]pb.ServerAddress{filerAddr}, 1)

	r := httptest.NewRequest(http.MethodPut, "/b/o", nil)
	_, code, _ := s3a.putToFiler(r, "/buckets/b/o", strings.NewReader(""), "b", "o", 1, 0, nil, false, "")
	if code != s3err.ErrNone {
		t.Fatalf("putToFiler returned %v, want success — the stored empty entry is this write's own commit", code)
	}
	if filerImpl.stores != 0 {
		t.Fatalf("recovery re-committed an entry that was already stored %d times", filerImpl.stores)
	}
}

// A conditional recovery create is itself ambiguous once sent: if it commits
// and the response is lost, replaying it on another filer can resurrect an
// entry whose chunks a delete already reclaimed. It also serializes with
// routed writes only under the owner's per-path lock, so it is sent to the
// owner alone — never failed over — and a possibly-committed attempt is
// confirmed by lookup instead.
func TestPutToFilerRecoveryCreateNotReplayed(t *testing.T) {
	volume := startFakeVolumeServer(t)
	// The owner commits the conditional create and loses the response. The
	// healthy failover filer must never see it: the committed copy on the
	// owner is confirmed instead of re-sent elsewhere.
	filerA := &ambiguousPutFiler{
		volume:     volume,
		entries:    map[string]*filer_pb.Entry{},
		apply:      true,
		createErr:  status.Error(codes.Unavailable, "response lost after commit"),
		txnNoApply: true,
		txnErr:     status.Error(codes.Unavailable, "response lost"),
	}
	filerB := &ambiguousPutFiler{
		volume:  volume,
		entries: map[string]*filer_pb.Entry{},
		apply:   true,
	}
	addrA := startFakeFiler(t, filerA)
	addrB := startFakeFiler(t, filerB)
	s3a := newPutTestServer(t, addrA, addrB)
	s3a.objectWriteLockClient = cluster.NewLockClient(s3a.option.GrpcDialOption, addrA)
	s3a.objectWriteLockClient.SetRing([]pb.ServerAddress{addrA}, 1)

	_, code := putTestObject(t, s3a)
	if code != s3err.ErrNone {
		t.Fatalf("putToFiler returned %v — the committed create should be confirmed, not replayed", code)
	}
	if filerA.stores != 1 {
		t.Fatalf("the owner stored the entry %d times, want once", filerA.stores)
	}
	if filerB.stores != 0 || len(filerB.conds) != 0 {
		t.Fatalf("the recovery create reached a failover filer: stores=%d conds=%d", filerB.stores, len(filerB.conds))
	}
	if got := filerA.entries["/buckets/b/o"]; got == nil || len(got.Chunks) != 1 {
		t.Fatalf("the committed entry is missing on the owner: %+v", got)
	}
	if deleted := volume.deleted(); len(deleted) != 0 {
		t.Fatalf("chunks under a live entry were deleted: %v", deleted)
	}
}

// The conditional recovery create serializes with routed writes only under the
// owner's per-path lock, so it goes to the owner alone — a failover filer's
// lock is a different serialization boundary.
func TestPutToFilerRoutedRecoveryCreateStaysOnOwner(t *testing.T) {
	volume := startFakeVolumeServer(t)
	filerA := &ambiguousPutFiler{
		volume:     volume,
		entries:    map[string]*filer_pb.Entry{},
		apply:      true,
		txnNoApply: true,
		txnErr:     status.Error(codes.Unavailable, "response lost"),
	}
	filerB := &ambiguousPutFiler{
		volume:  volume,
		entries: map[string]*filer_pb.Entry{},
		apply:   true,
	}
	addrA := startFakeFiler(t, filerA)
	addrB := startFakeFiler(t, filerB)
	s3a := newPutTestServer(t, addrA, addrB)
	s3a.objectWriteLockClient = cluster.NewLockClient(s3a.option.GrpcDialOption, addrA)
	s3a.objectWriteLockClient.SetRing([]pb.ServerAddress{addrA}, 1)

	_, code := putTestObject(t, s3a)
	if code != s3err.ErrNone {
		t.Fatalf("putToFiler returned %v — the conditional create should commit on the owner", code)
	}
	if filerA.stores != 1 {
		t.Fatalf("the owner stored the entry %d times, want once", filerA.stores)
	}
	if len(filerA.conds) != 1 || filerA.conds[0].GetClauses()[0].GetKind() != filer_pb.WriteCondition_IF_NOT_EXISTS {
		t.Fatalf("recovery create carried %+v, want one IF_NOT_EXISTS", filerA.conds)
	}
	if filerB.stores != 0 || len(filerB.conds) != 0 {
		t.Fatalf("the conditional create failed over to another filer: stores=%d conds=%d", filerB.stores, len(filerB.conds))
	}
}

// When the owner cannot accept the recovery create, the outcome is uncertain —
// not a failover create under a different filer's lock.
func TestPutToFilerRoutedRecoveryOwnerDownIsUncertain(t *testing.T) {
	volume := startFakeVolumeServer(t)
	filerA := &ambiguousPutFiler{
		volume:     volume,
		entries:    map[string]*filer_pb.Entry{},
		createErr:  status.Error(codes.Unavailable, "connection refused"),
		txnNoApply: true,
		txnErr:     status.Error(codes.Unavailable, "response lost"),
	}
	filerB := &ambiguousPutFiler{
		volume:  volume,
		entries: map[string]*filer_pb.Entry{},
		apply:   true,
	}
	addrA := startFakeFiler(t, filerA)
	addrB := startFakeFiler(t, filerB)
	s3a := newPutTestServer(t, addrA, addrB)
	s3a.objectWriteLockClient = cluster.NewLockClient(s3a.option.GrpcDialOption, addrA)
	s3a.objectWriteLockClient.SetRing([]pb.ServerAddress{addrA}, 1)

	_, code := putTestObject(t, s3a)
	if code != s3err.ErrServiceUnavailable {
		t.Fatalf("putToFiler returned %v, want an uncertain result when the owner cannot take the create", code)
	}
	if filerB.stores != 0 || len(filerB.conds) != 0 {
		t.Fatalf("the conditional create failed over to another filer: stores=%d conds=%d", filerB.stores, len(filerB.conds))
	}
	if deleted := volume.deleted(); len(deleted) == 0 {
		t.Fatal("orphaned chunks were not deleted")
	}
}

// An absent entry cannot prove an empty PUT never committed — the routed
// create may have landed and a delete already removed it. Recovery must not
// recreate the object: the outcome is uncertain, not an IF_NOT_EXISTS create.
func TestPutToFilerRoutedAmbiguousEmptyPutAbsentNotRecreated(t *testing.T) {
	volume := startFakeVolumeServer(t)
	filerImpl := &ambiguousPutFiler{
		volume:  volume,
		entries: map[string]*filer_pb.Entry{},
		txnErr:  status.Error(codes.Unavailable, "response lost after commit"),
	}
	// The committed empty object is deleted before recovery looks it up.
	filerImpl.beforeLookup = func() {
		delete(filerImpl.entries, "/buckets/b/o")
	}
	filerAddr := startFakeFiler(t, filerImpl)
	s3a := newPutTestServer(t, filerAddr)
	s3a.objectWriteLockClient = cluster.NewLockClient(s3a.option.GrpcDialOption, filerAddr)
	s3a.objectWriteLockClient.SetRing([]pb.ServerAddress{filerAddr}, 1)

	r := httptest.NewRequest(http.MethodPut, "/b/o", nil)
	_, code, _ := s3a.putToFiler(r, "/buckets/b/o", strings.NewReader(""), "b", "o", 1, 0, nil, false, "")
	if code != s3err.ErrServiceUnavailable {
		t.Fatalf("putToFiler returned %v, want an uncertain result for an unverifiable empty PUT", code)
	}
	if filerImpl.stores != 0 {
		t.Fatalf("recovery recreated an object a delete may have removed %d times", filerImpl.stores)
	}
	if got := filerImpl.entries["/buckets/b/o"]; got != nil {
		t.Fatalf("the deleted object was resurrected: %+v", got)
	}
}

// Matching metadata alone cannot pass an older empty object off as this PUT's
// commit: a foreign write id marks it as a different write, so the outcome
// stays uncertain for the client to retry.
func TestPutToFilerRoutedAmbiguousEmptyPutForeignWriteId(t *testing.T) {
	volume := startFakeVolumeServer(t)
	emptyMd5 := md5.Sum(nil)
	filerImpl := &ambiguousPutFiler{
		volume:     volume,
		txnNoApply: true,
		txnErr:     status.Error(codes.Unavailable, "response lost before commit"),
		entries: map[string]*filer_pb.Entry{
			"/buckets/b/o": {
				Name:       "o",
				Attributes: &filer_pb.FuseAttributes{Mime: "application/octet-stream", Md5: emptyMd5[:]},
				Extended: map[string][]byte{
					s3_constants.ExtETagKey:       []byte("\"d41d8cd98f00b204e9800998ecf8427e\""),
					s3_constants.SeaweedFSWriteId: []byte("another-write"),
				},
			},
		},
	}
	filerAddr := startFakeFiler(t, filerImpl)
	s3a := newPutTestServer(t, filerAddr)
	s3a.objectWriteLockClient = cluster.NewLockClient(s3a.option.GrpcDialOption, filerAddr)
	s3a.objectWriteLockClient.SetRing([]pb.ServerAddress{filerAddr}, 1)

	r := httptest.NewRequest(http.MethodPut, "/b/o", nil)
	_, code, _ := s3a.putToFiler(r, "/buckets/b/o", strings.NewReader(""), "b", "o", 1, 0, nil, false, "")
	if code != s3err.ErrServiceUnavailable {
		t.Fatalf("putToFiler returned %v, want refusal when the stored empty entry carries another write's id", code)
	}
	if filerImpl.stores != 0 {
		t.Fatalf("recovery committed over an unverifiable empty entry %d times", filerImpl.stores)
	}
	if got := filerImpl.entries["/buckets/b/o"]; string(got.GetExtended()[s3_constants.SeaweedFSWriteId]) != "another-write" {
		t.Fatalf("the older object's entry was replaced: %+v", got)
	}
}

// A volume-encrypted chunk must be decrypted before SSE decryption sees it:
// fetchChunkData feeds ciphered chunks through the cipher-aware read path and
// slices plaintext space, while plain chunks keep streaming range reads.
func TestFetchChunkDataDecryptsVolumeCipher(t *testing.T) {
	volume := startFakeVolumeServer(t)
	filerImpl := &ambiguousPutFiler{volume: volume, entries: map[string]*filer_pb.Entry{}}
	s3a := newPutTestServer(t, startFakeFiler(t, filerImpl))

	plaintext := []byte("0123456789abcdefghijklmnopqrstuvwxyz")
	cipherKey := util.GenCipherKey()
	ciphertext, err := util.Encrypt(plaintext, cipherKey)
	if err != nil {
		t.Fatal(err)
	}
	fid := "3,01637037d6"
	volume.mu.Lock()
	volume.stored[fid] = ciphertext
	volume.mu.Unlock()

	full, err := s3a.fetchFullChunk(context.Background(), &filer.ChunkView{
		FileId: fid, ChunkSize: uint64(len(plaintext)), CipherKey: cipherKey,
	})
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(full)
	full.Close()
	if !bytes.Equal(got, plaintext) {
		t.Fatalf("full chunk read = %q, want %q", got, plaintext)
	}

	view, err := s3a.fetchChunkViewData(context.Background(), &filer.ChunkView{
		FileId: fid, OffsetInChunk: 5, ViewSize: 4, ChunkSize: uint64(len(plaintext)), CipherKey: cipherKey,
	})
	if err != nil {
		t.Fatal(err)
	}
	got, _ = io.ReadAll(view)
	view.Close()
	if !bytes.Equal(got, plaintext[5:9]) {
		t.Fatalf("ranged ciphered read = %q, want %q", got, plaintext[5:9])
	}

	volume.mu.Lock()
	volume.stored[fid] = plaintext
	volume.mu.Unlock()
	plain, err := s3a.fetchChunkViewData(context.Background(), &filer.ChunkView{
		FileId: fid, OffsetInChunk: 5, ViewSize: 4, ChunkSize: uint64(len(plaintext)),
	})
	if err != nil {
		t.Fatal(err)
	}
	got, _ = io.ReadAll(plain)
	plain.Close()
	if !bytes.Equal(got, plaintext[5:9]) {
		t.Fatalf("ranged plain read = %q, want %q", got, plaintext[5:9])
	}
}

// A suspended-versioning overwrite resolves the conditional check against the
// entry the read path serves: the null slot when present, else the latest
// version under .versions/. The routed transaction carries that version as the
// condition's fallback key so check and write stay under the filer lock —
// checking the empty null-version slot alone would reject a matching If-Match.
func TestSuspendedPutIfMatchRoutesWithConditionAltKey(t *testing.T) {
	volume := startFakeVolumeServer(t)
	filerImpl := &ambiguousPutFiler{
		volume: volume,
		entries: map[string]*filer_pb.Entry{
			"/buckets/b": {
				Name:        "b",
				IsDirectory: true,
				Attributes:  &filer_pb.FuseAttributes{},
				Extended:    map[string][]byte{s3_constants.ExtVersioningKey: []byte(s3_constants.VersioningSuspended)},
			},
			"/buckets/b/k.versions": {
				Name:        "k.versions",
				IsDirectory: true,
				Attributes:  &filer_pb.FuseAttributes{},
				Extended: map[string][]byte{
					s3_constants.ExtLatestVersionIdKey:       []byte("v1"),
					s3_constants.ExtLatestVersionFileNameKey: []byte("v_v1"),
				},
			},
			"/buckets/b/k.versions/v_v1": {
				Name:       "v_v1",
				Attributes: &filer_pb.FuseAttributes{Md5: []byte{0xab, 0xc1, 0x23}, FileSize: 1},
				Extended: map[string][]byte{
					s3_constants.ExtETagKey:      []byte("abc123"),
					s3_constants.ExtVersionIdKey: []byte("v1"),
				},
			},
		},
		apply: true,
	}
	filerAddr := startFakeFiler(t, filerImpl)
	s3a := newPutTestServer(t, filerAddr)
	s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
	s3a.objectWriteLockClient = cluster.NewLockClient(s3a.option.GrpcDialOption, filerAddr)
	s3a.objectWriteLockClient.SetRing([]pb.ServerAddress{filerAddr}, 1)

	filerImpl.txnCheck = func(req *filer_pb.ObjectTransactionRequest) {
		if req.ConditionAltKey != "/buckets/b/k.versions" {
			t.Fatalf("condition alt key = %q, want /buckets/b/k.versions", req.ConditionAltKey)
		}
	}

	r := httptest.NewRequest(http.MethodPut, "/b/k", strings.NewReader("new"))
	r.Header.Set(s3_constants.IfMatch, `"abc123"`)
	_, code, _ := s3a.putSuspendedVersioningObject(r, "b", "k", r.Body, "")
	if code != s3err.ErrNone {
		t.Fatalf("suspended If-Match PUT = %v", code)
	}
	if filerImpl.entries["/buckets/b/k"] == nil {
		t.Fatal("null-version entry was not created")
	}
	if filerImpl.txnCount != 1 {
		t.Fatalf("conditional suspended write should route one transaction, got %d", filerImpl.txnCount)
	}
}

// When the latest version is a delete marker, a suspended If-None-Match: *
// write must treat the object as absent — the fallback resolves a tombstone
// to "no current version", not an existing one.
func TestSuspendedPutIfNoneMatchWithDeleteMarkerLatest(t *testing.T) {
	volume := startFakeVolumeServer(t)
	filerImpl := &ambiguousPutFiler{
		volume: volume,
		entries: map[string]*filer_pb.Entry{
			"/buckets/b": {
				Name:        "b",
				IsDirectory: true,
				Attributes:  &filer_pb.FuseAttributes{},
				Extended:    map[string][]byte{s3_constants.ExtVersioningKey: []byte(s3_constants.VersioningSuspended)},
			},
			"/buckets/b/k.versions": {
				Name:        "k.versions",
				IsDirectory: true,
				Attributes:  &filer_pb.FuseAttributes{},
				Extended: map[string][]byte{
					s3_constants.ExtLatestVersionFileNameKey: []byte("v_v1"),
				},
			},
			"/buckets/b/k.versions/v_v1": {
				Name:       "v_v1",
				Attributes: &filer_pb.FuseAttributes{FileSize: 0},
				Extended: map[string][]byte{
					s3_constants.ExtVersionIdKey:    []byte("v1"),
					s3_constants.ExtDeleteMarkerKey: []byte("true"),
				},
			},
		},
		apply: true,
	}
	filerAddr := startFakeFiler(t, filerImpl)
	s3a := newPutTestServer(t, filerAddr)
	s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
	s3a.objectWriteLockClient = cluster.NewLockClient(s3a.option.GrpcDialOption, filerAddr)
	s3a.objectWriteLockClient.SetRing([]pb.ServerAddress{filerAddr}, 1)

	r := httptest.NewRequest(http.MethodPut, "/b/k", strings.NewReader("new"))
	r.Header.Set(s3_constants.IfNoneMatch, "*")
	_, code, _ := s3a.putSuspendedVersioningObject(r, "b", "k", r.Body, "")
	if code != s3err.ErrNone {
		t.Fatalf("suspended If-None-Match PUT over a delete-marker latest = %v, want success", code)
	}
	if filerImpl.entries["/buckets/b/k"] == nil {
		t.Fatal("null-version entry was not created")
	}
}

// A filer that predates condition_alt_key ignores the field and checks the
// empty null slot alone, so the transaction precondition-fails even though
// If-Match matches the served version. The gateway resends the transaction
// against the resolved version instead of surfacing the false 412.
func TestSuspendedPutIfMatchRecheckedWhenAltKeyIgnored(t *testing.T) {
	volume := startFakeVolumeServer(t)
	filerImpl := &ambiguousPutFiler{
		volume: volume,
		entries: map[string]*filer_pb.Entry{
			"/buckets/b": {
				Name:        "b",
				IsDirectory: true,
				Attributes:  &filer_pb.FuseAttributes{},
				Extended:    map[string][]byte{s3_constants.ExtVersioningKey: []byte(s3_constants.VersioningSuspended)},
			},
			"/buckets/b/k.versions": {
				Name:        "k.versions",
				IsDirectory: true,
				Attributes:  &filer_pb.FuseAttributes{},
				Extended: map[string][]byte{
					s3_constants.ExtLatestVersionIdKey:       []byte("v1"),
					s3_constants.ExtLatestVersionFileNameKey: []byte("v_v1"),
				},
			},
			"/buckets/b/k.versions/v_v1": {
				Name:       "v_v1",
				Attributes: &filer_pb.FuseAttributes{Md5: []byte{0xab, 0xc1, 0x23}, FileSize: 1},
				Extended: map[string][]byte{
					s3_constants.ExtETagKey:      []byte("abc123"),
					s3_constants.ExtVersionIdKey: []byte("v1"),
				},
			},
		},
		apply:        true,
		ignoreAltKey: true,
	}
	filerAddr := startFakeFiler(t, filerImpl)
	s3a := newPutTestServer(t, filerAddr)
	s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
	s3a.objectWriteLockClient = cluster.NewLockClient(s3a.option.GrpcDialOption, filerAddr)
	s3a.objectWriteLockClient.SetRing([]pb.ServerAddress{filerAddr}, 1)

	var conditionKeys []string
	filerImpl.txnCheck = func(req *filer_pb.ObjectTransactionRequest) {
		conditionKeys = append(conditionKeys, req.ConditionKey)
	}

	r := httptest.NewRequest(http.MethodPut, "/b/k", strings.NewReader("new"))
	r.Header.Set(s3_constants.IfMatch, `"abc123"`)
	_, code, _ := s3a.putSuspendedVersioningObject(r, "b", "k", r.Body, "")
	if code != s3err.ErrNone {
		t.Fatalf("suspended If-Match PUT against a filer ignoring condition_alt_key = %v", code)
	}
	if filerImpl.entries["/buckets/b/k"] == nil {
		t.Fatal("null-version entry was not created")
	}
	if filerImpl.txnCount != 2 {
		t.Fatalf("got %d transactions, want the routed attempt plus a resend against the resolved version", filerImpl.txnCount)
	}
	if len(conditionKeys) != 2 || conditionKeys[0] != "" || conditionKeys[1] != "/buckets/b/k.versions/v_v1" {
		t.Fatalf("condition keys = %v, want [\"\" \"/buckets/b/k.versions/v_v1\"]", conditionKeys)
	}
}
