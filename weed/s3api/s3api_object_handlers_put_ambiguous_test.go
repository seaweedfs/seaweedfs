package s3api

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
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
	v := &fakeVolumeServer{}
	upload := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
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
	apply         bool
	createErr     error
	respError     string
	lookupErr     error
	lookupFailKey string
	nextKey       uint64
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
	if f.apply {
		entry := proto.Clone(req.Entry).(*filer_pb.Entry)
		filer_pb.BeforeEntrySerialization(entry.Chunks)
		f.entries[req.Directory+"/"+req.Entry.Name] = entry
	}
	if f.createErr != nil {
		return nil, f.createErr
	}
	return &filer_pb.CreateEntryResponse{Error: f.respError}, nil
}

func (f *ambiguousPutFiler) LookupDirectoryEntry(_ context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
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
