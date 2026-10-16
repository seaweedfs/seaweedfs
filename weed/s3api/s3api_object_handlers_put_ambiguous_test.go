package s3api

import (
	"bytes"
	"context"
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
	txnCount      int
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

// ObjectTransaction applies the WriteCondition against the entry the condition
// key names, the way a real filer evaluates it under the path lock, then
// applies PUT mutations. Routed-write tests use it to catch a condition sent
// for an entry the gateway's conditional check never looked at.
func (f *ambiguousPutFiler) ObjectTransaction(_ context.Context, req *filer_pb.ObjectTransactionRequest) (*filer_pb.ObjectTransactionResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.txnCount++
	key := req.LockKey
	if req.ConditionKey != "" {
		key = req.ConditionKey
	}
	current := f.entries[key]
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
	for _, m := range req.Mutations {
		if m.Type == filer_pb.ObjectMutation_PUT {
			f.entries[m.Directory+"/"+m.Entry.Name] = proto.Clone(m.Entry).(*filer_pb.Entry)
		}
	}
	return &filer_pb.ObjectTransactionResponse{}, nil
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
// entry the read path serves (the latest version under .versions/), so the
// write must not carry a routed WriteCondition: the filer would evaluate it
// against the empty null-version slot and reject a matching If-Match.
func TestSuspendedPutIfMatchStaysOffRoutedCondition(t *testing.T) {
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

	r := httptest.NewRequest(http.MethodPut, "/b/k", strings.NewReader("new"))
	r.Header.Set(s3_constants.IfMatch, `"abc123"`)
	_, code, _ := s3a.putSuspendedVersioningObject(r, "b", "k", r.Body, "")
	if code != s3err.ErrNone {
		t.Fatalf("suspended If-Match PUT = %v", code)
	}
	if filerImpl.entries["/buckets/b/k"] == nil {
		t.Fatal("null-version entry was not created")
	}
	if filerImpl.txnCount != 0 {
		t.Fatalf("conditional suspended write routed %d transaction(s)", filerImpl.txnCount)
	}
}
