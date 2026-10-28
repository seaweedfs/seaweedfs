package iceberg

import (
	"context"
	"fmt"
	"net"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// remoteCachingFiler is the fake filer plus what a remote mount needs, shaped
// as the real filer answers: a file of a remote-mounted bucket that is not
// cached locally has only its RemoteEntry; caching it writes its bytes to a
// volume and returns the entry with chunks and no inline content, so a read
// goes through the chunk reader, the volume lookup and the volume server.
type remoteCachingFiler struct {
	*fakeFilerServer
	volume     *fakeVolumeServer
	volumeHost string

	mu         sync.Mutex
	remote     map[string][]byte // dir/name -> the remote object's bytes
	cacheCalls int
	nextFid    int
	stayRemote bool // answer without caching, as on a racing uncache
	failFirst  int  // fail the first N cache calls
}

func (f *remoteCachingFiler) CacheRemoteObjectToLocalCluster(_ context.Context, req *filer_pb.CacheRemoteObjectToLocalClusterRequest) (*filer_pb.CacheRemoteObjectToLocalClusterResponse, error) {
	f.mu.Lock()
	body, ok := f.remote[req.Directory+"/"+req.Name]
	f.cacheCalls++
	f.nextFid++
	fid := fmt.Sprintf("7,%08x", f.nextFid)
	stay := f.stayRemote
	fail := f.failFirst > 0
	if fail {
		f.failFirst--
	}
	f.mu.Unlock()
	if fail {
		return nil, fmt.Errorf("remote read timed out")
	}
	entry := f.getEntry(req.Directory, req.Name)
	if !ok || entry == nil {
		return nil, fmt.Errorf("no remote object %s/%s", req.Directory, req.Name)
	}
	if stay {
		return &filer_pb.CacheRemoteObjectToLocalClusterResponse{Entry: entry}, nil
	}
	f.volume.put(fid, body)
	cached := &filer_pb.Entry{Name: entry.Name, Attributes: entry.Attributes, RemoteEntry: entry.RemoteEntry,
		Chunks: []*filer_pb.FileChunk{{FileId: fid, Offset: 0, Size: uint64(len(body)), ModifiedTsNs: time.Now().UnixNano()}}}
	f.putEntry(req.Directory, req.Name, cached)
	return &filer_pb.CacheRemoteObjectToLocalClusterResponse{Entry: cached}, nil
}

func (f *remoteCachingFiler) LookupVolume(_ context.Context, req *filer_pb.LookupVolumeRequest) (*filer_pb.LookupVolumeResponse, error) {
	out := &filer_pb.LookupVolumeResponse{LocationsMap: map[string]*filer_pb.Locations{}}
	for _, vid := range req.VolumeIds {
		out.LocationsMap[vid] = &filer_pb.Locations{Locations: []*filer_pb.Location{{Url: f.volumeHost, PublicUrl: f.volumeHost}}}
	}
	return out, nil
}

func startRemoteCachingFiler(t *testing.T) (*remoteCachingFiler, filer_pb.SeaweedFilerClient) {
	t.Helper()
	vol := startFakeVolumeServer(t)
	srv := &remoteCachingFiler{fakeFilerServer: newFakeFilerServer(), volume: vol, volumeHost: vol.hostPort(t), remote: map[string][]byte{}}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := grpc.NewServer()
	filer_pb.RegisterSeaweedFilerServer(server, srv)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.GracefulStop)
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return srv, filer_pb.NewSeaweedFilerClient(conn)
}

// putRemoteOnly records body as the remote object behind an Iceberg path and
// puts its remote-only entry: no chunks, no content, only the RemoteEntry.
func putRemoteOnly(t *testing.T, srv *remoteCachingFiler, bucket, dataPath, file string, body []byte) {
	t.Helper()
	full, err := icebergFilerPath(bucket, dataPath, file)
	if err != nil {
		t.Fatalf("path: %v", err)
	}
	dir, name := path.Dir(full), path.Base(full)
	srv.remote[dir+"/"+name] = body
	srv.putEntry(dir, name, &filer_pb.Entry{Name: name,
		Attributes:  &filer_pb.FuseAttributes{FileSize: uint64(len(body)), Mtime: time.Now().Unix()},
		RemoteEntry: &filer_pb.RemoteEntry{StorageName: "r", RemoteSize: int64(len(body)), RemoteMtime: time.Now().Unix()}})
}

func TestRemoteOnlyFileIsCachedThenReadThroughItsChunks(t *testing.T) {
	srv, client := startRemoteCachingFiler(t)
	const bucket, dataPath, file = "b", "ns/t", "data/d1-deletes.parquet"
	putRemoteOnly(t, srv, bucket, dataPath, file, positionDeleteParquet(t, []positionDeleteTestRow{
		{"s3://b/ns/t/data/f1.parquet", 3}, {"s3://b/ns/t/data/f1.parquet", 9},
	}))

	var got map[string][]int64
	var err error
	noPanic(t, func() {
		got, err = readPositionDeleteFile(context.Background(), client, bucket, dataPath, file)
	})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if srv.cacheCalls != 1 {
		t.Fatalf("CacheRemoteObjectToLocalCluster called %d times, want 1", srv.cacheCalls)
	}
	if n := len(got["s3://b/ns/t/data/f1.parquet"]); n != 2 {
		t.Fatalf("got %v, want the two positions of the remote object", got)
	}
}

func TestRemoteOnlyFileSchemaIsReadThroughItsChunks(t *testing.T) {
	srv, client := startRemoteCachingFiler(t)
	const bucket, dataPath, file = "b", "ns/t", "data/d2-deletes.parquet"
	putRemoteOnly(t, srv, bucket, dataPath, file, positionDeleteParquet(t, []positionDeleteTestRow{{"s3://b/ns/t/data/f1.parquet", 1}}))
	schema, err := parquetSchemaByIcebergPath(context.Background(), client, bucket, dataPath, file)
	if err != nil {
		t.Fatalf("schema: %v", err)
	}
	if _, ok := schema.Lookup("pos"); !ok {
		t.Fatalf("schema %v has no pos column", schema)
	}
}

func TestACachedFileIsNotCachedAgain(t *testing.T) {
	srv, client := startRemoteCachingFiler(t)
	const bucket, dataPath, file = "b", "ns/t", "data/d3-deletes.parquet"
	full, _ := icebergFilerPath(bucket, dataPath, file)
	body := positionDeleteParquet(t, []positionDeleteTestRow{{"s3://b/ns/t/data/f1.parquet", 1}})
	srv.volume.put("7,000000ff", body)
	srv.putEntry(path.Dir(full), path.Base(full), &filer_pb.Entry{Name: path.Base(full),
		Attributes:  &filer_pb.FuseAttributes{FileSize: uint64(len(body))},
		RemoteEntry: &filer_pb.RemoteEntry{StorageName: "r", RemoteSize: int64(len(body))},
		Chunks:      []*filer_pb.FileChunk{{FileId: "7,000000ff", Offset: 0, Size: uint64(len(body)), ModifiedTsNs: 1}}})
	if _, err := readPositionDeleteFile(context.Background(), client, bucket, dataPath, file); err != nil {
		t.Fatalf("read: %v", err)
	}
	if srv.cacheCalls != 0 {
		t.Fatalf("CacheRemoteObjectToLocalCluster called %d times for a cached file", srv.cacheCalls)
	}
}

func TestAFileStillRemoteOnlyAfterCachingIsACacheError(t *testing.T) {
	srv, client := startRemoteCachingFiler(t)
	srv.stayRemote = true
	const bucket, dataPath, file = "b", "ns/t", "data/d4-deletes.parquet"
	putRemoteOnly(t, srv, bucket, dataPath, file, positionDeleteParquet(t, []positionDeleteTestRow{{"s3://b/ns/t/data/f1.parquet", 1}}))
	var err error
	noPanic(t, func() {
		_, err = readPositionDeleteFile(context.Background(), client, bucket, dataPath, file)
	})
	if err == nil || !strings.Contains(err.Error(), "still remote-only") {
		t.Fatalf("err = %v, want a cache error", err)
	}
}

func TestAFailedCacheCallIsRetriedOnce(t *testing.T) {
	const bucket, dataPath, file = "b", "ns/t", "data/d5-deletes.parquet"
	body := positionDeleteParquet(t, []positionDeleteTestRow{{"s3://b/ns/t/data/f1.parquet", 1}})

	srv, client := startRemoteCachingFiler(t)
	putRemoteOnly(t, srv, bucket, dataPath, file, body)
	srv.failFirst = 1
	if _, err := readPositionDeleteFile(context.Background(), client, bucket, dataPath, file); err != nil {
		t.Fatalf("one failed cache call, then a good one: %v", err)
	}
	if srv.cacheCalls != 2 {
		t.Fatalf("%d cache calls, want 2", srv.cacheCalls)
	}

	srv, client = startRemoteCachingFiler(t)
	putRemoteOnly(t, srv, bucket, dataPath, file, body)
	srv.failFirst = 2
	if _, err := readPositionDeleteFile(context.Background(), client, bucket, dataPath, file); err == nil || !strings.Contains(err.Error(), "remote read timed out") {
		t.Fatalf("two failed cache calls: err = %v, want the cache error", err)
	}
}

func TestAFileWithNoContentOrChunksIsAnEmptyFileError(t *testing.T) {
	srv, client := startRemoteCachingFiler(t)
	const bucket, dataPath, file = "b", "ns/t", "snap-1.avro"
	full, _ := icebergFilerPath(bucket, dataPath, file)
	srv.putEntry(path.Dir(full), path.Base(full), &filer_pb.Entry{Name: path.Base(full),
		Attributes: &filer_pb.FuseAttributes{FileSize: 2057}}) // its size says bytes, it holds none
	if _, err := loadFileByIcebergPath(context.Background(), client, bucket, dataPath, file); err == nil || !strings.Contains(err.Error(), "empty") {
		t.Fatalf("err = %v, want an empty-file error", err)
	}
}
