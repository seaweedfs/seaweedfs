package mount

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/mount/meta_cache"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// proxyFilerServer replays the previous minute's metadata as a persisted log
// chunk, the way a filer answers every fresh subscription, and points volume
// lookups at an address nothing listens on: the mount host's view of a volume
// server it is meant to reach only through -volumeServerAccess=filerProxy.
type proxyFilerServer struct {
	filer_pb.UnimplementedSeaweedFilerServer
	chunk         *filer_pb.FileChunk
	subscriptions atomic.Int32
	volumeLookups atomic.Int32
	stop          chan struct{}
}

func (s *proxyFilerServer) SubscribeMetadata(req *filer_pb.SubscribeMetadataRequest, stream filer_pb.SeaweedFiler_SubscribeMetadataServer) error {
	if s.subscriptions.Add(1) > 1 {
		<-s.stop
		return nil
	}
	return stream.Send(&filer_pb.SubscribeMetadataResponse{
		LogFileRefs: []*filer_pb.LogFileChunkRef{{
			FilerId:  "5319c0e8",
			FileTsNs: time.Now().UnixNano(),
			Chunks:   []*filer_pb.FileChunk{s.chunk},
		}},
	})
}

func (s *proxyFilerServer) LookupVolume(ctx context.Context, req *filer_pb.LookupVolumeRequest) (*filer_pb.LookupVolumeResponse, error) {
	s.volumeLookups.Add(1)
	locations := make(map[string]*filer_pb.Locations)
	for _, vid := range req.VolumeIds {
		locations[vid] = &filer_pb.Locations{Locations: []*filer_pb.Location{{Url: "127.0.0.1:1", PublicUrl: "127.0.0.1:1"}}}
	}
	return &filer_pb.LookupVolumeResponse{LocationsMap: locations}, nil
}

// buildLogFileData writes the on-disk log format: [4-byte size | LogEntry].
func buildLogFileData(events ...*filer_pb.SubscribeMetadataResponse) []byte {
	var buf bytes.Buffer
	for _, event := range events {
		eventData, _ := proto.Marshal(event)
		entryData, _ := proto.Marshal(&filer_pb.LogEntry{TsNs: event.TsNs, Data: eventData, Key: []byte(event.Directory)})
		sizeBuf := make([]byte, 4)
		util.Uint32toBytes(sizeBuf, uint32(len(entryData)))
		buf.Write(sizeBuf)
		buf.Write(entryData)
	}
	return buf.Bytes()
}

// TestSubscribeMetaEventsReplaysThroughFilerProxy pins the replay to the lookup
// the mount was given: resolving volume servers here fails every subscription
// of a filerProxy mount, which then resubscribes a second later, forever.
func TestSubscribeMetaEventsReplaysThroughFilerProxy(t *testing.T) {
	logData := buildLogFileData(&filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		TsNs:      time.Now().UnixNano(),
		EventNotification: &filer_pb.EventNotification{
			NewEntry: &filer_pb.Entry{Name: "file", Attributes: &filer_pb.FuseAttributes{FileSize: 7}},
		},
	})

	var proxiedChunkIds atomic.Value
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fileId := r.URL.Query().Get("proxyChunkId")
		if fileId == "" {
			http.Error(w, "not a filer proxy read", http.StatusBadRequest)
			return
		}
		proxiedChunkIds.Store(fileId)
		w.Write(logData)
	}))
	t.Cleanup(proxy.Close)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	testServer := &proxyFilerServer{
		chunk: &filer_pb.FileChunk{FileId: "4471,ce598bfff8416e", Size: uint64(len(logData))},
		stop:  make(chan struct{}),
	}
	server := pb.NewGrpcServer()
	filer_pb.RegisterSeaweedFilerServer(server, testServer)
	go server.Serve(listener)
	t.Cleanup(func() {
		close(testServer.stop)
		server.Stop()
	})

	proxyUrl, err := url.Parse(proxy.URL)
	if err != nil {
		t.Fatalf("parse proxy url: %v", err)
	}

	uidGidMapper, err := meta_cache.NewUidGidMapper("", "")
	if err != nil {
		t.Fatalf("create uid/gid mapper: %v", err)
	}
	root := util.FullPath("/")
	wfs := &WFS{
		signature:         1,
		inodeToPath:       NewInodeToPath(root, 0),
		fhMap:             NewFileHandleToInode(),
		fhLockTable:       util.NewLockTable[FileHandleId](),
		hardLinkLockTable: util.NewLockTable[string](),
		option: &Option{
			ChunkSizeLimit:     1024,
			ConcurrentReaders:  1,
			VolumeServerAccess: "filerProxy",
			FilerMountRootPath: "/",
			FilerAddresses: []pb.ServerAddress{
				pb.NewServerAddressWithGrpcPort(proxyUrl.Host, listener.Addr().(*net.TCPAddr).Port),
			},
			GrpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	}
	wfs.metaCache = meta_cache.NewMetaCache(
		filepath.Join(t.TempDir(), "meta"),
		uidGidMapper,
		root,
		false,
		func(path util.FullPath) { wfs.inodeToPath.MarkChildrenCached(path) },
		func(path util.FullPath) bool { return wfs.inodeToPath.IsChildrenCached(path) },
		func(meta_cache.EntryInvalidation) {},
		nil,
	)
	t.Cleanup(wfs.metaCache.Shutdown)
	wfs.inodeToPath.MarkChildrenCached(root)
	wfs.inodeToPath.Lookup(util.FullPath("/dir"), time.Now().Unix(), true, false, 0, false)
	wfs.inodeToPath.MarkChildrenCached(util.FullPath("/dir"))

	followed := make(chan struct{})
	go func() {
		defer close(followed)
		meta_cache.SubscribeMetaEvents(wfs.metaCache, wfs.signature, wfs, wfs.LookupFn(), wfs.option.FilerMountRootPath, 0, false, nil)
	}()

	select {
	case <-followed:
	case <-time.After(20 * time.Second):
		t.Fatal("the log chunk replay never succeeded")
	}

	if got := testServer.volumeLookups.Load(); got != 0 {
		t.Errorf("filerProxy mount resolved %d volume locations, want 0", got)
	}
	if got, _ := proxiedChunkIds.Load().(string); got != testServer.chunk.FileId {
		t.Errorf("filer proxy served chunk %q, want %q", got, testServer.chunk.FileId)
	}

	entry, _, err := wfs.metaCache.FindEntry(context.Background(), util.FullPath("/dir/file"))
	if err != nil {
		t.Fatalf("replayed entry: %v", err)
	}
	if entry.Attr.FileSize != 7 {
		t.Errorf("replayed file size %d, want 7", entry.Attr.FileSize)
	}
}
