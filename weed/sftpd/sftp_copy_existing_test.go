package sftpd

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/sftpd/user"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type copyTestFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	entries   map[string]*filer_pb.Entry
	volumeUrl string
}

func (f *copyTestFiler) LookupDirectoryEntry(_ context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	if entry, ok := f.entries[req.Directory+"/"+req.Name]; ok {
		return &filer_pb.LookupDirectoryEntryResponse{Entry: entry}, nil
	}
	return nil, status.Error(codes.NotFound, "no entry is found")
}

func (f *copyTestFiler) LookupVolume(_ context.Context, req *filer_pb.LookupVolumeRequest) (*filer_pb.LookupVolumeResponse, error) {
	resp := &filer_pb.LookupVolumeResponse{LocationsMap: make(map[string]*filer_pb.Locations)}
	for _, vid := range req.VolumeIds {
		resp.LocationsMap[vid] = &filer_pb.Locations{Locations: []*filer_pb.Location{{Url: f.volumeUrl}}}
	}
	return resp, nil
}

func newCopyTestServer(t *testing.T, volumeUrl string, entries map[string]*filer_pb.Entry) *SftpServer {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	filer_pb.RegisterSeaweedFilerServer(grpcServer, &copyTestFiler{entries: entries, volumeUrl: volumeUrl})
	go func() { _ = grpcServer.Serve(lis) }()
	t.Cleanup(grpcServer.Stop)

	_, port, _ := net.SplitHostPort(lis.Addr().String())
	return &SftpServer{
		filerAddr:      pb.ServerAddress("127.0.0.1:0." + port),
		grpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
		user:           &user.User{Username: "alice", Uid: 1000, HomeDir: "/"},
	}
}

func chunkedEntry(size int64, chunks ...*filer_pb.FileChunk) *filer_pb.Entry {
	return &filer_pb.Entry{
		Attributes:  &filer_pb.FuseAttributes{FileSize: uint64(size)},
		Chunks:      chunks,
		IsDirectory: false,
	}
}

func fileChunk(fileId string, offset, size int64) *filer_pb.FileChunk {
	return &filer_pb.FileChunk{FileId: fileId, Offset: offset, Size: uint64(size), ModifiedTsNs: 1}
}

// A non-truncating write must keep untouched bytes, including a sparse file's
// zeros between chunks. Stopping at the first gap would drop the tail.
func TestCopyExistingContentFillsSparseGaps(t *testing.T) {
	chunkA := []byte("AAAAAAAAAA")
	chunkB := []byte("BBBBBBBBBB")
	objects := map[string][]byte{"3,01": chunkA, "3,02": chunkB}
	volume := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, ok := objects[r.URL.Path[1:]]
		if !ok {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		_, _ = w.Write(data)
	}))
	t.Cleanup(volume.Close)

	entries := map[string]*filer_pb.Entry{
		"/home/sparse.bin": chunkedEntry(100, fileChunk("3,01", 0, 10), fileChunk("3,02", 90, 10)),
	}
	fs := newCopyTestServer(t, volume.Listener.Addr().String(), entries)

	var dst bytes.Buffer
	if err := fs.copyExistingContent("/home/sparse.bin", &dst); err != nil {
		t.Fatalf("copyExistingContent: %v", err)
	}
	want := append(append(append([]byte{}, chunkA...), make([]byte, 80)...), chunkB...)
	if !bytes.Equal(dst.Bytes(), want) {
		t.Fatalf("copied %d bytes, want %d with a zero-filled gap; tail present: %v", dst.Len(), len(want), bytes.Contains(dst.Bytes(), chunkB))
	}
}

// A remote-only file has no local chunks: copying nothing would upload an
// empty replacement on close. Refusing the read keeps the remote object.
func TestCopyExistingContentRejectsUnreadableEntry(t *testing.T) {
	remoteOnly := chunkedEntry(0)
	remoteOnly.RemoteEntry = &filer_pb.RemoteEntry{RemoteSize: 100}
	entries := map[string]*filer_pb.Entry{
		"/home/remote.bin": remoteOnly,
	}
	fs := newCopyTestServer(t, "", entries)

	var dst bytes.Buffer
	if err := fs.copyExistingContent("/home/remote.bin", &dst); err == nil {
		t.Fatal("expected refusal when no local bytes exist")
	}
}

// A locally shortened file still names its older, larger remote object. Once
// the local entry is newer the remote size is stale: copying must not pad the
// file back out to it.
func TestCopyExistingContentIgnoresStaleRemoteSize(t *testing.T) {
	chunkA := []byte("AAAAAAAAAA")
	volume := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/3,01" {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		_, _ = w.Write(chunkA)
	}))
	t.Cleanup(volume.Close)

	entries := map[string]*filer_pb.Entry{
		"/home/short.bin": {
			Attributes:  &filer_pb.FuseAttributes{FileSize: 10, Mtime: 200},
			Chunks:      []*filer_pb.FileChunk{fileChunk("3,01", 0, 10)},
			RemoteEntry: &filer_pb.RemoteEntry{RemoteSize: 100, RemoteMtime: 100},
		},
	}
	fs := newCopyTestServer(t, volume.Listener.Addr().String(), entries)

	var dst bytes.Buffer
	if err := fs.copyExistingContent("/home/short.bin", &dst); err != nil {
		t.Fatalf("copyExistingContent: %v", err)
	}
	if !bytes.Equal(dst.Bytes(), chunkA) {
		t.Fatalf("copied %d bytes, want the %d local bytes only", dst.Len(), len(chunkA))
	}
}

// A file whose size is all holes still keeps its zeros.
func TestCopyExistingContentAllHoles(t *testing.T) {
	entries := map[string]*filer_pb.Entry{
		"/home/holes.bin": chunkedEntry(10),
	}
	fs := newCopyTestServer(t, "", entries)

	var dst bytes.Buffer
	if err := fs.copyExistingContent("/home/holes.bin", &dst); err != nil {
		t.Fatalf("all-holes file: %v", err)
	}
	if dst.Len() != 10 || !bytes.Equal(dst.Bytes(), make([]byte, 10)) {
		t.Fatalf("wrote %d bytes, want 10 zeros", dst.Len())
	}
}

func TestCopyExistingContentInlineAndMissing(t *testing.T) {
	entries := map[string]*filer_pb.Entry{
		"/home/inline.txt": {Attributes: &filer_pb.FuseAttributes{FileSize: 5}, Content: []byte("hello")},
	}
	fs := newCopyTestServer(t, "", entries)

	var dst bytes.Buffer
	if err := fs.copyExistingContent("/home/inline.txt", &dst); err != nil {
		t.Fatalf("inline content: %v", err)
	}
	if got := dst.String(); got != "hello" {
		t.Fatalf("inline content = %q, want %q", got, "hello")
	}

	dst.Reset()
	if err := fs.copyExistingContent("/home/missing.txt", &dst); err != nil || dst.Len() != 0 {
		t.Fatalf("missing file: err=%v written=%d", err, dst.Len())
	}
}

func TestCopyExistingContentTrailingGap(t *testing.T) {
	chunkA := []byte("AAAAAAAAAA")
	volume := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/3,01" {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		_, _ = w.Write(chunkA)
	}))
	t.Cleanup(volume.Close)

	entries := map[string]*filer_pb.Entry{
		"/home/trailing.bin": chunkedEntry(50, fileChunk("3,01", 0, 10)),
	}
	fs := newCopyTestServer(t, volume.Listener.Addr().String(), entries)

	var dst bytes.Buffer
	if err := fs.copyExistingContent("/home/trailing.bin", &dst); err != nil {
		t.Fatalf("copyExistingContent: %v", err)
	}
	want := append(append([]byte{}, chunkA...), make([]byte, 40)...)
	if !bytes.Equal(dst.Bytes(), want) {
		t.Fatalf("copied %d bytes, want %d with a zero-filled tail", dst.Len(), len(want))
	}
}

// mkdir and uploads must not create a missing parent folder on the way.
func TestRequireParentDir(t *testing.T) {
	fs := newCopyTestServer(t, "", map[string]*filer_pb.Entry{
		"/home/d":       {Name: "d", IsDirectory: true},
		"/home/d/a.txt": {Name: "a.txt"},
	})

	if err := fs.requireParentDir("/home/d/new"); err != nil {
		t.Fatalf("existing parent: %v", err)
	}
	if err := fs.requireParentDir("/home/missing/new"); err != os.ErrNotExist {
		t.Fatalf("missing parent: got %v, want os.ErrNotExist", err)
	}
	if err := fs.requireParentDir("/home/d/a.txt/new"); err != os.ErrNotExist {
		t.Fatalf("file as parent: got %v, want os.ErrNotExist", err)
	}
}

// rm takes files and rmdir directories; a missing path is an error for both.
func TestRequireEntry(t *testing.T) {
	fs := newCopyTestServer(t, "", map[string]*filer_pb.Entry{
		"/home/d":       {Name: "d", IsDirectory: true},
		"/home/d/a.txt": {Name: "a.txt"},
	})

	if err := fs.requireEntry("/home/d/a.txt", false); err != nil {
		t.Fatalf("rm of a file: %v", err)
	}
	if err := fs.requireEntry("/home/d", true); err != nil {
		t.Fatalf("rmdir of a directory: %v", err)
	}
	if err := fs.requireEntry("/home/d/missing", false); err != os.ErrNotExist {
		t.Fatalf("rm of a missing path: got %v, want os.ErrNotExist", err)
	}
	if err := fs.requireEntry("/home/d/a.txt", true); err != os.ErrNotExist {
		t.Fatalf("rmdir of a file: got %v, want os.ErrNotExist", err)
	}
	if err := fs.requireEntry("/home/d", false); err == nil || err == os.ErrNotExist {
		t.Fatalf("rm of a directory: got %v, want a failure", err)
	}
}
