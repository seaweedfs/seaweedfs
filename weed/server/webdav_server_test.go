package weed_server

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"testing"

	"golang.org/x/net/webdav"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"github.com/stretchr/testify/assert"
)

func TestToFileInfoName(t *testing.T) {
	tests := []struct {
		fullpath string
		want     string
	}{
		{"/photo.jpg", "photo.jpg"},
		{"/Images/photo.jpg", "photo.jpg"},
		{"/Images/2026/photo.jpg", "photo.jpg"},
		{"/Images", "Images"},
		{"/Images/", "Images"},
		{"/", ""},
	}
	for _, tt := range tests {
		entry := &filer_pb.Entry{Name: "photo.jpg", Attributes: &filer_pb.FuseAttributes{}}
		fi := toFileInfo(util.FullPath(tt.fullpath), entry)
		if fi.Name() != tt.want {
			t.Errorf("toFileInfo(%q).Name() = %q, want %q (DAV:displayname must not carry the path)", tt.fullpath, fi.Name(), tt.want)
		}
	}
}

func TestToFileInfoRootIsDirectory(t *testing.T) {
	entry := &filer_pb.Entry{Attributes: &filer_pb.FuseAttributes{}}
	if !toFileInfo("/", entry).IsDir() {
		t.Error("root is not a directory")
	}
}

func TestFileInfoETag(t *testing.T) {
	ctx := context.Background()

	if _, err := (&FileInfo{}).ETag(ctx); err != webdav.ErrNotImplemented {
		t.Errorf("empty etag returned %v, want ErrNotImplemented so webdav derives one", err)
	}
	if etag, err := (&FileInfo{etag: "abc"}).ETag(ctx); err != nil || etag != "abc" {
		t.Errorf("ETag() = %q, %v, want \"abc\", nil", etag, err)
	}
}

type noVolumeFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
}

func (f *noVolumeFiler) LookupVolume(_ context.Context, _ *filer_pb.LookupVolumeRequest) (*filer_pb.LookupVolumeResponse, error) {
	return &filer_pb.LookupVolumeResponse{LocationsMap: map[string]*filer_pb.Locations{}}, nil
}

type createRecordingFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	req *filer_pb.CreateEntryRequest
}

func (f *createRecordingFiler) LookupDirectoryEntry(_ context.Context, _ *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	return nil, filer_pb.ErrNotFound
}

func (f *createRecordingFiler) CreateEntry(_ context.Context, req *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	f.req = req
	return &filer_pb.CreateEntryResponse{}, nil
}

type deleteRecordingFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	req      *filer_pb.DeleteEntryRequest
	existing *filer_pb.Entry
}

func (f *deleteRecordingFiler) LookupDirectoryEntry(_ context.Context, _ *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	if f.existing == nil {
		return nil, filer_pb.ErrNotFound
	}
	return &filer_pb.LookupDirectoryEntryResponse{Entry: f.existing}, nil
}

func (f *deleteRecordingFiler) DeleteEntry(_ context.Context, req *filer_pb.DeleteEntryRequest) (*filer_pb.DeleteEntryResponse, error) {
	f.req = req
	return &filer_pb.DeleteEntryResponse{}, nil
}

func startFakeWebDavFiler(t *testing.T, impl filer_pb.SeaweedFilerServer) pb.ServerAddress {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	filer_pb.RegisterSeaweedFilerServer(srv, impl)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	port := lis.Addr().(*net.TCPAddr).Port
	return pb.ServerAddress(fmt.Sprintf("127.0.0.1:1.%d", port))
}

// WebDavFile.Read must fail when chunk manifest resolution fails, instead of streaming zeros.
func TestWebDavFile_Read_ManifestResolveFailure(t *testing.T) {
	filerAddr := startFakeWebDavFiler(t, &noVolumeFiler{})
	fc := wdclient.NewFilerClient([]pb.ServerAddress{filerAddr}, grpc.WithTransportCredentials(insecure.NewCredentials()), "")
	t.Cleanup(fc.Close)

	fs := &WebDavFileSystem{filerClient: fc}
	entry := &filer_pb.Entry{
		Name:       "file",
		Attributes: &filer_pb.FuseAttributes{FileSize: 1 << 20},
		Chunks: []*filer_pb.FileChunk{
			{FileId: "1,1679011dc64abd40", IsChunkManifest: true, Offset: 0, Size: 1 << 20},
		},
	}
	f := &WebDavFile{fs: fs, name: "/file", entry: entry, ctx: context.Background()}

	n, err := f.Read(make([]byte, 16))
	assert.Error(t, err)
	assert.Equal(t, 0, n)
}

// Small files stored inline in the entry have no chunks to read from.
func TestWebDavFile_Read_InlineContent(t *testing.T) {
	content := []byte("hello inline")
	entry := &filer_pb.Entry{
		Name:       "small.txt",
		Attributes: &filer_pb.FuseAttributes{FileSize: uint64(len(content))},
		Content:    content,
	}
	filerAddr := startFakeWebDavFiler(t, &noVolumeFiler{})
	fc := wdclient.NewFilerClient([]pb.ServerAddress{filerAddr}, grpc.WithTransportCredentials(insecure.NewCredentials()), "")
	t.Cleanup(fc.Close)
	f := &WebDavFile{fs: &WebDavFileSystem{filerClient: fc}, name: "/small.txt", entry: entry, ctx: context.Background()}

	got, err := io.ReadAll(f)
	assert.NoError(t, err)
	assert.Equal(t, content, got)

	_, err = f.Seek(6, io.SeekStart)
	assert.NoError(t, err)
	buf := make([]byte, 3)
	n, err := f.Read(buf)
	assert.NoError(t, err)
	assert.Equal(t, "inl", string(buf[:n]))
}

// MKCOL paths end in "/"; the entry must be created under its own name, not as "" inside itself.
func TestWebDavFileSystem_Mkdir_EntryName(t *testing.T) {
	filer := &createRecordingFiler{}
	filerAddr := startFakeWebDavFiler(t, filer)
	fs := &WebDavFileSystem{option: &WebDavOption{
		Filer:          filerAddr,
		GrpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
	}}

	assert.NoError(t, fs.Mkdir(context.Background(), "/dir/sub/", 0755))
	if assert.NotNil(t, filer.req) {
		assert.Equal(t, "/dir", filer.req.Directory)
		assert.Equal(t, "sub", filer.req.Entry.Name)
		assert.True(t, filer.req.Entry.IsDirectory)
	}
}

// DELETE, and MOVE or COPY with Overwrite: T, remove a collection through RemoveAll, which must take its contents too.
func TestWebDavFileSystem_RemoveAll_Recursive(t *testing.T) {
	filer := &deleteRecordingFiler{}
	filerAddr := startFakeWebDavFiler(t, filer)
	fs := &WebDavFileSystem{option: &WebDavOption{
		Filer:          filerAddr,
		GrpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
	}}

	assert.NoError(t, fs.RemoveAll(context.Background(), "/dir/sub"))
	if assert.NotNil(t, filer.req) {
		assert.Equal(t, "/dir", filer.req.Directory)
		assert.Equal(t, "sub", filer.req.Name)
		assert.True(t, filer.req.IsRecursive)
	}

	filer.req = nil
	assert.NoError(t, fs.RemoveAll(context.Background(), "/dir/sub/"))
	if assert.NotNil(t, filer.req) {
		assert.Equal(t, "/dir", filer.req.Directory)
		assert.Equal(t, "sub", filer.req.Name)
	}

	filer.req = nil
	wrapped := NewWrappedFs(fs, "/served")
	assert.Equal(t, os.ErrInvalid, fs.RemoveAll(context.Background(), "/"))
	assert.Equal(t, os.ErrInvalid, wrapped.RemoveAll(context.Background(), "/../"))
	assert.Equal(t, os.ErrInvalid, wrapped.Rename(context.Background(), "/", "/x"))
	assert.Nil(t, filer.req)
}

// MOVE or COPY into an ancestor destination is refused before the
// destination is removed: removing it first would delete the source too.
func TestCopyMoveGuardHandler(t *testing.T) {
	called := false
	h := copyMoveGuardHandler{
		fs: &stubDavFs{present: map[string]bool{"/dir": true, "/dir/sub": true, "/dirs": true, "/dir/other": true, "/anywhere": true}},
		next: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			called = true
		}),
	}

	for _, tc := range []struct{ method, src, dst string }{
		{"MOVE", "/dir/sub", "/dir"},
		{"MOVE", "/dir/sub", "/dir/"},
		{"MOVE", "/dir", "/dir"},
		{"MOVE", "/dir", "/dir/sub"},
		{"MOVE", "/", "/anywhere"},
		{"COPY", "/dir/sub", "/dir"},
		{"COPY", "/dir", "/dir/sub"},
	} {
		called = false
		req := httptest.NewRequest(tc.method, "http://x"+tc.src, nil)
		req.Header.Set("Destination", "http://x"+tc.dst)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		assert.Equal(t, http.StatusForbidden, w.Code, "%s %s -> %s", tc.method, tc.src, tc.dst)
		assert.False(t, called)
	}

	for _, tc := range []struct{ method, src, dst, depth string }{
		{"COPY", "/dir/sub", "/dir/other", ""},
		{"COPY", "/dirs", "/dir", ""},
		{"COPY", "/dir", "/dir/sub", "0"},
	} {
		called = false
		req := httptest.NewRequest(tc.method, "http://x"+tc.src, nil)
		req.Header.Set("Destination", "http://x"+tc.dst)
		if tc.depth != "" {
			req.Header.Set("Depth", tc.depth)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		assert.True(t, called, "%s %s -> %s depth=%q", tc.method, tc.src, tc.dst, tc.depth)
	}

	// a source that fails to stat must not reach the handler either — the
	// destination would be deleted before the operation fails anyway; only
	// confirmed absence is a 404
	errFs := copyMoveGuardHandler{
		fs:   &stubDavFs{errPaths: map[string]bool{"/broken": true}},
		next: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true }),
	}
	for _, tc := range []struct {
		method, src, dst string
		want             int
	}{
		{"MOVE", "/missing", "/dir", http.StatusNotFound},
		{"COPY", "/missing", "/dir", http.StatusNotFound},
		{"MOVE", "/broken", "/dir", http.StatusInternalServerError},
	} {
		called = false
		req := httptest.NewRequest(tc.method, "http://x"+tc.src, nil)
		req.Header.Set("Destination", "http://x"+tc.dst)
		w := httptest.NewRecorder()
		errFs.ServeHTTP(w, req)
		assert.Equal(t, tc.want, w.Code, "%s %s -> %s", tc.method, tc.src, tc.dst)
		assert.False(t, called)
	}
}

type stubDavFs struct {
	webdav.FileSystem
	present  map[string]bool
	errPaths map[string]bool
}

func (f *stubDavFs) Stat(_ context.Context, name string) (os.FileInfo, error) {
	if f.errPaths[path.Clean(name)] {
		return nil, fmt.Errorf("filer unavailable")
	}
	if f.present[path.Clean(name)] {
		return &FileInfo{name: path.Base(name)}, nil
	}
	return nil, os.ErrNotExist
}

// A PUT onto a folder must not delete the folder to make room for the file.
func TestWebDavFileSystem_OpenFile_DoesNotReplaceFolder(t *testing.T) {
	filer := &deleteRecordingFiler{existing: &filer_pb.Entry{
		Name:        "sub",
		IsDirectory: true,
		Attributes:  &filer_pb.FuseAttributes{FileMode: uint32(os.ModeDir | 0755)},
	}}
	filerAddr := startFakeWebDavFiler(t, filer)
	fs := &WebDavFileSystem{option: &WebDavOption{
		Filer:          filerAddr,
		GrpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
	}}

	_, err := fs.OpenFile(context.Background(), "/dir/sub", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	assert.Equal(t, os.ErrExist, err)
	assert.Nil(t, filer.req)
}

type renameRecordingFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	req *filer_pb.AtomicRenameEntryRequest
}

func (f *renameRecordingFiler) LookupDirectoryEntry(_ context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	if req.Name != "file.txt" {
		return nil, filer_pb.ErrNotFound
	}
	return &filer_pb.LookupDirectoryEntryResponse{Entry: &filer_pb.Entry{Name: "file.txt", Attributes: &filer_pb.FuseAttributes{}}}, nil
}

func (f *renameRecordingFiler) AtomicRenameEntry(_ context.Context, req *filer_pb.AtomicRenameEntryRequest) (*filer_pb.AtomicRenameEntryResponse, error) {
	f.req = req
	return &filer_pb.AtomicRenameEntryResponse{}, nil
}

// MOVE of a file onto an existing folder removes the folder, then renames onto "/dir/coll/".
func TestWebDavFileSystem_Rename_FileOntoSlashedPath(t *testing.T) {
	filer := &renameRecordingFiler{}
	filerAddr := startFakeWebDavFiler(t, filer)
	fs := &WebDavFileSystem{option: &WebDavOption{
		Filer:          filerAddr,
		GrpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
	}}

	assert.NoError(t, fs.Rename(context.Background(), "/dir/file.txt", "/dir/coll/"))
	if assert.NotNil(t, filer.req) {
		assert.Equal(t, "/dir", filer.req.NewDirectory)
		assert.Equal(t, "coll", filer.req.NewName)
	}
}
