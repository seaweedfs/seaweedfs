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
	"strings"
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
	req    *filer_pb.CreateEntryRequest
	delReq *filer_pb.DeleteEntryRequest
	dirs   map[string]bool // folders that exist, by full path
	files  map[string]bool // files that exist, by full path
}

func (f *createRecordingFiler) LookupDirectoryEntry(_ context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	p := string(util.NewFullPath(req.Directory, req.Name))
	if f.dirs[p] {
		return &filer_pb.LookupDirectoryEntryResponse{Entry: &filer_pb.Entry{
			Name:        req.Name,
			IsDirectory: true,
			Attributes:  &filer_pb.FuseAttributes{FileMode: uint32(os.ModeDir | 0755)},
		}}, nil
	}
	if f.files[p] {
		return &filer_pb.LookupDirectoryEntryResponse{Entry: &filer_pb.Entry{
			Name:       req.Name,
			Attributes: &filer_pb.FuseAttributes{FileMode: 0644},
		}}, nil
	}
	return nil, filer_pb.ErrNotFound
}

func (f *createRecordingFiler) CreateEntry(_ context.Context, req *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	f.req = req
	p := string(util.NewFullPath(req.Directory, req.Entry.Name))
	resp := &filer_pb.CreateEntryResponse{}
	if f.dirs[p] && !req.Entry.IsDirectory {
		resp.Error = "existing entry is a directory"
		resp.ErrorCode = filer_pb.FilerError_EXISTING_IS_DIRECTORY
		return resp, nil
	}
	if req.OExcl && (f.files[p] || f.dirs[p]) {
		resp.Error = "entry already exists"
		resp.ErrorCode = filer_pb.FilerError_ENTRY_ALREADY_EXISTS
		return resp, nil
	}
	if f.files[p] {
		resp.MetadataEvent = &filer_pb.SubscribeMetadataResponse{
			EventNotification: &filer_pb.EventNotification{
				OldEntry: &filer_pb.Entry{Name: req.Entry.Name},
			},
		}
	}
	if f.files == nil {
		f.files = map[string]bool{}
	}
	f.files[p] = true
	return resp, nil
}

func (f *createRecordingFiler) DeleteEntry(_ context.Context, req *filer_pb.DeleteEntryRequest) (*filer_pb.DeleteEntryResponse, error) {
	f.delReq = req
	return &filer_pb.DeleteEntryResponse{}, nil
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

func (f *deleteRecordingFiler) CreateEntry(_ context.Context, req *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	if f.existing != nil && f.existing.IsDirectory && !req.Entry.IsDirectory {
		return &filer_pb.CreateEntryResponse{
			Error:     "existing entry is a directory",
			ErrorCode: filer_pb.FilerError_EXISTING_IS_DIRECTORY,
		}, nil
	}
	return &filer_pb.CreateEntryResponse{}, nil
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
	filer := &createRecordingFiler{dirs: map[string]bool{"/dir": true}}
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

// MKCOL and PUT under a missing folder must fail with 409 (RFC 4918 9.3, 9.7.1),
// which x/net/webdav answers for os.ErrNotExist, and must not create anything.
func TestWebDavFileSystem_MissingParent(t *testing.T) {
	filer := &createRecordingFiler{}
	filerAddr := startFakeWebDavFiler(t, filer)
	fs := &WebDavFileSystem{option: &WebDavOption{
		Filer:          filerAddr,
		GrpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
	}}

	assert.ErrorIs(t, fs.Mkdir(context.Background(), "/missing/sub/", 0755), os.ErrNotExist)
	_, err := fs.OpenFile(context.Background(), "/missing/f.txt", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	assert.ErrorIs(t, err, os.ErrNotExist)
	assert.Nil(t, filer.req)

	assert.NoError(t, fs.Mkdir(context.Background(), "/top/", 0755), "a folder at the root needs no parent")
}

// A PUT overwrite checks the parent before deleting the file it replaces:
// a failed check must leave the old file in place.
func TestWebDavFileSystem_OverwriteKeepsTargetOnParentFailure(t *testing.T) {
	filer := &createRecordingFiler{files: map[string]bool{"/missing/f.txt": true}}
	filerAddr := startFakeWebDavFiler(t, filer)
	fs := &WebDavFileSystem{option: &WebDavOption{
		Filer:          filerAddr,
		GrpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
	}}

	_, err := fs.OpenFile(context.Background(), "/missing/f.txt", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	assert.ErrorIs(t, err, os.ErrNotExist)
	assert.Nil(t, filer.delReq)
	assert.Nil(t, filer.req)
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
		fs: &stubDavFs{
			present: map[string]bool{"/dir": true, "/dir/sub": true, "/dirs": true, "/dir/other": true, "/anywhere": true},
			dirs:    map[string]bool{"/dir": true, "/dirs": true},
		},
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

// MOVE or COPY under a missing collection is a 409: the webdav handler would
// only report the failed Mkdir or Rename as 403.
func TestCopyMoveGuardHandler_MissingDestParent(t *testing.T) {
	called := false
	h := copyMoveGuardHandler{
		fs: &stubDavFs{
			present:  map[string]bool{"/dir": true, "/dir/sub": true, "/file": true},
			dirs:     map[string]bool{"/dir": true},
			errPaths: map[string]bool{"/broken": true},
		},
		next: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true }),
	}

	for _, tc := range []struct {
		method, src, dst string
		want             int
	}{
		{"MOVE", "/dir/sub", "http://x/missing/sub", http.StatusConflict},
		{"COPY", "/dir/sub", "http://x/missing/sub", http.StatusConflict},
		{"COPY", "/dir", "http://x/missing/deep/dir", http.StatusConflict},
		{"MOVE", "/dir/sub", "http://x/file/sub", http.StatusConflict}, // parent is a file
		{"MOVE", "/dir/sub", "http://x/broken/sub", http.StatusInternalServerError},
		{"COPY", "/dir/sub", "http://x/dir/other", 0},   // parent exists
		{"MOVE", "/dir/sub", "http://x/moved", 0},       // destination at the root
		{"MOVE", "/dir/sub", "http://y/missing/sub", 0}, // foreign host is a 502, not ours to judge
		{"MOVE", "/dir/sub", "missing/sub", 0},          // a relative destination fails downstream
	} {
		called = false
		req := httptest.NewRequest(tc.method, "http://x"+tc.src, nil)
		req.Header.Set("Destination", tc.dst)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if tc.want == 0 {
			assert.True(t, called, "%s %s -> %s", tc.method, tc.src, tc.dst)
		} else {
			assert.Equal(t, tc.want, w.Code, "%s %s -> %s", tc.method, tc.src, tc.dst)
			assert.False(t, called)
		}
	}
}

// A file copied or moved onto a collection URL is created at the name without
// the slash: OpenFile refuses "name/" only after the collection is removed.
func TestCopyMoveGuardHandler_FileOntoCollectionURL(t *testing.T) {
	var got string
	h := copyMoveGuardHandler{
		fs: &stubDavFs{present: map[string]bool{"/f": true, "/d": true}, dirs: map[string]bool{"/d": true}},
		next: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			got = r.Header.Get("Destination")
		}),
	}
	for _, tc := range []struct{ method, src, dst, want string }{
		{"COPY", "/f", "/coll/", "http://x/coll"},
		{"MOVE", "/f", "/coll/", "http://x/coll"},
		{"COPY", "/f", "/a%20b/", "http://x/a%20b"},
		{"COPY", "/f", "/coll", "http://x/coll"},
		{"COPY", "/d", "/coll/", "http://x/coll/"},
	} {
		got = ""
		req := httptest.NewRequest(tc.method, "http://x"+tc.src, nil)
		req.Header.Set("Destination", "http://x"+tc.dst)
		h.ServeHTTP(httptest.NewRecorder(), req)
		assert.Equal(t, tc.want, got, "%s %s -> %s", tc.method, tc.src, tc.dst)
	}
}

// Through webdav.Handler, an overwriting COPY of a file onto a collection URL
// replaces the collection with the file; without the destination rewrite the
// collection is already gone when OpenFile("coll/", O_CREATE) is refused.
func TestCopyMoveGuardHandler_FileOntoCollection_EndToEnd(t *testing.T) {
	ctx := context.Background()
	fs := slashedCreateFS{FileSystem: webdav.NewMemFS()}
	put := func(name, data string) {
		w, err := fs.OpenFile(ctx, name, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write([]byte(data)); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := fs.Mkdir(ctx, "/coll", 0755); err != nil {
		t.Fatal(err)
	}
	put("/coll/inner.txt", "inner")
	put("/src.txt", "data")

	h := copyMoveGuardHandler{
		fs: fs,
		next: &webdav.Handler{
			FileSystem: fs,
			LockSystem: webdav.NewMemLS(),
		},
	}
	copySrc := func(overwrite string) *httptest.ResponseRecorder {
		req := httptest.NewRequest("COPY", "http://x/src.txt", nil)
		req.Header.Set("Destination", "http://x/coll/")
		req.Header.Set("Overwrite", overwrite)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		return w
	}

	w := copySrc("F")
	assert.Equal(t, http.StatusPreconditionFailed, w.Code)
	if fi, err := fs.Stat(ctx, "/coll"); assert.NoError(t, err) {
		assert.True(t, fi.IsDir())
	}
	_, err := fs.Stat(ctx, "/coll/inner.txt")
	assert.NoError(t, err)

	w = copySrc("T")
	assert.Equal(t, http.StatusNoContent, w.Code)
	if fi, err := fs.Stat(ctx, "/coll"); assert.NoError(t, err) {
		assert.False(t, fi.IsDir())
	}
	r, err := fs.OpenFile(ctx, "/coll", os.O_RDONLY, 0)
	if assert.NoError(t, err) {
		data, err := io.ReadAll(r)
		assert.NoError(t, err)
		assert.Equal(t, "data", string(data))
	}
	_, err = fs.Stat(ctx, "/coll/inner.txt")
	assert.Error(t, err)
}

// slashedCreateFS mimics the filer-backed fs: creating a file at a name ending
// in "/" is refused, which x/net/webdav only reaches after the overwritten
// destination has already been removed.
type slashedCreateFS struct {
	webdav.FileSystem
}

func (f slashedCreateFS) OpenFile(ctx context.Context, name string, flag int, perm os.FileMode) (webdav.File, error) {
	if flag&os.O_CREATE != 0 && strings.HasSuffix(name, "/") {
		return nil, os.ErrInvalid
	}
	return f.FileSystem.OpenFile(ctx, name, flag, perm)
}

type stubDavFs struct {
	webdav.FileSystem
	present  map[string]bool
	dirs     map[string]bool
	errPaths map[string]bool
}

func (f *stubDavFs) Stat(_ context.Context, name string) (os.FileInfo, error) {
	if f.errPaths[path.Clean(name)] {
		return nil, fmt.Errorf("filer unavailable")
	}
	if f.present[path.Clean(name)] {
		fi := &FileInfo{name: path.Base(name)}
		if f.dirs[path.Clean(name)] {
			fi.isDirectory = true
		}
		return fi, nil
	}
	return nil, os.ErrNotExist
}

// A PUT onto a folder must not delete the folder to make room for the file.
// Without an Overwrite header a MOVE replaces an existing destination, as
// COPY already does.
func TestCopyMoveGuardHandler_MoveWithoutOverwriteHeader(t *testing.T) {
	ctx := context.Background()
	fs := webdav.NewMemFS()
	for name, data := range map[string]string{"/a.txt": "a", "/b.txt": "b"} {
		f, err := fs.OpenFile(ctx, name, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write([]byte(data)); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}
	h := copyMoveGuardHandler{
		fs:   fs,
		next: &webdav.Handler{FileSystem: fs, LockSystem: webdav.NewMemLS()},
	}
	req := httptest.NewRequest("MOVE", "http://x/a.txt", nil)
	req.Header.Set("Destination", "http://x/b.txt")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNoContent, w.Code)
	r, err := fs.OpenFile(ctx, "/b.txt", os.O_RDONLY, 0)
	if assert.NoError(t, err) {
		data, err := io.ReadAll(r)
		assert.NoError(t, err)
		assert.Equal(t, "a", string(data))
	}
	_, err = fs.Stat(ctx, "/a.txt")
	assert.True(t, os.IsNotExist(err))
}

// An Overwrite header with an empty first value is present, not missing, so
// the default must not overwrite it — a later explicit F forbids the move.
func TestCopyMoveGuardHandler_MoveEmptyOverwriteValue(t *testing.T) {
	ctx := context.Background()
	fs := webdav.NewMemFS()
	for name, data := range map[string]string{"/a.txt": "a", "/b.txt": "b"} {
		f, err := fs.OpenFile(ctx, name, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write([]byte(data)); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}
	h := copyMoveGuardHandler{
		fs:   fs,
		next: &webdav.Handler{FileSystem: fs, LockSystem: webdav.NewMemLS()},
	}
	req := httptest.NewRequest("MOVE", "http://x/a.txt", nil)
	req.Header.Set("Destination", "http://x/b.txt")
	req.Header["Overwrite"] = []string{"", "F"}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	assert.Equal(t, http.StatusPreconditionFailed, w.Code)
	r, err := fs.OpenFile(ctx, "/b.txt", os.O_RDONLY, 0)
	if assert.NoError(t, err) {
		data, err := io.ReadAll(r)
		assert.NoError(t, err)
		assert.Equal(t, "b", string(data))
	}
}

// An overwriting MOVE of a collection onto a non-empty collection removes the
// destination: the deferred rename only merges over an empty one, so this
// case parks the target aside and deletes it once the move has landed.
func TestCopyMoveGuardHandler_MoveDirOntoNonEmptyDir(t *testing.T) {
	ctx := context.Background()
	fs := webdav.NewMemFS()
	put := func(name, data string) {
		f, err := fs.OpenFile(ctx, name, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write([]byte(data)); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := fs.Mkdir(ctx, "/src", 0755); err != nil {
		t.Fatal(err)
	}
	put("/src/s.txt", "s")
	if err := fs.Mkdir(ctx, "/dst", 0755); err != nil {
		t.Fatal(err)
	}
	put("/dst/d.txt", "d")

	h := copyMoveGuardHandler{
		fs:   fs,
		next: &webdav.Handler{FileSystem: fs, LockSystem: webdav.NewMemLS()},
	}
	req := httptest.NewRequest("MOVE", "http://x/src", nil)
	req.Header.Set("Destination", "http://x/dst")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNoContent, w.Code)
	_, err := fs.Stat(ctx, "/dst/d.txt")
	assert.True(t, os.IsNotExist(err))
	if r, err := fs.OpenFile(ctx, "/dst/s.txt", os.O_RDONLY, 0); assert.NoError(t, err) {
		data, err := io.ReadAll(r)
		assert.NoError(t, err)
		assert.Equal(t, "s", string(data))
	}
	_, err = fs.Stat(ctx, "/src")
	assert.True(t, os.IsNotExist(err))
}

// An overwriting MOVE of a collection onto an empty collection keeps the
// deferred path: the destination survives if the rename itself fails.
func TestCopyMoveGuardHandler_MoveDirOntoEmptyDirKeepsDestinationOnFailure(t *testing.T) {
	ctx := context.Background()
	mem := webdav.NewMemFS()
	if err := mem.Mkdir(ctx, "/src", 0755); err != nil {
		t.Fatal(err)
	}
	if err := mem.Mkdir(ctx, "/dst", 0755); err != nil {
		t.Fatal(err)
	}
	fs := failRenameFS{mem}
	h := copyMoveGuardHandler{
		fs:   fs,
		next: &webdav.Handler{FileSystem: fs, LockSystem: webdav.NewMemLS()},
	}
	req := httptest.NewRequest("MOVE", "http://x/src", nil)
	req.Header.Set("Destination", "http://x/dst")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	assert.Equal(t, http.StatusForbidden, w.Code)
	if fi, err := mem.Stat(ctx, "/dst"); assert.NoError(t, err) {
		assert.True(t, fi.IsDir())
	}
}

type failRenameFS struct {
	webdav.FileSystem
}

func (f failRenameFS) Rename(context.Context, string, string) error {
	return fmt.Errorf("rename refused")
}

type failPairRenameFS struct {
	webdav.FileSystem
	oldName, newName string
}

func (f failPairRenameFS) Rename(ctx context.Context, oldName, newName string) error {
	if oldName == f.oldName && newName == f.newName {
		return fmt.Errorf("rename refused")
	}
	return f.FileSystem.Rename(ctx, oldName, newName)
}

// A MOVE of a collection onto a non-empty collection whose rename is refused
// (cross-collection moves, bucket targets) must bring the destination back:
// it is only parked aside while the winning rename runs.
func TestCopyMoveGuardHandler_MoveDirOntoNonEmptyDirRestoresOnRenameFailure(t *testing.T) {
	ctx := context.Background()
	mem := webdav.NewMemFS()
	put := func(name, data string) {
		f, err := mem.OpenFile(ctx, name, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write([]byte(data)); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := mem.Mkdir(ctx, "/src", 0755); err != nil {
		t.Fatal(err)
	}
	put("/src/s.txt", "s")
	if err := mem.Mkdir(ctx, "/dst", 0755); err != nil {
		t.Fatal(err)
	}
	put("/dst/d.txt", "d")

	fs := failPairRenameFS{mem, "/src", "/dst"}
	h := copyMoveGuardHandler{
		fs:   fs,
		next: &webdav.Handler{FileSystem: fs, LockSystem: webdav.NewMemLS()},
	}
	req := httptest.NewRequest("MOVE", "http://x/src", nil)
	req.Header.Set("Destination", "http://x/dst")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	assert.Equal(t, http.StatusForbidden, w.Code)
	if r, err := mem.OpenFile(ctx, "/dst/d.txt", os.O_RDONLY, 0); assert.NoError(t, err) {
		data, err := io.ReadAll(r)
		assert.NoError(t, err)
		assert.Equal(t, "d", string(data))
		r.Close()
	}
	if r, err := mem.OpenFile(ctx, "/src/s.txt", os.O_RDONLY, 0); assert.NoError(t, err) {
		r.Close()
	}
	root, err := mem.OpenFile(ctx, "/", os.O_RDONLY, 0)
	if assert.NoError(t, err) {
		entries, err := root.Readdir(-1)
		assert.NoError(t, err)
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		assert.ElementsMatch(t, []string{"src", "dst"}, names)
	}
}

type cancelingRenameFS struct {
	webdav.FileSystem
	cancel           context.CancelFunc
	oldName, newName string
}

func (f cancelingRenameFS) Rename(ctx context.Context, oldName, newName string) error {
	if oldName == f.oldName && newName == f.newName {
		f.cancel()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return f.FileSystem.Rename(ctx, oldName, newName)
}

// A client disconnect cancels the request context mid-move; restoring the
// parked destination runs under a detached context, so the destination is
// not left under its hidden name.
func TestDeferredDeleteFS_RestoreSurvivesCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mem := webdav.NewMemFS()
	put := func(name, data string) {
		f, err := mem.OpenFile(ctx, name, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write([]byte(data)); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := mem.Mkdir(ctx, "/src", 0755); err != nil {
		t.Fatal(err)
	}
	put("/src/s.txt", "s")
	if err := mem.Mkdir(ctx, "/dst", 0755); err != nil {
		t.Fatal(err)
	}
	put("/dst/d.txt", "d")

	fs := deferredDeleteFS{
		FileSystem: cancelingRenameFS{mem, cancel, "/src", "/dst"},
		dst:        "/dst",
	}
	err := fs.Rename(ctx, "/src", "/dst")
	assert.ErrorIs(t, err, context.Canceled)
	if r, err := mem.OpenFile(context.Background(), "/dst/d.txt", os.O_RDONLY, 0); assert.NoError(t, err) {
		data, err := io.ReadAll(r)
		assert.NoError(t, err)
		assert.Equal(t, "d", string(data))
		r.Close()
	}
}

// A MOVE whose rename fails must leave the existing destination in place:
// the delete is deferred into the rename, which replaces atomically.
func TestCopyMoveGuardHandler_MoveKeepsDestinationOnRenameFailure(t *testing.T) {
	ctx := context.Background()
	mem := webdav.NewMemFS()
	for name, data := range map[string]string{"/a.txt": "a", "/b.txt": "b"} {
		f, err := mem.OpenFile(ctx, name, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write([]byte(data)); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}
	fs := failRenameFS{mem}
	h := copyMoveGuardHandler{
		fs:   fs,
		next: &webdav.Handler{FileSystem: fs, LockSystem: webdav.NewMemLS()},
	}
	req := httptest.NewRequest("MOVE", "http://x/a.txt", nil)
	req.Header.Set("Destination", "http://x/b.txt")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	assert.Equal(t, http.StatusForbidden, w.Code)
	r, err := mem.OpenFile(ctx, "/b.txt", os.O_RDONLY, 0)
	if assert.NoError(t, err) {
		data, err := io.ReadAll(r)
		assert.NoError(t, err)
		assert.Equal(t, "b", string(data))
	}
}

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

// putOutcomeFS mimics the filer-backed fs: creating over an existing name
// marks the request's PUT outcome as a replace.
type putOutcomeFS struct {
	webdav.FileSystem
}

func (f putOutcomeFS) OpenFile(ctx context.Context, name string, flag int, perm os.FileMode) (webdav.File, error) {
	if flag&os.O_CREATE != 0 {
		if _, err := f.FileSystem.Stat(ctx, name); err == nil {
			if replaced, ok := ctx.Value(putOutcomeKey{}).(*bool); ok {
				*replaced = true
			}
		}
	}
	return f.FileSystem.OpenFile(ctx, name, flag, perm)
}

func TestCopyMoveGuardHandler_PutStatus(t *testing.T) {
	fs := putOutcomeFS{FileSystem: webdav.NewMemFS()}
	h := copyMoveGuardHandler{
		fs:   fs,
		next: &webdav.Handler{FileSystem: fs, LockSystem: webdav.NewMemLS()},
	}
	put := func(body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest("PUT", "http://x/a.txt", strings.NewReader(body))
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		return w
	}
	assert.Equal(t, http.StatusCreated, put("one").Code)
	w := put("two")
	assert.Equal(t, http.StatusNoContent, w.Code)
	assert.Empty(t, w.Body.String(), "a 204 carries no body")
}

// The 204 comes from OpenFile's own bookkeeping: creating over an existing
// entry flags the request context, a fresh create does not.
func TestWebDavFileSystem_OpenFile_PutOutcomeFlag(t *testing.T) {
	filer := &createRecordingFiler{files: map[string]bool{"/old.txt": true}}
	filerAddr := startFakeWebDavFiler(t, filer)
	fs := &WebDavFileSystem{option: &WebDavOption{
		Filer:          filerAddr,
		GrpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
	}}

	open := func(name string) bool {
		replaced := new(bool)
		ctx := context.WithValue(context.Background(), putOutcomeKey{}, replaced)
		f, err := fs.OpenFile(ctx, name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
		assert.NoError(t, err)
		if f != nil {
			assert.NoError(t, f.Close())
		}
		return *replaced
	}

	assert.True(t, open("/old.txt"))
	assert.False(t, open("/new.txt"))
	assert.True(t, open("/new.txt"), "a PUT over the entry the first create made is a replace")
}

type renameRecordingFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	req  *filer_pb.AtomicRenameEntryRequest
	dirs map[string]bool // folders that exist, by full path
}

func (f *renameRecordingFiler) LookupDirectoryEntry(_ context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	if f.dirs[string(util.NewFullPath(req.Directory, req.Name))] {
		return &filer_pb.LookupDirectoryEntryResponse{Entry: &filer_pb.Entry{
			Name:        req.Name,
			IsDirectory: true,
			Attributes:  &filer_pb.FuseAttributes{FileMode: uint32(os.ModeDir | 0755)},
		}}, nil
	}
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
	filer := &renameRecordingFiler{dirs: map[string]bool{"/dir": true}}
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

// MOVE to a missing destination folder must fail before the rename: the filer
// grows any missing parents itself, so an unchecked call would create them.
func TestWebDavFileSystem_Rename_MissingDestParent(t *testing.T) {
	filer := &renameRecordingFiler{dirs: map[string]bool{"/dir": true}}
	filerAddr := startFakeWebDavFiler(t, filer)
	fs := &WebDavFileSystem{option: &WebDavOption{
		Filer:          filerAddr,
		GrpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
	}}

	assert.ErrorIs(t, fs.Rename(context.Background(), "/dir/file.txt", "/missing/moved.txt"), os.ErrNotExist)
	assert.ErrorIs(t, fs.Rename(context.Background(), "/dir/nothere.txt", "/dir/file2.txt"), os.ErrNotExist)
	assert.Nil(t, filer.req)

	assert.NoError(t, fs.Rename(context.Background(), "/dir/file.txt", "/dir/moved.txt"))
	assert.NotNil(t, filer.req)
}
