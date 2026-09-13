package weed_server

import (
	"context"
	"fmt"
	"net"
	"os"
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

	failed := &FileInfo{err: os.ErrInvalid}
	if _, err := failed.ETag(ctx); err != os.ErrInvalid {
		t.Errorf("ETag() = %v, want the stat error", err)
	}
}

type noVolumeFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
}

func (f *noVolumeFiler) LookupVolume(_ context.Context, _ *filer_pb.LookupVolumeRequest) (*filer_pb.LookupVolumeResponse, error) {
	return &filer_pb.LookupVolumeResponse{LocationsMap: map[string]*filer_pb.Locations{}}, nil
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
