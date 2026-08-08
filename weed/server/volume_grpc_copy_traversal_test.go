package weed_server

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// newTraversalTestStore builds a single-location store rooted at dir.
func newTraversalTestStore(dir string) *storage.Store {
	return storage.NewStore(
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		"127.0.0.1", 0, 0, "", "test-store",
		[]string{dir}, []int32{10}, []util.MinFreeSpace{{}},
		dir, storage.NeedleMapInMemory,
		[]types.DiskType{types.HardDriveType}, [][]string{nil},
		0, stats.DiskIOProbeConfig{},
	)
}

// fakeReceiveFileStream scripts a ReceiveFile request sequence and records the
// final response returned via SendAndClose.
type fakeReceiveFileStream struct {
	grpc.ServerStream
	reqs  []*volume_server_pb.ReceiveFileRequest
	index int
	resp  *volume_server_pb.ReceiveFileResponse
}

func (s *fakeReceiveFileStream) Recv() (*volume_server_pb.ReceiveFileRequest, error) {
	if s.index >= len(s.reqs) {
		return nil, io.EOF
	}
	r := s.reqs[s.index]
	s.index++
	return r, nil
}

func (s *fakeReceiveFileStream) SendAndClose(resp *volume_server_pb.ReceiveFileResponse) error {
	s.resp = resp
	return nil
}

func infoReq(info *volume_server_pb.ReceiveFileInfo) *volume_server_pb.ReceiveFileRequest {
	return &volume_server_pb.ReceiveFileRequest{Data: &volume_server_pb.ReceiveFileRequest_Info{Info: info}}
}

func contentReq(b []byte) *volume_server_pb.ReceiveFileRequest {
	return &volume_server_pb.ReceiveFileRequest{Data: &volume_server_pb.ReceiveFileRequest_FileContent{FileContent: b}}
}

// TestReceiveFile_RejectsTraversalExt ensures a client-supplied Ext with parent
// references cannot steer the EC-shard write outside the volume directory. The
// EC branch joins the Ext with util.Join, which path-cleans the ".." away
// before os.Create, so a bare concatenation check would miss it.
func TestReceiveFile_RejectsTraversalExt(t *testing.T) {
	root := t.TempDir()
	storeDir := filepath.Join(root, "a", "b", "store")
	if err := os.MkdirAll(storeDir, 0o755); err != nil {
		t.Fatal(err)
	}
	vs := &VolumeServer{store: newTraversalTestStore(storeDir)}

	stream := &fakeReceiveFileStream{reqs: []*volume_server_pb.ReceiveFileRequest{
		infoReq(&volume_server_pb.ReceiveFileInfo{
			VolumeId:   4,
			Ext:        "/../../pwned.ec00",
			IsEcVolume: true,
			FileSize:   5,
		}),
		contentReq([]byte("pwned")),
	}}

	if err := vs.ReceiveFile(stream); err != nil {
		t.Fatalf("ReceiveFile returned transport error: %v", err)
	}
	if stream.resp == nil || stream.resp.Error == "" {
		t.Errorf("traversal ext was accepted; response = %+v", stream.resp)
	}

	assertNoEscapedFile(t, root, storeDir, "pwned")
}

// TestReceiveFile_AcceptsNormalExt is the positive control: a legitimate EC
// shard extension still lands inside the volume directory.
func TestReceiveFile_AcceptsNormalExt(t *testing.T) {
	storeDir := t.TempDir()
	vs := &VolumeServer{store: newTraversalTestStore(storeDir)}

	stream := &fakeReceiveFileStream{reqs: []*volume_server_pb.ReceiveFileRequest{
		infoReq(&volume_server_pb.ReceiveFileInfo{
			VolumeId:   4,
			Ext:        ".ec00",
			IsEcVolume: true,
			FileSize:   5,
		}),
		contentReq([]byte("shard")),
	}}

	if err := vs.ReceiveFile(stream); err != nil {
		t.Fatalf("ReceiveFile: %v", err)
	}
	if stream.resp == nil || stream.resp.Error != "" {
		t.Fatalf("normal ext rejected: %+v", stream.resp)
	}
	if got, err := os.ReadFile(filepath.Join(storeDir, "4.ec00")); err != nil || string(got) != "shard" {
		t.Fatalf("expected shard written inside volume dir, got %q err %v", got, err)
	}
}

// fakeCopyFileServer records streamed content for CopyFile.
type fakeCopyFileServer struct {
	grpc.ServerStream
	sent []byte
}

func (s *fakeCopyFileServer) Send(resp *volume_server_pb.CopyFileResponse) error {
	s.sent = append(s.sent, resp.FileContent...)
	return nil
}

// TestCopyFile_RejectsTraversalExt ensures the read side cannot be steered to an
// arbitrary file with a traversal Ext.
func TestCopyFile_RejectsTraversalExt(t *testing.T) {
	root := t.TempDir()
	storeDir := filepath.Join(root, "a", "b", "store")
	if err := os.MkdirAll(storeDir, 0o755); err != nil {
		t.Fatal(err)
	}
	secret := filepath.Join(root, "a", "b", "secret.txt")
	if err := os.WriteFile(secret, []byte("top secret"), 0o644); err != nil {
		t.Fatal(err)
	}
	vs := &VolumeServer{store: newTraversalTestStore(storeDir)}

	// util.Join(storeDir, "4"+"/../../secret.txt") path-cleans to the secret.
	req := &volume_server_pb.CopyFileRequest{
		VolumeId:   4,
		Ext:        "/../../secret.txt",
		IsEcVolume: true,
		StopOffset: 1 << 20,
	}
	server := &fakeCopyFileServer{}
	err := vs.CopyFile(req, server)
	if err == nil {
		t.Errorf("CopyFile accepted a traversal ext")
	}
	if len(server.sent) != 0 {
		t.Errorf("CopyFile leaked %d bytes for a traversal ext: %q", len(server.sent), server.sent)
	}
}

// assertNoEscapedFile fails if any file whose name contains needle exists under
// root but outside storeDir.
func assertNoEscapedFile(t *testing.T, root, storeDir, needle string) {
	t.Helper()
	_ = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		rel, _ := filepath.Rel(storeDir, path)
		outside := rel == "" || rel[0] == '.'
		if outside && strings.Contains(d.Name(), needle) {
			t.Errorf("path traversal: file written outside volume dir at %s", path)
		}
		return nil
	})
}
