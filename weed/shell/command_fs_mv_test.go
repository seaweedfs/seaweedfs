package shell

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type fsMvTestFilerServer struct {
	filer_pb.UnimplementedSeaweedFilerServer

	lookupReq *filer_pb.LookupDirectoryEntryRequest
	renameReq *filer_pb.AtomicRenameEntryRequest
}

func (s *fsMvTestFilerServer) LookupDirectoryEntry(_ context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	s.lookupReq = req
	if req.Directory == "/dst" && req.Name == "dir" {
		return &filer_pb.LookupDirectoryEntryResponse{
			Entry: &filer_pb.Entry{
				Name:        "dir",
				IsDirectory: true,
				Attributes:  &filer_pb.FuseAttributes{},
			},
		}, nil
	}
	return nil, status.Error(codes.NotFound, "not found")
}

func (s *fsMvTestFilerServer) AtomicRenameEntry(_ context.Context, req *filer_pb.AtomicRenameEntryRequest) (*filer_pb.AtomicRenameEntryResponse, error) {
	s.renameReq = req
	return &filer_pb.AtomicRenameEntryResponse{}, nil
}

func TestFsMvMovesIntoExistingDestinationDirectory(t *testing.T) {
	filerServer := &fsMvTestFilerServer{}
	commandEnv, cleanup := newFsMvTestCommandEnv(t, filerServer)
	defer cleanup()

	var output bytes.Buffer
	err := (&commandFsMv{}).Do([]string{"/src/file", "/dst/dir"}, commandEnv, &output)
	if err != nil {
		t.Fatalf("fs.mv returned error: %v", err)
	}

	if filerServer.lookupReq == nil {
		t.Fatal("expected fs.mv to look up destination entry")
	}
	if filerServer.lookupReq.Directory != "/dst" || filerServer.lookupReq.Name != "dir" {
		t.Fatalf("destination lookup = directory %q name %q, want /dst dir", filerServer.lookupReq.Directory, filerServer.lookupReq.Name)
	}
	if filerServer.renameReq == nil {
		t.Fatal("expected fs.mv to issue rename")
	}
	if filerServer.renameReq.NewDirectory != "/dst/dir" || filerServer.renameReq.NewName != "file" {
		t.Fatalf("rename target = directory %q name %q, want /dst/dir file", filerServer.renameReq.NewDirectory, filerServer.renameReq.NewName)
	}
}

func newFsMvTestCommandEnv(t *testing.T, filerServer filer_pb.SeaweedFilerServer) (*CommandEnv, func()) {
	t.Helper()

	socketDir, err := os.MkdirTemp("/private/tmp", "swmv-")
	if err != nil {
		t.Fatalf("create socket dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketDir) })

	socketPath := filepath.Join(socketDir, "filer.sock")
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatalf("listen unix socket: %v", err)
	}

	grpcServer := grpc.NewServer()
	filer_pb.RegisterSeaweedFilerServer(grpcServer, filerServer)
	go func() {
		_ = grpcServer.Serve(listener)
	}()

	grpcPort := 47000 + os.Getpid()%1000
	pb.RegisterLocalGrpcSocket("127.0.0.1", grpcPort, socketPath)

	cleanup := func() {
		grpcServer.Stop()
		_ = listener.Close()
	}

	return &CommandEnv{
		option: &ShellOptions{
			FilerAddress:   pb.ServerAddress(fmt.Sprintf("127.0.0.1:8888.%d", grpcPort)),
			GrpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
			Directory:      "/",
		},
	}, cleanup
}
