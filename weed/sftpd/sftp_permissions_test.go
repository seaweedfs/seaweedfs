package sftpd

import (
	"context"
	"errors"
	"net"
	"os"
	"testing"

	"github.com/pkg/sftp"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/sftpd/user"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestPathWithin(t *testing.T) {
	tests := []struct {
		base      string
		candidate string
		want      bool
	}{
		{"/tenants/alice", "/tenants/alice", true},
		{"/tenants/alice", "/tenants/alice/file.txt", true},
		{"/tenants/alice", "/tenants/alice/sub/dir", true},
		{"/tenants/alice", "/tenants/alice-archive", false},
		{"/tenants/alice", "/tenants/alice-archive/secret.txt", false},
		{"/tenants/alice", "/tenants/alice2", false},
		{"/tenants/alice", "/tenants", false},
		{"/tenants/alice/", "/tenants/alice/file.txt", true},
		{"/tenants/alice", "/tenants/alice/../alice-archive", false},
		{"/tenants/alice", "/tenants/alice/..", false},
		{"/", "/", true},
		{"/", "/anything", true},
	}
	for _, tt := range tests {
		if got := pathWithin(tt.base, tt.candidate); got != tt.want {
			t.Errorf("pathWithin(%q, %q) = %v, want %v", tt.base, tt.candidate, got, tt.want)
		}
	}
}

func TestHasExplicitPermissionSiblingPrefix(t *testing.T) {
	u := &user.User{
		Username: "scoped",
		HomeDir:  "/",
		Permissions: map[string][]string{
			"/tenants/alice": {"read", "write", "list"},
		},
	}

	if !HasExplicitPermission(u, "/tenants/alice/file.txt", PermRead, false) {
		t.Error("expected read permission under /tenants/alice")
	}
	if HasExplicitPermission(u, "/tenants/alice-archive/secret.txt", PermRead, false) {
		t.Error("ACL for /tenants/alice must not grant read on sibling /tenants/alice-archive")
	}
	if HasExplicitPermission(u, "/tenants/alice-archive/secret.txt", PermWrite, false) {
		t.Error("ACL for /tenants/alice must not grant write on sibling /tenants/alice-archive")
	}
}

func TestIsPathInHomeDirectorySiblingPrefix(t *testing.T) {
	u := &user.User{Username: "alice", HomeDir: "/tenants/alice"}

	if !isPathInHomeDirectory(u, "/tenants/alice/file.txt") {
		t.Error("expected /tenants/alice/file.txt to be within home /tenants/alice")
	}
	if isPathInHomeDirectory(u, "/tenants/alice-archive/secret.txt") {
		t.Error("home /tenants/alice must not match sibling /tenants/alice-archive")
	}
}

// newFileWriter must honor a more specific ACL on the target file, not just
// the parent directory's: a file the user cannot write stays read-only even
// in a writable directory. Missing paths still defer to the parent directory.
func TestNewFileWriterChecksTargetFile(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	filer := &permissionTestFiler{entries: map[string]*filer_pb.Entry{
		"/":                       {IsDirectory: true, Attributes: &filer_pb.FuseAttributes{FileMode: 0755}},
		"/home":                   {IsDirectory: true, Attributes: &filer_pb.FuseAttributes{FileMode: 0755}},
		"/home/alice":             {IsDirectory: true, Attributes: &filer_pb.FuseAttributes{Uid: 1000, FileMode: 0755}},
		"/home/alice/locked.txt":  {Attributes: &filer_pb.FuseAttributes{Uid: 1000, FileMode: 0644}},
		"/home/alice/ok.txt":      {Attributes: &filer_pb.FuseAttributes{Uid: 1000, FileMode: 0644}},
		"/home/alice/nested":      {IsDirectory: true, Attributes: &filer_pb.FuseAttributes{Uid: 1000, FileMode: 0755}},
		"/home/alice/nested/ok.f": {Attributes: &filer_pb.FuseAttributes{Uid: 1000, FileMode: 0644}},
	}}
	grpcServer := grpc.NewServer()
	filer_pb.RegisterSeaweedFilerServer(grpcServer, filer)
	go func() { _ = grpcServer.Serve(lis) }()
	t.Cleanup(grpcServer.Stop)

	_, port, _ := net.SplitHostPort(lis.Addr().String())
	fs := &SftpServer{
		filerAddr:      pb.ServerAddress("127.0.0.1:0." + port),
		grpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
		user: &user.User{
			Username: "alice", Uid: 1000, Gid: 1000, HomeDir: "/home/alice",
			Permissions: map[string][]string{
				"/home/alice":            {"readwrite"},
				"/home/alice/locked.txt": {"read"},
			},
		},
	}

	write := func(path string) error {
		w, err := fs.newFileWriter(&sftp.Request{Filepath: path})
		if writer, ok := w.(*SeaweedSftpFileWriter); ok {
			_ = os.Remove(writer.tmpFile.Name())
			_ = writer.tmpFile.Close()
		}
		return err
	}

	if err := write("/locked.txt"); err == nil {
		t.Error("expected write denial on a file with an explicit read-only ACL")
	}
	if err := write("/ok.txt"); err != nil {
		t.Errorf("expected write under the directory ACL, got %v", err)
	}
	if err := write("/new.txt"); err != nil {
		t.Errorf("expected write on a new file in a writable dir, got %v", err)
	}
	if err := write("/nested/new.txt"); err != nil {
		t.Errorf("expected write on a new file under a missing intermediate check, got %v", err)
	}
	delete(filer.entries, "/home/alice/nested")
	delete(filer.entries, "/home/alice/nested/ok.f")
	if err := write("/gone/deeper.txt"); err != nil {
		t.Errorf("expected write when ancestors resolve to the writable dir, got %v", err)
	}
}

// renameEntry must honor a more specific ACL on an existing target: renaming
// over a read-only file deletes it, so it requires delete permission on the
// target file itself, not only write permission on the target directory.
func TestRenameEntryChecksTargetFile(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	filer := &permissionTestFiler{entries: map[string]*filer_pb.Entry{
		"/home/alice":            {IsDirectory: true, Attributes: &filer_pb.FuseAttributes{Uid: 1000, FileMode: 0755}},
		"/home/alice/own.txt":    {Attributes: &filer_pb.FuseAttributes{Uid: 1000, FileMode: 0644}},
		"/home/alice/ok.txt":     {Attributes: &filer_pb.FuseAttributes{Uid: 1000, FileMode: 0644}},
		"/home/alice/locked.txt": {Attributes: &filer_pb.FuseAttributes{Uid: 1000, FileMode: 0644}},
	}}
	grpcServer := grpc.NewServer()
	filer_pb.RegisterSeaweedFilerServer(grpcServer, filer)
	go func() { _ = grpcServer.Serve(lis) }()
	t.Cleanup(grpcServer.Stop)

	_, port, _ := net.SplitHostPort(lis.Addr().String())
	fs := &SftpServer{
		filerAddr:      pb.ServerAddress("127.0.0.1:0." + port),
		grpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
		user: &user.User{
			Username: "alice", Uid: 1000, Gid: 1000, HomeDir: "/home/alice",
			Permissions: map[string][]string{
				"/home/alice":            {"read", "write", "list", "delete", "rename"},
				"/home/alice/locked.txt": {"read", "list"},
			},
		},
	}

	// the fake filer has no AtomicRenameEntry, so an authorized rename fails
	// with a gRPC error; a denied one fails with os.ErrPermission
	renameDenied := func(src, dst string) bool {
		return errors.Is(fs.renameEntry(src, dst), os.ErrPermission)
	}

	if !renameDenied("/home/alice/own.txt", "/home/alice/locked.txt") {
		t.Error("expected rename over a read-only file to be denied")
	}
	if renameDenied("/home/alice/own.txt", "/home/alice/ok.txt") {
		t.Error("expected rename over a writable file to be allowed")
	}
	if renameDenied("/home/alice/own.txt", "/home/alice/new.txt") {
		t.Error("expected rename onto a new path in a writable dir to be allowed")
	}
}

func TestCheckFilePermissionMissingAncestorsTerminates(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	filer := &permissionTestFiler{entries: map[string]*filer_pb.Entry{}}
	grpcServer := grpc.NewServer()
	filer_pb.RegisterSeaweedFilerServer(grpcServer, filer)
	go func() { _ = grpcServer.Serve(lis) }()
	t.Cleanup(grpcServer.Stop)

	_, port, _ := net.SplitHostPort(lis.Addr().String())
	fs := &SftpServer{
		filerAddr:      pb.ServerAddress("127.0.0.1:0." + port),
		grpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
		user:           &user.User{Username: "alice", Uid: 1000, HomeDir: "/home/alice"},
	}
	if err := fs.checkFilePermission("/a/b/c", "write"); err == nil {
		t.Error("expected denial when no ancestor exists")
	}
}

type permissionTestFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	entries map[string]*filer_pb.Entry
}

func (f *permissionTestFiler) LookupDirectoryEntry(_ context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	if entry, ok := f.entries[req.Directory+"/"+req.Name]; ok {
		return &filer_pb.LookupDirectoryEntryResponse{Entry: entry}, nil
	}
	return nil, status.Error(codes.NotFound, "no entry is found")
}
