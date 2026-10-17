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

	renameDenied := func(src, dst string) bool {
		return errors.Is(fs.renameEntry(src, dst), os.ErrPermission)
	}

	if !renameDenied("/home/alice/own.txt", "/home/alice/locked.txt") {
		t.Error("expected rename over a read-only file to be denied")
	}
	if err := fs.renameEntry("/home/alice/own.txt", "/home/alice/ok.txt"); err != nil {
		t.Errorf("expected rename over a writable file to succeed, got %v", err)
	}
	if err := fs.renameEntry("/home/alice/ok.txt", "/home/alice/new.txt"); err != nil {
		t.Errorf("expected rename onto a new path in a writable dir to succeed, got %v", err)
	}
	if err := fs.renameEntry("/home/alice/new.txt", "/home/alice/new.txt"); err != nil {
		t.Errorf("expected a no-op rename to skip the target delete check, got %v", err)
	}
}

// Renaming a directory must not strand a more specific ACL configured under
// it: after /sub moves to /sub2, /sub/ro.txt no longer matches its read-only
// ACL, so the move is denied unless the destination grants no new access.
func TestRenameEntryKeepsNestedAcl(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	filer := &permissionTestFiler{entries: map[string]*filer_pb.Entry{
		"/home/alice":            {IsDirectory: true, Attributes: &filer_pb.FuseAttributes{Uid: 1000, FileMode: 0755}},
		"/home/alice/sub":        {IsDirectory: true, Attributes: &filer_pb.FuseAttributes{Uid: 1000, FileMode: 0755}},
		"/home/alice/sub/ro.txt": {Attributes: &filer_pb.FuseAttributes{Uid: 1000, FileMode: 0644}},
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
				"/home/alice/sub/ro.txt": {"read", "list"},
			},
		},
	}

	if err := fs.renameEntry("/home/alice/sub", "/home/alice/sub2"); !errors.Is(err, os.ErrPermission) {
		t.Errorf("expected rename that strands the /sub/ro.txt ACL to be denied, got %v", err)
	}
	delete(fs.user.Permissions, "/home/alice/sub/ro.txt")
	if err := fs.renameEntry("/home/alice/sub", "/home/alice/sub2"); err != nil {
		t.Errorf("expected rename without a nested carve-out to succeed, got %v", err)
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
	entries   map[string]*filer_pb.Entry
	lookupErr map[string]error
	// createBeforeRename inserts an entry at the rename target inside the RPC,
	// simulating a file a concurrent connection creates after the checks ran.
	createBeforeRename map[string]*filer_pb.Entry
}

func (f *permissionTestFiler) LookupDirectoryEntry(_ context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	key := req.Directory + "/" + req.Name
	if err, ok := f.lookupErr[key]; ok {
		return nil, err
	}
	if entry, ok := f.entries[key]; ok {
		return &filer_pb.LookupDirectoryEntryResponse{Entry: entry}, nil
	}
	return nil, status.Error(codes.NotFound, "no entry is found")
}

func (f *permissionTestFiler) AtomicRenameEntry(_ context.Context, req *filer_pb.AtomicRenameEntryRequest) (*filer_pb.AtomicRenameEntryResponse, error) {
	oldKey := req.OldDirectory + "/" + req.OldName
	entry, ok := f.entries[oldKey]
	if !ok {
		return nil, status.Error(codes.NotFound, "no entry is found")
	}
	newKey := req.NewDirectory + "/" + req.NewName
	if created, ok := f.createBeforeRename[newKey]; ok {
		f.entries[newKey] = created
	}
	if req.FailIfTargetExists {
		if _, exists := f.entries[newKey]; exists && newKey != oldKey {
			return nil, status.Error(codes.AlreadyExists, "entry already exists")
		}
	}
	delete(f.entries, oldKey)
	f.entries[newKey] = entry
	return &filer_pb.AtomicRenameEntryResponse{}, nil
}

// checkRenameKeepsAcl also compares the moved root (inherited restrictions) and
// ACL paths configured under the destination (deeper grants), while configured
// paths without a real entry do not block the rename.
func TestRenameEntryKeepsInheritedAndDestinationAcl(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	filer := &permissionTestFiler{entries: map[string]*filer_pb.Entry{
		"/home/alice":                   {IsDirectory: true, Attributes: &filer_pb.FuseAttributes{Uid: 1000, FileMode: 0755}},
		"/home/alice/restricted":        {IsDirectory: true, Attributes: &filer_pb.FuseAttributes{Uid: 1000, FileMode: 0755}},
		"/home/alice/restricted/sub":    {IsDirectory: true, Attributes: &filer_pb.FuseAttributes{Uid: 1000, FileMode: 0755}},
		"/home/alice/sub":               {IsDirectory: true, Attributes: &filer_pb.FuseAttributes{Uid: 1000, FileMode: 0755}},
		"/home/alice/sub/locked":        {IsDirectory: true, Attributes: &filer_pb.FuseAttributes{Uid: 1000, FileMode: 0755}},
		"/home/alice/sub/locked/secret": {Attributes: &filer_pb.FuseAttributes{Uid: 1000, FileMode: 0644}},
		"/home/alice/sub3":              {IsDirectory: true, Attributes: &filer_pb.FuseAttributes{Uid: 1000, FileMode: 0755}},
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
				"/home/alice":                    {"read", "write", "list", "delete", "rename"},
				"/home/alice/restricted":         {"read", "list", "rename"},
				"/home/alice/sub/locked":         {"read", "list"},
				"/home/alice/sub2/locked":        {"read", "list"},
				"/home/alice/sub2/locked/secret": {"read", "write", "list", "delete"},
				"/home/alice/sub3/ghost.txt":     {"read", "list"},
			},
		},
	}

	if err := fs.renameEntry("/home/alice/restricted/sub", "/home/alice/sub5"); !errors.Is(err, os.ErrPermission) {
		t.Errorf("expected rename escaping the inherited restricted ACL to be denied, got %v", err)
	}
	if err := fs.renameEntry("/home/alice/sub", "/home/alice/sub2"); !errors.Is(err, os.ErrPermission) {
		t.Errorf("expected rename into a deeper destination grant to be denied, got %v", err)
	}
	if err := fs.renameEntry("/home/alice/sub3", "/home/alice/sub4"); err != nil {
		t.Errorf("expected rename with a missing reserved ACL child to succeed, got %v", err)
	}
}

// A rename onto a name that does not exist needs no delete access unless a
// configured ACL restricts that name itself. Without the scoped check a user
// holding write+rename but not delete could no longer move files to new names.
func TestRenameEntryNewNameNeedsNoDelete(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	filer := &permissionTestFiler{entries: map[string]*filer_pb.Entry{
		"/home/alice":               {IsDirectory: true, Attributes: &filer_pb.FuseAttributes{Uid: 1000, FileMode: 0755}},
		"/home/alice/own.txt":       {Attributes: &filer_pb.FuseAttributes{Uid: 1000, FileMode: 0644}},
		"/home/alice/existing.txt":  {Attributes: &filer_pb.FuseAttributes{Uid: 1000, FileMode: 0644}},
		"/home/alice/removable.txt": {Attributes: &filer_pb.FuseAttributes{Uid: 1000, FileMode: 0644}},
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
				"/home/alice":               {"read", "write", "list", "rename"},
				"/home/alice/reserved.txt":  {"read", "list"},
				"/home/alice/managed.txt":   {"read", "write", "list", "delete"},
				"/home/alice/removable.txt": {"read", "write", "list", "delete", "rename"},
			},
		},
	}

	if err := fs.renameEntry("/home/alice/own.txt", "/home/alice/new.txt"); err != nil {
		t.Errorf("expected rename to a new name without delete access to succeed, got %v", err)
	}
	if err := fs.renameEntry("/home/alice/new.txt", "/home/alice/existing.txt"); !errors.Is(err, os.ErrPermission) {
		t.Errorf("expected rename over an existing file without delete access to be denied, got %v", err)
	}
	if err := fs.renameEntry("/home/alice/new.txt", "/home/alice/reserved.txt"); !errors.Is(err, os.ErrPermission) {
		t.Errorf("expected rename onto a delete-protected configured name to be denied, got %v", err)
	}
	if err := fs.renameEntry("/home/alice/removable.txt", "/home/alice/managed.txt"); err != nil {
		t.Errorf("expected rename onto a configured name that grants delete to succeed, got %v", err)
	}
}

// checkRenameKeepsAcl fails closed: a lookup error on a configured ACL path is
// not proof that the entry is missing, so it must stop the rename instead of
// skipping the comparison.
func TestRenameEntryFailsClosedOnLookupError(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	filer := &permissionTestFiler{
		entries: map[string]*filer_pb.Entry{
			"/home/alice":            {IsDirectory: true, Attributes: &filer_pb.FuseAttributes{Uid: 1000, FileMode: 0755}},
			"/home/alice/sub":        {IsDirectory: true, Attributes: &filer_pb.FuseAttributes{Uid: 1000, FileMode: 0755}},
			"/home/alice/sub/ro.txt": {Attributes: &filer_pb.FuseAttributes{Uid: 1000, FileMode: 0644}},
		},
		lookupErr: map[string]error{
			"/home/alice/sub/ro.txt": status.Error(codes.Unavailable, "filer unavailable"),
		},
	}
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
				"/home/alice/sub/ro.txt": {"read", "list"},
			},
		},
	}

	err = fs.renameEntry("/home/alice/sub", "/home/alice/sub2")
	if err == nil || errors.Is(err, os.ErrPermission) {
		t.Errorf("expected rename to fail closed on a lookup error, got %v", err)
	}
	delete(filer.lookupErr, "/home/alice/sub/ro.txt")
	if err := fs.renameEntry("/home/alice/sub", "/home/alice/sub2"); !errors.Is(err, os.ErrPermission) {
		t.Errorf("expected the stranded ACL check to deny the rename once the lookup succeeds, got %v", err)
	}
}

// A rename must not grant access the source side lacked, including read and
// directory listing: moving a subtree whose ACL withholds read/list into a
// readable destination would expose its contents.
func TestRenameEntryKeepsReadAndListAcl(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	filer := &permissionTestFiler{entries: map[string]*filer_pb.Entry{
		"/home/alice":              {IsDirectory: true, Attributes: &filer_pb.FuseAttributes{Uid: 1000, FileMode: 0755}},
		"/home/alice/noread":       {IsDirectory: true, Attributes: &filer_pb.FuseAttributes{Uid: 1000, FileMode: 0755}},
		"/home/alice/noread/f.txt": {Attributes: &filer_pb.FuseAttributes{Uid: 1000, FileMode: 0644}},
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
				"/home/alice":        {"read", "write", "list", "delete", "rename"},
				"/home/alice/noread": {"write", "delete", "rename"},
			},
		},
	}

	if err := fs.renameEntry("/home/alice/noread", "/home/alice/visible"); !errors.Is(err, os.ErrPermission) {
		t.Errorf("expected rename that grants read/list on the moved directory to be denied, got %v", err)
	}
}

// A moved directory carries file children whose ACLs resolve with
// isDirectory=false, where list does not count as read. Moving a directory
// that grants list but not read into a readable destination must be denied:
// its file children would become readable even though the directory check
// passes.
func TestRenameEntryKeepsFileChildAcl(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	filer := &permissionTestFiler{entries: map[string]*filer_pb.Entry{
		"/home/alice":                    {IsDirectory: true, Attributes: &filer_pb.FuseAttributes{Uid: 1000, FileMode: 0755}},
		"/home/alice/private":            {IsDirectory: true, Attributes: &filer_pb.FuseAttributes{Uid: 1000, FileMode: 0755}},
		"/home/alice/private/secret.txt": {Attributes: &filer_pb.FuseAttributes{Uid: 1000, FileMode: 0644}},
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
				"/home/alice":         {"read", "write", "list", "delete", "rename"},
				"/home/alice/private": {"list", "write", "delete", "rename"},
			},
		},
	}

	if err := fs.renameEntry("/home/alice/private", "/home/alice/public"); !errors.Is(err, os.ErrPermission) {
		t.Errorf("expected rename that grants read on file children to be denied, got %v", err)
	}
	fs.user.Permissions["/home/alice/public"] = []string{"list", "write", "delete", "rename"}
	if err := fs.renameEntry("/home/alice/private", "/home/alice/public"); err != nil {
		t.Errorf("expected rename onto an equally restricted ACL to succeed, got %v", err)
	}
}

// A rename onto a new name needs no delete access, but a target created in the
// gap between the check and the move must not be silently overwritten: the
// filer is asked to fail the rename when the target appears.
func TestRenameEntryRejectsConcurrentTarget(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	filer := &permissionTestFiler{
		entries: map[string]*filer_pb.Entry{
			"/home/alice":         {IsDirectory: true, Attributes: &filer_pb.FuseAttributes{Uid: 1000, FileMode: 0755}},
			"/home/alice/own.txt": {Attributes: &filer_pb.FuseAttributes{Uid: 1000, FileMode: 0644}},
		},
		createBeforeRename: map[string]*filer_pb.Entry{
			"/home/alice/taken.txt": {Attributes: &filer_pb.FuseAttributes{Uid: 1000, FileMode: 0644}},
		},
	}
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
				"/home/alice": {"read", "write", "list", "rename"},
			},
		},
	}

	if err := fs.renameEntry("/home/alice/own.txt", "/home/alice/free.txt"); err != nil {
		t.Errorf("expected rename to a name that stays absent to succeed, got %v", err)
	}
	if err := fs.renameEntry("/home/alice/free.txt", "/home/alice/taken.txt"); err == nil {
		t.Error("expected rename to fail when the target appears concurrently and the user lacks delete access")
	}
}
