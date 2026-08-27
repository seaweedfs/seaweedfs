package shell

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestVolumeFsckCanPurgeDirectory(t *testing.T) {
	testCases := []struct {
		scopedFilerPath string
		dir             util.FullPath
		expected        bool
	}{
		{"/", "/orphan/dir/deep", true},
		{"/", "/orphan", true},
		{"/", "/", false},
		{"/", "/buckets", false},
		{"/", "/buckets/bucket1", false},
		{"/", "/buckets/bucket1/dir", true},
		{"/buckets/bucket1", "/buckets/bucket1", false},
		{"/buckets/bucket1", "/buckets/bucket1/dir", true},
		{"/buckets/bucket1", "/buckets/bucket11/dir", false},
		{"/buckets/bucket1", "/orphan/dir", false},
	}
	for _, tc := range testCases {
		c := &commandVolumeFsck{bucketsPath: "/buckets", scopedFilerPath: tc.scopedFilerPath}
		if actual := c.canPurgeDirectory(tc.dir); actual != tc.expected {
			t.Errorf("scope %s: canPurgeDirectory(%s) = %v, expected %v", tc.scopedFilerPath, tc.dir, actual, tc.expected)
		}
	}
}

func TestVolumeFsckHttpDeleteRecordsParentDirectory(t *testing.T) {
	filer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/locked/") {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer filer.Close()

	verbose := false
	c := &commandVolumeFsck{
		verbose:    &verbose,
		writer:     io.Discard,
		purgedDirs: make(map[util.FullPath]struct{}),
		env:        &CommandEnv{option: &ShellOptions{FilerAddress: pb.ServerAddress(strings.TrimPrefix(filer.URL, "http://"))}},
	}

	c.httpDelete("/orphan/dir/deep/file.txt")
	c.httpDelete("/locked/dir/file.txt")

	if _, found := c.purgedDirs["/orphan/dir/deep"]; !found {
		t.Errorf("expected /orphan/dir/deep to be recorded, got %v", c.purgedDirs)
	}
	if _, found := c.purgedDirs["/locked/dir"]; found {
		t.Errorf("expected a rejected delete not to be recorded, got %v", c.purgedDirs)
	}
}

// stubFilerServer keeps a flat set of full paths and enforces the same
// non-recursive delete rule as the filer: a directory with children is kept.
type stubFilerServer struct {
	filer_pb.UnimplementedSeaweedFilerServer
	entries map[string]*filer_pb.Entry
	deleted []string
}

func (s *stubFilerServer) LookupDirectoryEntry(ctx context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	return &filer_pb.LookupDirectoryEntryResponse{Entry: s.entries[string(util.NewFullPath(req.Directory, req.Name))]}, nil
}

func (s *stubFilerServer) DeleteEntry(ctx context.Context, req *filer_pb.DeleteEntryRequest) (*filer_pb.DeleteEntryResponse, error) {
	fullPath := string(util.NewFullPath(req.Directory, req.Name))
	if _, found := s.entries[fullPath]; !found {
		return &filer_pb.DeleteEntryResponse{Error: filer_pb.ErrNotFound.Error()}, nil
	}
	for path := range s.entries {
		if strings.HasPrefix(path, fullPath+"/") {
			return &filer_pb.DeleteEntryResponse{Error: filer.MsgFailDelNonEmptyFolder + ": " + fullPath}, nil
		}
	}
	delete(s.entries, fullPath)
	s.deleted = append(s.deleted, fullPath)
	return &filer_pb.DeleteEntryResponse{}, nil
}

func TestVolumeFsckPurgeEmptyDirectories(t *testing.T) {
	directory := &filer_pb.Entry{IsDirectory: true}
	stub := &stubFilerServer{entries: map[string]*filer_pb.Entry{
		"/orphan":                 directory,
		"/orphan/dir":             directory,
		"/orphan/dir/deep":        directory,
		"/orphan/dir/wide":        directory,
		"/keep":                   directory,
		"/keep/file.txt":          {},
		"/buckets":                directory,
		"/buckets/bucket1":        directory,
		"/buckets/bucket1/folder": {IsDirectory: true, Attributes: &filer_pb.FuseAttributes{Mime: "application/octet-stream"}},
	}}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	filer_pb.RegisterSeaweedFilerServer(srv, stub)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)

	verbose := false
	c := &commandVolumeFsck{
		verbose:         &verbose,
		writer:          io.Discard,
		bucketsPath:     "/buckets",
		scopedFilerPath: "/",
		purgedDirs: map[util.FullPath]struct{}{
			"/orphan/dir/deep":        {},
			"/orphan/dir/wide":        {},
			"/keep":                   {},
			"/buckets/bucket1/folder": {},
		},
		env: &CommandEnv{option: &ShellOptions{
			FilerAddress:   pb.ServerAddress(fmt.Sprintf("127.0.0.1:1.%d", lis.Addr().(*net.TCPAddr).Port)),
			GrpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
		}},
	}

	c.purgeEmptyDirectories()

	// the two emptied leaves, then the parent they shared, then its own parent
	expected := []string{"/orphan/dir/deep", "/orphan/dir/wide", "/orphan/dir", "/orphan"}
	sort.Strings(expected)
	sort.Strings(stub.deleted)
	if strings.Join(stub.deleted, ",") != strings.Join(expected, ",") {
		t.Errorf("deleted %v, expected %v", stub.deleted, expected)
	}
}
