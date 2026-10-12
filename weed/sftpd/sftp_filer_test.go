package sftpd

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/pkg/sftp"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/sftpd/user"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// TestPutFileEscapesQueryInjection ensures a filename containing "?" cannot be
// reinterpreted by the filer as a query string that injects cp.from/mv.from
// commands, which would let an SFTP user escape their home directory.
func TestPutFileEscapesQueryInjection(t *testing.T) {
	var gotPath, gotRawQuery string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotRawQuery = r.URL.RawQuery
		if _, err := w.Write([]byte(`{}`)); err != nil {
			t.Errorf("write response: %v", err)
		}
	}))
	defer ts.Close()

	fs := &SftpServer{filerAddr: pb.ServerAddress(strings.TrimPrefix(ts.URL, "http://"))}

	malicious := "/home/alice/steal?cp.from=/home/bob/secret.txt"
	if err := fs.putFile(malicious, strings.NewReader("dummy"), nil); err != nil {
		t.Fatalf("putFile: %v", err)
	}

	if gotRawQuery != "" {
		t.Errorf("filename leaked into query string: RawQuery=%q", gotRawQuery)
	}
	if gotPath != malicious {
		t.Errorf("path not delivered literally: got %q, want %q", gotPath, malicious)
	}
}

type fakeListFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	names    []string
	failFrom int // ListEntries fails after sending this many entries, when > 0
}

func (f *fakeListFiler) ListEntries(req *filer_pb.ListEntriesRequest, stream grpc.ServerStreamingServer[filer_pb.ListEntriesResponse]) error {
	sent := 0
	for _, name := range f.names {
		if name <= req.StartFromFileName || sent >= int(req.Limit) {
			continue
		}
		if f.failFrom > 0 && sent == f.failFrom {
			return status.Error(codes.Unavailable, "filer went away")
		}
		if err := stream.Send(&filer_pb.ListEntriesResponse{Entry: &filer_pb.Entry{Name: name, Attributes: &filer_pb.FuseAttributes{}}}); err != nil {
			return err
		}
		sent++
	}
	return nil
}

func (f *fakeListFiler) LookupDirectoryEntry(ctx context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	return &filer_pb.LookupDirectoryEntryResponse{Entry: &filer_pb.Entry{Name: req.Name, Attributes: &filer_pb.FuseAttributes{}}}, nil
}

func newListTestServer(t *testing.T, filer *fakeListFiler) *SftpServer {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	filer_pb.RegisterSeaweedFilerServer(srv, filer)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	return &SftpServer{
		filerAddr:      pb.ServerAddress(fmt.Sprintf("127.0.0.1:1.%d", lis.Addr().(*net.TCPAddr).Port)),
		grpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
		user: &user.User{
			Username: "alice",
			HomeDir:  "/",
			Uid:      1001,
			Gid:      1001,
			// every entry under /d is listable except f00000
			Permissions: map[string][]string{"/d": {"list"}, "/d/f00000": {}},
		},
	}
}

func listNames(t *testing.T, fs *SftpServer) ([]string, error) {
	t.Helper()
	type result struct {
		lister sftp.ListerAt
		err    error
	}
	done := make(chan result, 1)
	go func() {
		lister, err := fs.listAllPages("/d")
		done <- result{lister, err}
	}()
	select {
	case r := <-done:
		if r.err != nil {
			return nil, r.err
		}
		infos := make([]os.FileInfo, 10000)
		n, _ := r.lister.ListAt(infos, 0)
		var names []string
		for _, fi := range infos[:n] {
			names = append(names, fi.Name())
		}
		return names, nil
	case <-time.After(10 * time.Second):
		t.Fatal("listing did not return")
		return nil, nil
	}
}

func fileNames(n int) []string {
	names := make([]string, n)
	for i := range names {
		names[i] = fmt.Sprintf("f%05d", i)
	}
	return names
}

func TestListAllPagesReturnsStreamError(t *testing.T) {
	fs := newListTestServer(t, &fakeListFiler{names: fileNames(10), failFrom: 3})
	_, err := listNames(t, fs)
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("expected the stream error, got %v", err)
	}
}

func TestListAllPagesContinuesPastHiddenEntries(t *testing.T) {
	fs := newListTestServer(t, &fakeListFiler{names: fileNames(defaultListLimit + 500)})
	names, err := listNames(t, fs)
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != defaultListLimit+499 {
		t.Fatalf("expected %d entries, got %d", defaultListLimit+499, len(names))
	}
	if names[0] != "f00001" || names[len(names)-1] != fmt.Sprintf("f%05d", defaultListLimit+499) {
		t.Fatalf("unexpected first/last entries %q, %q", names[0], names[len(names)-1])
	}
}

func TestListAllPagesContinuesPastFullyHiddenPage(t *testing.T) {
	const tail = 10
	fs := newListTestServer(t, &fakeListFiler{names: fileNames(defaultListLimit + tail)})
	// The whole first page is hidden from the user; only the tail is listable.
	for i := 0; i < defaultListLimit; i++ {
		fs.user.Permissions[fmt.Sprintf("/d/f%05d", i)] = []string{}
	}
	names, err := listNames(t, fs)
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != tail {
		t.Fatalf("expected the %d entries after the hidden page, got %d", tail, len(names))
	}
	if names[0] != fmt.Sprintf("f%05d", defaultListLimit) {
		t.Fatalf("unexpected first entry %q", names[0])
	}
}
