package filer_pb

import (
	"context"
	"fmt"
	"net"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// stubFiler serves canned listings and counts how often each directory is listed.
type stubFiler struct {
	UnimplementedSeaweedFilerServer
	mu       sync.Mutex
	listed   map[string]int
	listings func(req *ListEntriesRequest, send func(*Entry) error) error
}

func (s *stubFiler) ListEntries(req *ListEntriesRequest, stream SeaweedFiler_ListEntriesServer) error {
	s.mu.Lock()
	if s.listed == nil {
		s.listed = make(map[string]int)
	}
	s.listed[req.Directory]++
	s.mu.Unlock()
	return s.listings(req, func(entry *Entry) error {
		return stream.Send(&ListEntriesResponse{Entry: entry})
	})
}

func (s *stubFiler) listedCount(dir string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listed[dir]
}

type stubFilerClient struct {
	conn *grpc.ClientConn
}

func (c *stubFilerClient) WithFilerClient(streamingMode bool, fn func(SeaweedFilerClient) error) error {
	return fn(NewSeaweedFilerClient(c.conn))
}

func (c *stubFilerClient) AdjustedUrl(location *Location) string { return location.Url }

func (c *stubFilerClient) GetDataCenter() string { return "" }

func startStubFiler(t *testing.T, s *stubFiler) *stubFilerClient {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcServer := grpc.NewServer()
	RegisterSeaweedFilerServer(grpcServer, s)
	go grpcServer.Serve(listener)
	t.Cleanup(grpcServer.Stop)
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return &stubFilerClient{conn: conn}
}

// A directory delivered twice by a misbehaving listing must still be walked once.
func TestTraverseBfsVisitsDuplicateDirectoryOnce(t *testing.T) {
	server := &stubFiler{}
	server.listings = func(req *ListEntriesRequest, send func(*Entry) error) error {
		switch req.Directory {
		case "/":
			if req.StartFromFileName != "" {
				return nil
			}
			for _, name := range []string{"dup", "dup"} {
				if err := send(&Entry{Name: name, IsDirectory: true}); err != nil {
					return err
				}
			}
		case "/dup":
			if req.StartFromFileName != "" {
				return nil
			}
			for _, name := range []string{"a.txt", "b.txt"} {
				if err := send(&Entry{Name: name}); err != nil {
					return err
				}
			}
		}
		return nil
	}
	filerClient := startStubFiler(t, server)

	var mu sync.Mutex
	seen := make(map[string]int)
	err := TraverseBfs(context.Background(), filerClient, "/", func(parentPath util.FullPath, entry *Entry) error {
		mu.Lock()
		seen[string(parentPath.Child(entry.Name))]++
		mu.Unlock()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if got := server.listedCount("/dup"); got != 1 {
		t.Fatalf("listed /dup %d times, want 1", got)
	}
	for _, path := range []string{"/dup/a.txt", "/dup/b.txt"} {
		if seen[path] != 1 {
			t.Fatalf("visited %s %d times, want 1", path, seen[path])
		}
	}
}

// A store whose pagination never advances past the cursor must error out
// instead of re-listing the same page forever.
func TestReadDirAllEntriesStuckPagination(t *testing.T) {
	server := &stubFiler{}
	server.listings = func(req *ListEntriesRequest, send func(*Entry) error) error {
		for i := uint32(0); i < req.Limit; i++ {
			if err := send(&Entry{Name: fmt.Sprintf("f%05d", i)}); err != nil {
				return err
			}
		}
		return nil
	}
	filerClient := startStubFiler(t, server)

	done := make(chan error, 1)
	go func() {
		done <- ReadDirAllEntries(context.Background(), filerClient, "/", "", func(entry *Entry, isLast bool) error {
			return nil
		})
	}()

	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "pagination stuck") {
			t.Fatalf("want pagination stuck error, got %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("listing loops forever on a non-advancing store")
	}
}

// A start path with a trailing slash must still descend into subdirectories.
func TestTraverseBfsTrailingSlashRoot(t *testing.T) {
	server := &stubFiler{}
	server.listings = func(req *ListEntriesRequest, send func(*Entry) error) error {
		if req.StartFromFileName != "" {
			return nil
		}
		// the filer trims a single trailing slash, nothing more
		switch strings.TrimSuffix(req.Directory, "/") {
		case "/buckets/data":
			return send(&Entry{Name: "sub", IsDirectory: true})
		case "/buckets/data/sub":
			return send(&Entry{Name: "a.txt"})
		}
		return nil
	}
	filerClient := startStubFiler(t, server)

	var mu sync.Mutex
	var seen []string
	err := TraverseBfs(context.Background(), filerClient, "/buckets/data/", func(parentPath util.FullPath, entry *Entry) error {
		mu.Lock()
		seen = append(seen, fmt.Sprintf("%s -> %s", parentPath, entry.Name))
		mu.Unlock()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	sort.Strings(seen)
	want := []string{"/buckets/data -> sub", "/buckets/data/sub -> a.txt"}
	if !reflect.DeepEqual(seen, want) {
		t.Fatalf("visited %v, want %v", seen, want)
	}
}
