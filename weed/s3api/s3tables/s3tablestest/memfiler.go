// Package s3tablestest provides an in-memory filer for driving S3 Tables and
// Lance namespace operations end-to-end without a live cluster.
package s3tablestest

import (
	"bytes"
	"context"
	"net"
	"path"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// MemFiler is an in-memory filer used to drive Manager operations
// end-to-end without a live cluster.
type MemFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	entries map[string]map[string]*filer_pb.Entry // dir -> name -> entry
	Client  filer_pb.SeaweedFilerClient
	// BeforeUpdate runs once, at the start of the next UpdateEntry, so a test
	// can land a competing write in a handler's read-to-write window.
	BeforeUpdate func()
}

func newMemFiler() *MemFiler {
	return &MemFiler{entries: make(map[string]map[string]*filer_pb.Entry)}
}

func (f *MemFiler) Get(dir, name string) *filer_pb.Entry {
	if d, ok := f.entries[dir]; ok {
		return d[name]
	}
	return nil
}

func (f *MemFiler) Put(dir, name string, extended map[string][]byte) {
	if _, ok := f.entries[dir]; !ok {
		f.entries[dir] = make(map[string]*filer_pb.Entry)
	}
	f.entries[dir][name] = &filer_pb.Entry{Name: name, IsDirectory: true, Extended: extended}
}

func (f *MemFiler) LookupDirectoryEntry(_ context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	if e := f.Get(req.Directory, req.Name); e != nil {
		return &filer_pb.LookupDirectoryEntryResponse{Entry: e}, nil
	}
	// Carry the sentinel text so filer_pb.LookupEntry maps it to ErrNotFound.
	return nil, status.Errorf(codes.NotFound, "%s: %s/%s", filer_pb.ErrNotFound.Error(), req.Directory, req.Name)
}

// ListEntries honours prefix, start-from and limit the way the real filer does.
// A harness that ignores them makes a paginating caller re-read the first page
// forever, which looks like duplicated entries rather than a broken listing.
func (f *MemFiler) ListEntries(req *filer_pb.ListEntriesRequest, stream grpc.ServerStreamingServer[filer_pb.ListEntriesResponse]) error {
	d, ok := f.entries[req.Directory]
	if !ok {
		return nil
	}
	names := make([]string, 0, len(d))
	for name := range d {
		names = append(names, name)
	}
	sort.Strings(names)

	sent := uint32(0)
	for _, name := range names {
		if req.Prefix != "" && !strings.HasPrefix(name, req.Prefix) {
			continue
		}
		if req.StartFromFileName != "" {
			if name < req.StartFromFileName {
				continue
			}
			if name == req.StartFromFileName && !req.InclusiveStartFrom {
				continue
			}
		}
		if err := stream.Send(&filer_pb.ListEntriesResponse{Entry: d[name]}); err != nil {
			return err
		}
		sent++
		if req.Limit > 0 && sent >= req.Limit {
			return nil
		}
	}
	return nil
}

func (f *MemFiler) CreateEntry(_ context.Context, req *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	if _, ok := f.entries[req.Directory]; !ok {
		f.entries[req.Directory] = make(map[string]*filer_pb.Entry)
	}
	f.entries[req.Directory][req.Entry.Name] = req.Entry
	return &filer_pb.CreateEntryResponse{}, nil
}

func (f *MemFiler) UpdateEntry(_ context.Context, req *filer_pb.UpdateEntryRequest) (*filer_pb.UpdateEntryResponse, error) {
	if hook := f.BeforeUpdate; hook != nil {
		f.BeforeUpdate = nil
		hook()
	}
	// The real filer validates ExpectedExtended under the per-path lock; without
	// it here a lost update would look like a success.
	for key, expected := range req.ExpectedExtended {
		var actual []byte
		if existing := f.Get(req.Directory, req.Entry.Name); existing != nil {
			actual = existing.Extended[key]
		}
		if !bytes.Equal(actual, expected) {
			return nil, status.Errorf(codes.FailedPrecondition, "extended attribute %q changed", key)
		}
	}
	if _, ok := f.entries[req.Directory]; !ok {
		f.entries[req.Directory] = make(map[string]*filer_pb.Entry)
	}
	f.entries[req.Directory][req.Entry.Name] = req.Entry
	return &filer_pb.UpdateEntryResponse{}, nil
}

func (f *MemFiler) DeleteEntry(_ context.Context, req *filer_pb.DeleteEntryRequest) (*filer_pb.DeleteEntryResponse, error) {
	if d, ok := f.entries[req.Directory]; ok {
		delete(d, req.Name)
	}
	// Honor recursive data deletion so a regression that wipes the table directory
	// also drops its metadata/ and data/ children (the data-loss this guards against).
	if req.IsRecursive && req.IsDeleteData {
		child := path.Join(req.Directory, req.Name)
		for dir := range f.entries {
			if dir == child || strings.HasPrefix(dir, child+"/") {
				delete(f.entries, dir)
			}
		}
	}
	return &filer_pb.DeleteEntryResponse{}, nil
}

// GetFilerConfiguration answers with the defaults, so operations that resolve
// the buckets directory before touching an entry work against this filer.
func (f *MemFiler) GetFilerConfiguration(_ context.Context, _ *filer_pb.GetFilerConfigurationRequest) (*filer_pb.GetFilerConfigurationResponse, error) {
	return &filer_pb.GetFilerConfigurationResponse{DirBuckets: s3_constants.DefaultBucketsPath}, nil
}

func (f *MemFiler) Ping(_ context.Context, _ *filer_pb.PingRequest) (*filer_pb.PingResponse, error) {
	now := time.Now().UnixNano()
	return &filer_pb.PingResponse{StartTimeNs: now, RemoteTimeNs: now, StopTimeNs: now}, nil
}

func Start(t *testing.T) *MemFiler {
	t.Helper()
	fs := newMemFiler()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("start filer: %v", err)
	}

	server := grpc.NewServer()
	filer_pb.RegisterSeaweedFilerServer(server, fs)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.GracefulStop)

	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("start filer: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	fs.Client = filer_pb.NewSeaweedFilerClient(conn)
	deadline := time.Now().Add(5 * time.Second)
	for {
		pingCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		_, err := fs.Client.Ping(pingCtx, &filer_pb.PingRequest{})
		cancel()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("filer not ready: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fs
}
