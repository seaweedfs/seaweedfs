package localsink

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/replication/source"
)

type liveSourceFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	live    *filer_pb.Entry
	err     error
	lookups atomic.Int32
	asked   atomic.Value
}

func (s *liveSourceFiler) LookupDirectoryEntry(ctx context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	s.lookups.Add(1)
	s.asked.Store(req.Directory + "/" + req.Name)
	if s.err != nil {
		return nil, s.err
	}
	if s.live == nil {
		return nil, status.Error(codes.NotFound, filer_pb.ErrNotFound.Error())
	}
	return &filer_pb.LookupDirectoryEntryResponse{Entry: s.live}, nil
}

func startLiveSourceFiler(t *testing.T, impl *liveSourceFiler) *source.FilerSource {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := grpc.NewServer()
	filer_pb.RegisterSeaweedFilerServer(server, impl)
	go server.Serve(listener)
	t.Cleanup(server.Stop)

	address := listener.Addr().String()
	filerSource := &source.FilerSource{}
	if err := filerSource.DoInitialize(address, address, "/src", false); err != nil {
		t.Fatalf("DoInitialize: %v", err)
	}
	filerSource.SetGrpcDialOption(grpc.WithTransportCredentials(insecure.NewCredentials()))
	return filerSource
}

func stampedEntry(name string, commitTsNs, mtimeSec int64, isDirectory bool) *filer_pb.Entry {
	return &filer_pb.Entry{
		Name:        name,
		IsDirectory: isDirectory,
		CommitTsNs:  commitTsNs,
		Attributes:  &filer_pb.FuseAttributes{Mtime: mtimeSec, FileMode: 0644},
	}
}

func TestDeleteEntryVersionAgainstLiveSource(t *testing.T) {
	deleted := stampedEntry("x", 100, 10, false)
	tests := []struct {
		name     string
		live     *filer_pb.Entry
		lookup   error
		oldEntry *filer_pb.Entry
		tsNs     int64
		wantKept bool
		wantErr  bool
	}{
		{name: "source recreated the path", live: stampedEntry("x", 300, 5, false), oldEntry: deleted, tsNs: 200, wantKept: true},
		{name: "source deleted the path", oldEntry: deleted, tsNs: 200},
		{name: "source store lags the delete", live: stampedEntry("x", 100, 10, false), oldEntry: deleted, tsNs: 200},
		{name: "source holds a version written before the delete", live: stampedEntry("x", 200, 20, false), oldEntry: deleted, tsNs: 300},
		{name: "source recreated a directory", live: stampedEntry("x", 300, 30, true), oldEntry: deleted, tsNs: 200},
		{name: "stamped replacement of an unstamped entry", live: stampedEntry("x", 300, 5, false), oldEntry: stampedEntry("x", 0, 10, false), tsNs: 200, wantKept: true},
		{name: "no delete time, newer than the deleted stamp", live: stampedEntry("x", 300, 5, false), oldEntry: deleted, wantKept: true},
		{name: "no delete time, the deleted version", live: stampedEntry("x", 100, 10, false), oldEntry: deleted},
		{name: "no delete time, stamped replacement of an unstamped entry", live: stampedEntry("x", 300, 5, false), oldEntry: stampedEntry("x", 0, 10, false), wantKept: true},
		{name: "no delete time, unstamped entry with an older mtime", live: stampedEntry("x", 0, 5, false), oldEntry: stampedEntry("x", 0, 10, false)},
		{name: "unstamped source, replacement written after the delete", live: stampedEntry("x", 0, 30, false), oldEntry: stampedEntry("x", 0, 10, false), tsNs: 20e9, wantKept: true},
		{name: "unstamped source, entry written before the delete", live: stampedEntry("x", 0, 15, false), oldEntry: stampedEntry("x", 0, 10, false), tsNs: 20e9},
		{name: "nothing stamped, newer mtime", live: stampedEntry("x", 0, 20, false), oldEntry: stampedEntry("x", 0, 10, false), wantKept: true},
		{name: "nothing stamped, same mtime", live: stampedEntry("x", 0, 10, false), oldEntry: stampedEntry("x", 0, 10, false)},
		{name: "source lookup fails", lookup: status.Error(codes.Unavailable, "down"), oldEntry: deleted, tsNs: 200, wantKept: true, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			sourceFiler := &liveSourceFiler{live: tt.live, err: tt.lookup}
			localSink := &LocalSink{}
			localSink.initialize(dir, false)
			localSink.SetSourceFiler(startLiveSourceFiler(t, sourceFiler))

			key := filepath.Join(dir, "sub", "x")
			if err := os.MkdirAll(filepath.Dir(key), 0755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(key, []byte("replacement"), 0644); err != nil {
				t.Fatal(err)
			}

			err := localSink.DeleteEntryVersion(key, tt.oldEntry, tt.tsNs, true, nil)
			if (err != nil) != tt.wantErr {
				t.Fatalf("DeleteEntryVersion err = %v, want error %v", err, tt.wantErr)
			}
			if got := sourceFiler.asked.Load(); got != "/src/sub/x" {
				t.Fatalf("looked up %v, want /src/sub/x", got)
			}
			data, readErr := os.ReadFile(key)
			if tt.wantKept {
				if readErr != nil || string(data) != "replacement" {
					t.Fatalf("destination lost: %q %v", data, readErr)
				}
			} else if !os.IsNotExist(readErr) {
				t.Fatalf("destination kept after an ordered delete: %q %v", data, readErr)
			}
		})
	}
}

func TestDeleteEntryVersionSkipsLookupWhenDestinationIsGone(t *testing.T) {
	dir := t.TempDir()
	sourceFiler := &liveSourceFiler{err: status.Error(codes.Unavailable, "down")}
	localSink := &LocalSink{}
	localSink.initialize(dir, false)
	localSink.SetSourceFiler(startLiveSourceFiler(t, sourceFiler))

	if err := localSink.DeleteEntryVersion(filepath.Join(dir, "x"), stampedEntry("x", 100, 10, false), 200, true, nil); err != nil {
		t.Fatalf("duplicate delete: %v", err)
	}
	if n := sourceFiler.lookups.Load(); n != 0 {
		t.Fatalf("duplicate delete looked up the source %d times", n)
	}
}
