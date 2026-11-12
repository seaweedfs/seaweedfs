package command

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/replication/sink/localsink"
	"github.com/seaweedfs/seaweedfs/weed/replication/source"
)

type liveEntriesFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	entries map[string]*filer_pb.Entry
}

func (s *liveEntriesFiler) LookupDirectoryEntry(ctx context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	if entry, ok := s.entries[req.Directory+"/"+req.Name]; ok {
		return &filer_pb.LookupDirectoryEntryResponse{Entry: entry}, nil
	}
	return nil, status.Error(codes.NotFound, filer_pb.ErrNotFound.Error())
}

func TestGenProcessFunctionLocalSinkKeepsPathTheSourceRecreated(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	filer_pb.RegisterSeaweedFilerServer(server, &liveEntriesFiler{entries: map[string]*filer_pb.Entry{
		"/src/dir/recreated": {Name: "recreated", CommitTsNs: 300, Attributes: &filer_pb.FuseAttributes{}},
		"/src/dir/renamed":   {Name: "renamed", CommitTsNs: 300, Attributes: &filer_pb.FuseAttributes{}},
	}})
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	filerSource := &source.FilerSource{}
	if err := filerSource.DoInitialize(listener.Addr().String(), listener.Addr().String(), "/src", false); err != nil {
		t.Fatal(err)
	}
	filerSource.SetGrpcDialOption(grpc.WithTransportCredentials(insecure.NewCredentials()))

	target := filepath.Join(t.TempDir(), "backup")
	dataSink := &localsink.LocalSink{Dir: target}
	dataSink.SetSourceFiler(filerSource)
	if err := os.MkdirAll(filepath.Join(target, "dir"), 0755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"recreated", "renamed", "deleted"} {
		if err := os.WriteFile(filepath.Join(target, "dir", name), []byte("newer"), 0644); err != nil {
			t.Fatal(err)
		}
	}
	processFn := genProcessFunction("/src", target, nil, nil, nil, nil, dataSink, true, false)
	stale := func(name string) *filer_pb.Entry {
		return &filer_pb.Entry{Name: name, CommitTsNs: 100, Attributes: &filer_pb.FuseAttributes{FileMode: 0644}}
	}

	events := []*filer_pb.EventNotification{
		{OldEntry: stale("recreated")},
		{OldEntry: stale("deleted")},
		{OldEntry: stale("renamed"), NewParentPath: "/src/dir", NewEntry: &filer_pb.Entry{
			Name: "moved", Content: []byte("moved"), Attributes: &filer_pb.FuseAttributes{FileMode: 0644, FileSize: 5},
		}},
	}
	for _, event := range events {
		if err := processFn(&filer_pb.SubscribeMetadataResponse{Directory: "/src/dir", EventNotification: event}); err != nil {
			t.Fatalf("process %s: %v", event.OldEntry.Name, err)
		}
	}

	for _, name := range []string{"recreated", "renamed", "moved"} {
		if _, err := os.Stat(filepath.Join(target, "dir", name)); err != nil {
			t.Errorf("%s missing: %v", name, err)
		}
	}
	if _, err := os.Stat(filepath.Join(target, "dir", "deleted")); !os.IsNotExist(err) {
		t.Errorf("deleted path kept: %v", err)
	}
}
