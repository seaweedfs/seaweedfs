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
		"/src/dir/recreated":  {Name: "recreated", CommitTsNs: 300, Attributes: &filer_pb.FuseAttributes{}},
		"/src/dir/renamed":    {Name: "renamed", CommitTsNs: 300, Attributes: &filer_pb.FuseAttributes{}},
		"/src/dir/peer":       {Name: "peer", CommitTsNs: 350, Attributes: &filer_pb.FuseAttributes{}},
		"/src/dir/concurrent": {Name: "concurrent", CommitTsNs: 200, Attributes: &filer_pb.FuseAttributes{}},
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
	for _, name := range []string{"recreated", "renamed", "deleted", "peer", "concurrent"} {
		if err := os.WriteFile(filepath.Join(target, "dir", name), []byte("newer"), 0644); err != nil {
			t.Fatal(err)
		}
	}
	processFn := genProcessFunction("/src", target, nil, nil, nil, nil, dataSink, true, false)
	stale := func(name string) *filer_pb.Entry {
		return &filer_pb.Entry{Name: name, CommitTsNs: 100, Attributes: &filer_pb.FuseAttributes{FileMode: 0644}}
	}

	events := []*filer_pb.SubscribeMetadataResponse{
		{TsNs: 200, EventNotification: &filer_pb.EventNotification{OldEntry: stale("recreated")}},
		{TsNs: 200, EventNotification: &filer_pb.EventNotification{OldEntry: stale("deleted")}},
		{TsNs: 200, EventNotification: &filer_pb.EventNotification{OldEntry: stale("renamed"), NewParentPath: "/src/dir", NewEntry: &filer_pb.Entry{
			Name: "moved", Content: []byte("moved"), Attributes: &filer_pb.FuseAttributes{FileMode: 0644, FileSize: 5},
		}}},
		{TsNs: 400, MutationTsNs: 300, EventNotification: &filer_pb.EventNotification{OldEntry: stale("peer")}},
		{TsNs: 300, EventNotification: &filer_pb.EventNotification{OldEntry: stale("concurrent")}},
	}
	for _, event := range events {
		event.Directory = "/src/dir"
		if err := processFn(event); err != nil {
			t.Fatalf("process %s: %v", event.EventNotification.OldEntry.Name, err)
		}
	}

	for _, name := range []string{"recreated", "renamed", "moved", "peer"} {
		if _, err := os.Stat(filepath.Join(target, "dir", name)); err != nil {
			t.Errorf("%s missing: %v", name, err)
		}
	}
	for _, name := range []string{"deleted", "concurrent"} {
		if _, err := os.Stat(filepath.Join(target, "dir", name)); !os.IsNotExist(err) {
			t.Errorf("%s kept: %v", name, err)
		}
	}
}
