package weed_server

import (
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// A hidden filer.conf entry must be removed before ListEntries consumes its
// page limit. Otherwise an interceptor-level filter can leave a limit=1 page
// empty when filer.conf sorts before the first visible entry.
func TestScopeListEntriesHidesFilerConfBeforePagination(t *testing.T) {
	store := newRenameTestStore()
	if err := store.InsertEntry(t.Context(), newFileEntry(scopeFilerConfDir+"/"+scopeFilerConfName, 1)); err != nil {
		t.Fatalf("seed filer.conf: %v", err)
	}
	if err := store.InsertEntry(t.Context(), newFileEntry(scopeFilerConfDir+"/visible.json", 2)); err != nil {
		t.Fatalf("seed visible entry: %v", err)
	}
	f := newRenameTestFiler(t, store)
	fs := &FilerServer{filer: f, option: &FilerOption{DirListingLimit: 1}}
	fs.grpcScope = newFilerGrpcScope(scopeConfig{
		signingKey: func() security.SigningKey { return security.SigningKey(scopeTestKey) },
	}, fs)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := grpc.NewServer(fs.grpcScope.ServerOptions()...)
	filer_pb.RegisterSeaweedFilerServer(server, fs)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})
	conn, err := grpc.NewClient("passthrough:///"+listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithPerRPCCredentials(scopeToken(scopedToken("alice", time.Hour, []string{scopeFilerConfDir}, nil))))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	stream, err := filer_pb.NewSeaweedFilerClient(conn).ListEntries(scopeCtx(t), &filer_pb.ListEntriesRequest{Directory: scopeFilerConfDir, Limit: 1})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	response, err := stream.Recv()
	if err != nil {
		t.Fatalf("first visible entry: %v", err)
	}
	if got := response.GetEntry().GetName(); got != "visible.json" {
		t.Fatalf("first visible entry = %q, want visible.json", got)
	}
	if response.GetSnapshotTsNs() == 0 {
		t.Fatal("first visible entry did not carry the listing snapshot timestamp")
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("second list response = %v, want EOF after limit=1", err)
	}
}
