package filersink

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type lookupErrFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	err     error
	lookups atomic.Int32
}

func (f *lookupErrFiler) LookupDirectoryEntry(context.Context, *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	f.lookups.Add(1)
	return nil, f.err
}

func TestUpdateEntryRecreatesOnlyOnNotFound(t *testing.T) {
	defer func(wait time.Duration) { util.RetryWaitTime = wait }(util.RetryWaitTime)
	util.RetryWaitTime = 2 * time.Second
	for _, tc := range []struct {
		name        string
		err         error
		wantFound   bool
		wantLookups int32
	}{
		{"not found", status.Error(codes.NotFound, filer_pb.ErrNotFound.Error()), false, 1},
		{"canceled", status.Error(codes.Canceled, "context canceled"), true, 2},
		{"unavailable", status.Error(codes.Unavailable, "connection refused"), true, 2},
		{"unavailable with the not-found text", status.Error(codes.Unavailable, filer_pb.ErrNotFound.Error()), true, 2},
		{"plain-text not found", status.Error(codes.Unknown, filer_pb.ErrNotFound.Error()), false, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lis, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			srv := grpc.NewServer()
			filer := &lookupErrFiler{err: tc.err}
			filer_pb.RegisterSeaweedFilerServer(srv, filer)
			go srv.Serve(lis)
			defer srv.Stop()

			fs := &FilerSink{grpcAddress: lis.Addr().String(), grpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials())}
			dir := &filer_pb.Entry{Name: "object.versions", IsDirectory: true}
			found, err := fs.UpdateEntry("/buckets/b/object.versions", dir, "/buckets/b", dir, true, nil)
			if err == nil {
				t.Fatal("expected the lookup error")
			}
			if found != tc.wantFound {
				t.Fatalf("foundExistingEntry = %v, want %v (err %v)", found, tc.wantFound, err)
			}
			if got := filer.lookups.Load(); got != tc.wantLookups {
				t.Fatalf("lookups = %d, want %d", got, tc.wantLookups)
			}
		})
	}
}
