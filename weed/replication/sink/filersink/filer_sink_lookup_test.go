package filersink

import (
	"context"
	"net"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type lookupErrFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	err error
}

func (f *lookupErrFiler) LookupDirectoryEntry(context.Context, *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	return nil, f.err
}

func TestUpdateEntryRecreatesOnlyOnNotFound(t *testing.T) {
	for _, tc := range []struct {
		name      string
		err       error
		wantFound bool
	}{
		{"not found", status.Error(codes.NotFound, filer_pb.ErrNotFound.Error()), false},
		{"canceled", status.Error(codes.Canceled, "context canceled"), true},
		{"unavailable", status.Error(codes.Unavailable, "connection refused"), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lis, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			srv := grpc.NewServer()
			filer_pb.RegisterSeaweedFilerServer(srv, &lookupErrFiler{err: tc.err})
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
		})
	}
}
