package shell

import (
	"context"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type exclusiveCreateFiler struct {
	filer_pb.SeaweedFilerClient
	resp *filer_pb.CreateEntryResponse
	req  *filer_pb.CreateEntryRequest
}

func (s *exclusiveCreateFiler) CreateEntry(_ context.Context, req *filer_pb.CreateEntryRequest, _ ...grpc.CallOption) (*filer_pb.CreateEntryResponse, error) {
	s.req = req
	return s.resp, nil
}

// meta.sync updates entries from a directory listing taken once per
// directory; a file written locally after that listing must not be reset to
// the remote pointer.
func TestDoSaveRemoteEntryIsConditionedOnTheListedEntry(t *testing.T) {
	listed := &filer_pb.Entry{
		Name:        "f",
		Attributes:  &filer_pb.FuseAttributes{FileSize: 3, Mtime: 10},
		Chunks:      []*filer_pb.FileChunk{{Fid: &filer_pb.FileId{VolumeId: 3, FileKey: 1, Cookie: 7}}},
		RemoteEntry: &filer_pb.RemoteEntry{StorageName: "r", RemoteETag: "old", RemoteSize: 3, RemoteMtime: 10},
	}
	remote := &filer_pb.RemoteEntry{StorageName: "r", RemoteETag: "new", RemoteSize: 5, RemoteMtime: 20}

	for _, tc := range []struct {
		name  string
		err   error
		stale bool
	}{
		{"unchanged", nil, false},
		{"written since listed", status.Error(codes.FailedPrecondition, "precondition failed"), true},
		{"deleted since listed", status.Error(codes.NotFound, "not found"), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := &conditionalUpdateFiler{err: tc.err}
			stale, err := doSaveRemoteEntry(fake, "/buckets/b", proto.Clone(listed).(*filer_pb.Entry), remote)
			if stale != tc.stale || (err != nil) != (tc.err != nil) {
				t.Fatalf("stale=%v err=%v, want stale=%v err=%v", stale, err, tc.stale, tc.err)
			}
			clauses := fake.req.GetCondition().GetClauses()
			if len(clauses) != 1 || clauses[0].Kind != filer_pb.WriteCondition_IF_ENTRY_EQUAL || !proto.Equal(clauses[0].ExpectedEntry, listed) {
				t.Fatalf("condition %v must expect the listed entry", fake.req.GetCondition())
			}
			written := fake.req.Entry
			if len(written.Chunks) != 0 || !proto.Equal(written.RemoteEntry, remote) || written.Attributes.FileSize != 5 {
				t.Fatalf("written entry %v must point at the new remote copy", written)
			}
		})
	}
}

// A name missing from the listing may be created locally before meta.sync
// gets to it; the remote-only entry must not replace it.
func TestCreateRemoteEntryKeepsAnEntryCreatedSinceListed(t *testing.T) {
	child := &remoteChild{remoteEntry: &filer_pb.RemoteEntry{StorageName: "r", RemoteSize: 5, RemoteMtime: 20}}

	for _, tc := range []struct {
		name   string
		resp   *filer_pb.CreateEntryResponse
		output string
		err    bool
	}{
		{"absent", &filer_pb.CreateEntryResponse{}, "/buckets/b/f (create)", false},
		{"created since listed", &filer_pb.CreateEntryResponse{Error: "exists", ErrorCode: filer_pb.FilerError_ENTRY_ALREADY_EXISTS}, "/buckets/b/f (skip, created since listed)", false},
		{"refused", &filer_pb.CreateEntryResponse{Error: "read only", ErrorCode: filer_pb.FilerError_READ_ONLY}, "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := &exclusiveCreateFiler{resp: tc.resp}
			var out strings.Builder
			err := createRemoteEntry(context.Background(), fake, &out, util.FullPath("/buckets/b"), "f", child, "r")
			if (err != nil) != tc.err {
				t.Fatalf("err=%v, want error %v", err, tc.err)
			}
			if !fake.req.OExcl {
				t.Fatalf("create of %s must be exclusive", fake.req.Entry.Name)
			}
			if got := strings.TrimSpace(out.String()); got != tc.output {
				t.Fatalf("output %q, want %q", got, tc.output)
			}
		})
	}
}
