package shell

import (
	"context"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
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
			taken, err := createRemoteEntry(context.Background(), fake, &out, util.FullPath("/buckets/b"), "f", child, "r")
			if (err != nil) != tc.err || taken != (tc.resp.ErrorCode == filer_pb.FilerError_ENTRY_ALREADY_EXISTS) {
				t.Fatalf("taken=%v err=%v, want error %v", taken, err, tc.err)
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

type takenNameFiler struct {
	stubFilerSvc
	creates []string
}

func (s *takenNameFiler) CreateEntry(_ context.Context, req *filer_pb.CreateEntryRequest, _ ...grpc.CallOption) (*filer_pb.CreateEntryResponse, error) {
	s.creates = append(s.creates, string(util.FullPath(req.Directory).Child(req.Entry.Name)))
	return &filer_pb.CreateEntryResponse{Error: "exists", ErrorCode: filer_pb.FilerError_ENTRY_ALREADY_EXISTS}, nil
}

type listingRemote struct {
	remote_storage.RemoteStorageClient
	dirs map[string]map[string]bool
}

func (r *listingRemote) ListDirectory(_ context.Context, loc *remote_pb.RemoteStorageLocation, visitFn remote_storage.VisitFunc) error {
	for name, isDirectory := range r.dirs[loc.Path] {
		if err := visitFn(loc.Path, name, isDirectory, &filer_pb.RemoteEntry{StorageName: "r", RemoteSize: 1, RemoteMtime: 1}); err != nil {
			return err
		}
	}
	return nil
}

// A remote directory whose name is taken locally after the listing is only
// descended into when the name now holds a directory: creating remote
// children under a local file would turn that file into a directory.
func TestPullMetadataDirectoryDescendsOnlyIntoATakenDirectory(t *testing.T) {
	remote := &listingRemote{dirs: map[string]map[string]bool{
		"/":     {"docs": true},
		"/docs": {"a.txt": false},
	}}
	for _, tc := range []struct {
		name    string
		isDir   bool
		creates []string
	}{
		{"taken by a file", false, []string{"/buckets/b/docs"}},
		{"taken by a directory", true, []string{"/buckets/b/docs", "/buckets/b/docs/a.txt"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := &takenNameFiler{stubFilerSvc: stubFilerSvc{lookupResp: &filer_pb.LookupDirectoryEntryResponse{
				Entry: &filer_pb.Entry{Name: "docs", IsDirectory: tc.isDir},
			}}}
			var out strings.Builder
			loc := &remote_pb.RemoteStorageLocation{Name: "r", Bucket: "rb", Path: "/"}
			if err := pullMetadataDirectory(context.Background(), fake, &out, remote, util.FullPath("/buckets/b"), loc, false, false); err != nil {
				t.Fatalf("sync: %v", err)
			}
			if strings.Join(fake.creates, ",") != strings.Join(tc.creates, ",") {
				t.Fatalf("creates %v, want %v", fake.creates, tc.creates)
			}
		})
	}
}

type deleteRecordingFiler struct {
	takenNameFiler
	deletes []*filer_pb.DeleteEntryRequest
	listing map[string][]*filer_pb.Entry
}

func (s *deleteRecordingFiler) ListEntries(_ context.Context, req *filer_pb.ListEntriesRequest, _ ...grpc.CallOption) (filer_pb.SeaweedFiler_ListEntriesClient, error) {
	return &stubListStream{entries: s.listing[req.Directory]}, nil
}

func (s *deleteRecordingFiler) DeleteEntry(_ context.Context, req *filer_pb.DeleteEntryRequest, _ ...grpc.CallOption) (*filer_pb.DeleteEntryResponse, error) {
	s.deletes = append(s.deletes, req)
	return &filer_pb.DeleteEntryResponse{}, nil
}

// A remote-backed entry that meta.sync drops may be replaced locally after the
// directory listing; the delete must only remove the listed version.
func TestPullMetadataDirectoryDeletesOnlyTheListedFile(t *testing.T) {
	remoteFile := &filer_pb.RemoteEntry{StorageName: "r", RemoteSize: 1, RemoteMtime: 10}
	for _, tc := range []struct {
		name        string
		listed      *filer_pb.Entry
		remote      map[string]bool
		deleteStale bool
		sec, ns     int64
	}{
		{"remote turned it into a directory", &filer_pb.Entry{Name: "f", Attributes: &filer_pb.FuseAttributes{Mtime: 10, MtimeNs: 5}, RemoteEntry: remoteFile}, map[string]bool{"f": true}, false, 10, 10e9 + 5},
		{"gone from the remote", &filer_pb.Entry{Name: "f", Attributes: &filer_pb.FuseAttributes{Mtime: 10, MtimeNs: 5}, RemoteEntry: remoteFile}, map[string]bool{}, true, 10, 10e9 + 5},
		{"listed without an mtime", &filer_pb.Entry{Name: "f", Attributes: &filer_pb.FuseAttributes{}, RemoteEntry: remoteFile}, map[string]bool{}, true, 0, 1},
		{"listed at the epoch with nanoseconds", &filer_pb.Entry{Name: "f", Attributes: &filer_pb.FuseAttributes{MtimeNs: 500}, RemoteEntry: remoteFile}, map[string]bool{}, true, 0, 500},
		{"empty directory gone from the remote", &filer_pb.Entry{Name: "f", IsDirectory: true, Attributes: &filer_pb.FuseAttributes{Mtime: 20}, RemoteEntry: &filer_pb.RemoteEntry{StorageName: "r"}}, map[string]bool{}, true, 20, 20e9},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := &deleteRecordingFiler{
				takenNameFiler: takenNameFiler{stubFilerSvc: stubFilerSvc{
					lookupResp: &filer_pb.LookupDirectoryEntryResponse{Entry: &filer_pb.Entry{Name: "f"}},
				}},
				listing: map[string][]*filer_pb.Entry{"/buckets/b": {tc.listed}},
			}
			remote := &listingRemote{dirs: map[string]map[string]bool{"/": tc.remote}}
			var out strings.Builder
			loc := &remote_pb.RemoteStorageLocation{Name: "r", Bucket: "rb", Path: "/"}
			if err := pullMetadataDirectory(context.Background(), fake, &out, remote, util.FullPath("/buckets/b"), loc, tc.deleteStale, false); err != nil {
				t.Fatalf("sync: %v", err)
			}
			if len(fake.deletes) != 1 || fake.deletes[0].Name != "f" || fake.deletes[0].IfNotModifiedAfter != tc.sec || fake.deletes[0].IfNotModifiedAfterNs != tc.ns {
				t.Fatalf("deletes %v must be conditioned on the listed mtime", fake.deletes)
			}
		})
	}
}

// The filer answers a delete it skipped under the cutoff with success, so the
// sync reports what a lookup after the delete finds.
func TestPullMetadataDirectoryReportsAKeptEntry(t *testing.T) {
	listed := &filer_pb.Entry{Name: "f", Attributes: &filer_pb.FuseAttributes{Mtime: 10}, RemoteEntry: &filer_pb.RemoteEntry{StorageName: "r", RemoteSize: 1, RemoteMtime: 10}}
	for _, tc := range []struct {
		name        string
		remote      map[string]bool
		deleteStale bool
		kept        bool
		want        string
		creates     int
	}{
		{"stale file kept", map[string]bool{}, true, true, "/buckets/b/f (skip, changed since listed)\n", 0},
		{"stale file deleted", map[string]bool{}, true, false, "/buckets/b/f (delete)\n", 0},
		{"file replaced by a remote directory kept", map[string]bool{"f": true}, false, true, "/buckets/b/f (skip, changed since listed)\n", 0},
		{"file replaced by a remote directory deleted", map[string]bool{"f": true}, false, false, "/buckets/b/f (delete)\n/buckets/b/f (skip, created since listed)\n", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stub := stubFilerSvc{lookupErr: filer_pb.ErrNotFound}
			if tc.kept {
				stub = stubFilerSvc{lookupResp: &filer_pb.LookupDirectoryEntryResponse{Entry: listed}}
			}
			fake := &deleteRecordingFiler{
				takenNameFiler: takenNameFiler{stubFilerSvc: stub},
				listing:        map[string][]*filer_pb.Entry{"/buckets/b": {listed}},
			}
			remote := &listingRemote{dirs: map[string]map[string]bool{"/": tc.remote}}
			var out strings.Builder
			loc := &remote_pb.RemoteStorageLocation{Name: "r", Bucket: "rb", Path: "/"}
			if err := pullMetadataDirectory(context.Background(), fake, &out, remote, util.FullPath("/buckets/b"), loc, tc.deleteStale, false); err != nil {
				t.Fatalf("sync: %v", err)
			}
			if out.String() != tc.want || len(fake.creates) != tc.creates {
				t.Fatalf("output %q with %d creates, want %q with %d", out.String(), len(fake.creates), tc.want, tc.creates)
			}
		})
	}
}
