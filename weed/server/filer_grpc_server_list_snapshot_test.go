package weed_server

import (
	"context"
	"strconv"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

type captureListStream struct {
	grpc.ServerStream
	responses []*filer_pb.ListEntriesResponse
	trailer   metadata.MD
}

func (s *captureListStream) Context() context.Context { return context.Background() }

func (s *captureListStream) Send(resp *filer_pb.ListEntriesResponse) error {
	s.responses = append(s.responses, resp)
	return nil
}

func (s *captureListStream) SetTrailer(md metadata.MD) { s.trailer = md }

func (s *captureListStream) snapshotTsNs(t *testing.T) int64 {
	t.Helper()
	values := s.trailer.Get(filer_pb.ListSnapshotTsNsTrailerKey)
	if len(values) != 1 {
		t.Fatalf("snapshot trailer = %v", values)
	}
	tsNs, err := strconv.ParseInt(values[0], 10, 64)
	if err != nil {
		t.Fatalf("snapshot trailer %q: %v", values[0], err)
	}
	return tsNs
}

// A listing snapshot tells the client every event at or below it is in the
// listing, so the client drops those events. It must stay below a create that
// is stamped but may not be in the store yet, and below every stamp issued
// after it, or a create landing right after the listing never reaches the
// client's cache.
func TestListEntriesSnapshotBelowUnfinishedAndLaterStamps(t *testing.T) {
	store := newRenameTestStore()
	store.entries["/dir"] = newDirectoryEntry("/dir", 100)
	f := newRenameTestFiler(t, store)
	fs := &FilerServer{filer: f, option: &FilerOption{DirListingLimit: 1000}}

	inFlightTsNs := f.StampMetaLogInflightForTest()
	stream := &captureListStream{}
	if err := fs.ListEntries(&filer_pb.ListEntriesRequest{Directory: "/dir"}, stream); err != nil {
		t.Fatalf("ListEntries: %v", err)
	}
	f.DoneMetaLogInflightForTest(inFlightTsNs)
	snapshotTsNs := stream.snapshotTsNs(t)
	if snapshotTsNs >= inFlightTsNs {
		t.Fatalf("snapshot %d covers the unfinished stamp %d", snapshotTsNs, inFlightTsNs)
	}

	stream = &captureListStream{}
	if err := fs.ListEntries(&filer_pb.ListEntriesRequest{Directory: "/dir"}, stream); err != nil {
		t.Fatalf("ListEntries: %v", err)
	}
	snapshotTsNs = stream.snapshotTsNs(t)
	laterTsNs := f.StampMetaLogInflightForTest()
	f.DoneMetaLogInflightForTest(laterTsNs)
	if laterTsNs <= snapshotTsNs {
		t.Fatalf("stamp %d issued after the listing is not above its snapshot %d", laterTsNs, snapshotTsNs)
	}
}
