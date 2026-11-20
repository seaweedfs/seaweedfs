package s3api

import (
	"context"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// childProbeFiler answers ListEntries from fixed directory contents, or with
// listErr, and counts the listings it served.
type childProbeFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer

	mu       sync.Mutex
	children map[string][]string
	listErr  error
	lists    int
}

func (f *childProbeFiler) ListEntries(req *filer_pb.ListEntriesRequest, stream filer_pb.SeaweedFiler_ListEntriesServer) error {
	f.mu.Lock()
	f.lists++
	listErr := f.listErr
	names := append([]string(nil), f.children[req.Directory]...)
	f.mu.Unlock()
	if listErr != nil {
		return listErr
	}
	for _, name := range names {
		if err := stream.Send(&filer_pb.ListEntriesResponse{Entry: &filer_pb.Entry{Name: name}}); err != nil {
			return err
		}
	}
	return nil
}

func (f *childProbeFiler) setListErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listErr = err
}

func (f *childProbeFiler) listCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lists
}

// HEAD of a 0-byte file answers 404 only when the filer confirms children. A
// probe that cannot run or fails must not turn an existing file into a 404,
// while the listing keeps treating a failed probe as "has children".
func TestZeroByteFileHasChildrenServesTheFileWhenTheProbeFails(t *testing.T) {
	filer := &childProbeFiler{children: map[string][]string{
		"/buckets/b/dataset": {"part-0.parquet"},
	}}
	s3a := newPutTestServer(t, startFakeFiler(t, filer))
	ctx := context.Background()

	if !s3a.zeroByteFileHasChildren(ctx, "b", "dataset") {
		t.Fatal("a 0-byte file with a child must answer as an implicit directory")
	}
	if s3a.zeroByteFileHasChildren(ctx, "b", "empty.lock") {
		t.Fatal("a 0-byte file without children must be served")
	}
	if !s3a.hasChildren(ctx, "b", "dataset") || s3a.hasChildren(ctx, "b", "empty.lock") {
		t.Fatal("hasChildren must report what the filer lists")
	}

	// The request context of a keep-alive connection can already be canceled
	// while the client still waits for the answer.
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	before := filer.listCount()
	if s3a.zeroByteFileHasChildren(canceled, "b", "empty.lock") {
		t.Fatal("a probe on a canceled context must not answer an existing 0-byte file with 404")
	}
	if !s3a.hasChildren(canceled, "b", "empty.lock") {
		t.Fatal("a listing must still keep an entry whose probe could not run")
	}
	if got := filer.listCount(); got != before {
		t.Fatalf("a canceled context must not reach the filer, got %d listings", got-before)
	}

	filer.setListErr(status.Error(codes.Unavailable, "filer restarting"))
	if s3a.zeroByteFileHasChildren(ctx, "b", "empty.lock") {
		t.Fatal("a failed probe must not answer an existing 0-byte file with 404")
	}
	if !s3a.hasChildren(ctx, "b", "empty.lock") {
		t.Fatal("a listing must still keep an entry whose probe failed")
	}
}
