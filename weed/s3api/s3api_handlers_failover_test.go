package s3api

import (
	"context"
	"io"
	"math"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// fakeListFiler serves ListEntries from names; the first failCalls calls die
// with Unavailable after streaming sendBeforeFail entries.
type fakeListFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	names          []string
	failCalls      int32
	sendBeforeFail int
	calls          int32
}

func (f *fakeListFiler) ListEntries(req *filer_pb.ListEntriesRequest, stream filer_pb.SeaweedFiler_ListEntriesServer) error {
	names := f.names
	failing := atomic.AddInt32(&f.calls, 1) <= f.failCalls
	if failing {
		names = names[:f.sendBeforeFail]
	}
	for _, name := range names {
		if err := stream.Send(&filer_pb.ListEntriesResponse{Entry: &filer_pb.Entry{Name: name}}); err != nil {
			return err
		}
	}
	if failing {
		return status.Error(codes.Unavailable, "filer restarting")
	}
	return nil
}

func newFailoverTestServer(t *testing.T, filers ...pb.ServerAddress) *S3ApiServer {
	t.Helper()
	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	return &S3ApiServer{
		option:      &S3ApiServerOption{Filers: filers, GrpcDialOption: dialOption},
		filerClient: wdclient.NewFilerClient(filers, dialOption, ""),
	}
}

// accumulateListing is the callback shape the failover contract has to protect:
// entries collect into a variable that survives a replay of the callback.
func accumulateListing(got *[]string) func(filer_pb.SeaweedFilerClient) error {
	return func(client filer_pb.SeaweedFilerClient) error {
		stream, err := client.ListEntries(context.Background(), &filer_pb.ListEntriesRequest{Directory: "/d"})
		if err != nil {
			return err
		}
		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
			*got = append(*got, resp.Entry.Name)
		}
	}
}

// A filer that dies mid-stream must surface the error, not fail over: the
// callback has already consumed part of the response, and replaying it against
// the next filer would append a second copy of what it accumulated.
func TestFailoverStopsAfterPartialResponse(t *testing.T) {
	names := []string{"e1", "e2", "e3", "e4"}
	flaky := &fakeListFiler{names: names, failCalls: math.MaxInt32, sendBeforeFail: 2}
	healthy := &fakeListFiler{names: names}
	s3a := newFailoverTestServer(t, startFakeFiler(t, flaky), startFakeFiler(t, healthy))

	var got []string
	err := s3a.WithFilerClient(true, accumulateListing(&got))
	if err == nil {
		t.Fatalf("want the mid-stream failure surfaced, got success with %v", got)
	}
	if calls := atomic.LoadInt32(&healthy.calls); calls != 0 {
		t.Fatalf("callback was replayed on the second filer %d time(s)", calls)
	}
}

// A filer that fails before delivering anything is still failed over, so the
// partial-response guard does not cost the healthy-peer retry that failover exists for.
func TestFailoverBeforeFirstResponse(t *testing.T) {
	names := []string{"e1", "e2", "e3", "e4"}
	flaky := &fakeListFiler{names: names, failCalls: math.MaxInt32, sendBeforeFail: 0}
	healthy := &fakeListFiler{names: names}
	s3a := newFailoverTestServer(t, startFakeFiler(t, flaky), startFakeFiler(t, healthy))

	var got []string
	err := s3a.WithFilerClient(true, accumulateListing(&got))
	if err != nil {
		t.Fatalf("want failover success, got %v", err)
	}
	if !reflect.DeepEqual(got, names) {
		t.Fatalf("entries = %v, want %v", got, names)
	}
}

// End to end through the real listing path: a mid-stream failure surfaces to
// listWithRetry, whose replay starts from a fresh accumulator, so the caller
// sees each entry exactly once instead of a silently duplicated prefix.
func TestListAfterMidStreamFailureHasNoDuplicates(t *testing.T) {
	names := []string{"e1", "e2", "e3", "e4"}
	flaky := &fakeListFiler{names: names, failCalls: 1, sendBeforeFail: 2}
	healthy := &fakeListFiler{names: names}
	s3a := newFailoverTestServer(t, startFakeFiler(t, flaky), startFakeFiler(t, healthy))

	entries, isLast, err := s3a.list("/d", "", "", false, 10)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	var got []string
	for _, entry := range entries {
		got = append(got, entry.Name)
	}
	if !reflect.DeepEqual(got, names) {
		t.Fatalf("entries = %v, want %v", got, names)
	}
	if !isLast {
		t.Fatal("want isLast")
	}
}

// hangingCollectionFiler answers CollectionList only when the caller gives up,
// standing in for a filer waiting on a master that has stopped answering.
type hangingCollectionFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	calls atomic.Int32
}

func (f *hangingCollectionFiler) CollectionList(ctx context.Context, _ *filer_pb.CollectionListRequest) (*filer_pb.CollectionListResponse, error) {
	f.calls.Add(1)
	<-ctx.Done()
	return nil, ctx.Err()
}

// The caller's own budget expiring says nothing about the filer that was
// answering it. Counted as a filer failure it would, after the three that open
// the circuit, mark every filer in the walk unhealthy -- and LookupVolumeIds
// skips unhealthy filers, so unrelated object reads would start failing over a
// slow master they never touched.
func TestFailoverDoesNotBlameFilersForTheCallersExpiredBudget(t *testing.T) {
	first := &hangingCollectionFiler{}
	second := &hangingCollectionFiler{}
	firstAddr := startFakeFiler(t, first)
	secondAddr := startFakeFiler(t, second)
	s3a := newFailoverTestServer(t, firstAddr, secondAddr)

	// More attempts than the three failures it takes to open the circuit.
	for attempt := 0; attempt < 5; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		err := s3a.WithFilerClientCtx(ctx, false, func(client filer_pb.SeaweedFilerClient) error {
			_, listErr := client.CollectionList(ctx, &filer_pb.CollectionListRequest{})
			return listErr
		})
		cancel()
		if err == nil {
			t.Fatalf("attempt %d: want the expired budget surfaced as an error", attempt)
		}
	}

	for _, addr := range []pb.ServerAddress{firstAddr, secondAddr} {
		if s3a.filerClient.ShouldSkipUnhealthyFiler(addr) {
			t.Errorf("filer %s was flagged unhealthy for the caller's own timeout; unrelated reads would now skip it", addr)
		}
	}

	// And the walk stops rather than spending an already-spent budget on the
	// next filer, which cannot answer any faster.
	if n := second.calls.Load(); n != 0 {
		t.Errorf("the second filer was tried %d times with no budget left to answer in", n)
	}
}
