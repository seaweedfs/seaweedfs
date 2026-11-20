package s3api

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// slowLookupFiler answers LookupDirectoryEntry and ListEntries with an entry
// named after itself, after delay. A delay of -1 never answers, standing in for
// an owner behind a partition that drops packets: the caller's deadline is the
// only way out.
type slowLookupFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	name  string
	delay time.Duration
	calls atomic.Int32
}

func (f *slowLookupFiler) wait(ctx context.Context) error {
	f.calls.Add(1)
	if f.delay < 0 {
		<-ctx.Done()
		return ctx.Err()
	}
	select {
	case <-time.After(f.delay):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (f *slowLookupFiler) LookupDirectoryEntry(ctx context.Context, _ *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	if err := f.wait(ctx); err != nil {
		return nil, err
	}
	return &filer_pb.LookupDirectoryEntryResponse{Entry: &filer_pb.Entry{Name: f.name}}, nil
}

func (f *slowLookupFiler) ListEntries(_ *filer_pb.ListEntriesRequest, stream filer_pb.SeaweedFiler_ListEntriesServer) error {
	if err := f.wait(stream.Context()); err != nil {
		return err
	}
	return stream.Send(&filer_pb.ListEntriesResponse{Entry: &filer_pb.Entry{Name: f.name}})
}

// newRoutedReadTestServer returns a gateway whose filer list is filers (the
// first is the current filer) and whose ring makes owner every key's owner.
func newRoutedReadTestServer(t *testing.T, owner pb.ServerAddress, routedLookupTimeout time.Duration, filers ...pb.ServerAddress) *S3ApiServer {
	t.Helper()
	s3a := newFailoverTestServer(t, filers...)
	s3a.option.RoutedLookupTimeout = routedLookupTimeout
	s3a.objectWriteLockClient = cluster.NewLockClient(s3a.option.GrpcDialOption, owner)
	s3a.objectWriteLockClient.SetRing([]pb.ServerAddress{owner}, 1)
	return s3a
}

// An owner that never answers costs a read the routed-lookup deadline, not
// gRPC's connect timeout: the walk moves on to the current filer, which reads
// the same entry, and flags the owner so the next read does not dial it at all.
func TestRoutedLookupFailsOverAtDeadline(t *testing.T) {
	owner := &slowLookupFiler{name: "owner", delay: -1}
	local := &slowLookupFiler{name: "local"}
	ownerAddr := startFakeFiler(t, owner)
	s3a := newRoutedReadTestServer(t, ownerAddr, 200*time.Millisecond, startFakeFiler(t, local))

	start := time.Now()
	entry, err := s3a.getObjectEntryRoutedByKey("b", "k")
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("getObjectEntryRoutedByKey: %v", err)
	}
	if entry.Name != "local" {
		t.Fatalf("entry from %q, want the current filer's", entry.Name)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("read took %v, want about the 200ms deadline", elapsed)
	}
	if !s3a.ownerRecentlyUnreachable(ownerAddr) {
		t.Fatal("owner that missed its deadline should be skipped by the next read")
	}

	if _, err := s3a.getObjectEntryRoutedByKey("b", "k"); err != nil {
		t.Fatalf("second read: %v", err)
	}
	if n := owner.calls.Load(); n != 1 {
		t.Fatalf("owner dialled %d times, want 1: the second read should skip it", n)
	}
}

// The deadline is a bound on an unreachable owner, not on a slow one when
// disabled: with RoutedLookupTimeout 0 a read waits for the owner as before.
func TestRoutedLookupWithoutDeadlineWaitsForOwner(t *testing.T) {
	owner := &slowLookupFiler{name: "owner", delay: 300 * time.Millisecond}
	local := &slowLookupFiler{name: "local"}
	s3a := newRoutedReadTestServer(t, startFakeFiler(t, owner), 0, startFakeFiler(t, local))

	entry, err := s3a.getObjectEntryRoutedByKey("b", "k")
	if err != nil {
		t.Fatalf("getObjectEntryRoutedByKey: %v", err)
	}
	if entry.Name != "owner" {
		t.Fatalf("entry from %q, want the owner's", entry.Name)
	}
}

// An owner that is the gateway's current filer gets no deadline: there is no
// closer filer to fail over to, and a slow answer there is still the answer.
func TestRoutedLookupAtCurrentFilerIsUnbounded(t *testing.T) {
	owner := &slowLookupFiler{name: "owner", delay: 300 * time.Millisecond}
	other := &slowLookupFiler{name: "other"}
	ownerAddr := startFakeFiler(t, owner)
	s3a := newRoutedReadTestServer(t, ownerAddr, 100*time.Millisecond, ownerAddr, startFakeFiler(t, other))
	if current := s3a.filerClient.GetCurrentFiler(); current != ownerAddr {
		t.Fatalf("current filer %s, want the owner %s", current, ownerAddr)
	}

	entry, err := s3a.getObjectEntryRoutedByKey("b", "k")
	if err != nil {
		t.Fatalf("getObjectEntryRoutedByKey: %v", err)
	}
	if entry.Name != "owner" {
		t.Fatalf("entry from %q, want the owner's", entry.Name)
	}
	if n := other.calls.Load(); n != 0 {
		t.Fatalf("other filer asked %d times; the current filer's owner should not be timed out", n)
	}
}

// Only the owner's attempt is bounded: when the walk falls through to a filer
// that is itself slow, that filer is waited for rather than failing the read.
func TestRoutedLookupDeadlineAppliesToOwnerOnly(t *testing.T) {
	owner := &slowLookupFiler{name: "owner", delay: -1}
	local := &slowLookupFiler{name: "local", delay: 300 * time.Millisecond}
	s3a := newRoutedReadTestServer(t, startFakeFiler(t, owner), 100*time.Millisecond, startFakeFiler(t, local))

	entry, err := s3a.getObjectEntryRoutedByKey("b", "k")
	if err != nil {
		t.Fatalf("getObjectEntryRoutedByKey: %v", err)
	}
	if entry.Name != "local" {
		t.Fatalf("entry from %q, want the current filer's", entry.Name)
	}
}

// The scoped listing (a .versions scan) fails over at the same deadline.
func TestRoutedListFailsOverAtDeadline(t *testing.T) {
	owner := &slowLookupFiler{name: "owner", delay: -1}
	local := &slowLookupFiler{name: "local"}
	s3a := newRoutedReadTestServer(t, startFakeFiler(t, owner), 200*time.Millisecond, startFakeFiler(t, local))

	start := time.Now()
	entries, _, err := s3a.listObjectScopedEntriesRoutedByKey("b", "k", "/buckets/b/k.versions", "", "", false, 10)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("listObjectScopedEntriesRoutedByKey: %v", err)
	}
	if len(entries) != 1 || entries[0].Name != "local" {
		t.Fatalf("entries %v, want the current filer's", entries)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("listing took %v, want about the 200ms deadline", elapsed)
	}
}

// An owner is skipped for UnreachableOwnerTTL, or the default when unset, and
// read owner-first again once it passes.
func TestUnreachableOwnerTTL(t *testing.T) {
	owner := pb.ServerAddress("127.0.0.1:8888.18888")

	for _, tc := range []struct {
		option *S3ApiServerOption
		want   time.Duration
	}{
		{nil, defaultUnreachableOwnerTTL},
		{&S3ApiServerOption{}, defaultUnreachableOwnerTTL},
		{&S3ApiServerOption{UnreachableOwnerTTL: 30 * time.Second}, 30 * time.Second},
	} {
		s3a := &S3ApiServer{option: tc.option}
		before := time.Now()
		s3a.markOwnerUnreachable(owner)
		v, ok := s3a.unreachableOwners.Load(owner)
		if !ok {
			t.Fatal("owner not flagged")
		}
		if ttl := v.(time.Time).Sub(before); ttl < tc.want || ttl > tc.want+time.Second {
			t.Errorf("option %+v: owner skipped for %v, want %v", tc.option, ttl, tc.want)
		}
	}

	s3a := &S3ApiServer{option: &S3ApiServerOption{UnreachableOwnerTTL: 50 * time.Millisecond}}
	s3a.markOwnerUnreachable(owner)
	if !s3a.ownerRecentlyUnreachable(owner) {
		t.Fatal("owner should be skipped within the TTL")
	}
	time.Sleep(100 * time.Millisecond)
	if s3a.ownerRecentlyUnreachable(owner) {
		t.Fatal("owner should be read owner-first again once the TTL passed")
	}
}

// trailingListFiler streams one entry at once and a second after tail, like a
// store scan that keeps going after its first rows.
type trailingListFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	tail time.Duration
}

func (f *trailingListFiler) ListEntries(_ *filer_pb.ListEntriesRequest, stream filer_pb.SeaweedFiler_ListEntriesServer) error {
	if err := stream.Send(&filer_pb.ListEntriesResponse{Entry: &filer_pb.Entry{Name: "v1"}}); err != nil {
		return err
	}
	select {
	case <-time.After(f.tail):
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
	return stream.Send(&filer_pb.ListEntriesResponse{Entry: &filer_pb.Entry{Name: "v2"}})
}

// The deadline bounds the wait for the owner's first answer, not a listing it
// has started streaming: the walk cannot replay a stream that already
// delivered entries, so cutting it off would fail the listing.
func TestRoutedListDeadlineEndsAtFirstEntry(t *testing.T) {
	owner := &trailingListFiler{tail: 400 * time.Millisecond}
	local := &slowLookupFiler{name: "local"}
	ownerAddr := startFakeFiler(t, owner)
	s3a := newRoutedReadTestServer(t, ownerAddr, 100*time.Millisecond, startFakeFiler(t, local))

	entries, _, err := s3a.listObjectScopedEntriesRoutedByKey("b", "k", "/buckets/b/k.versions", "", "", false, 10)
	if err != nil {
		t.Fatalf("listObjectScopedEntriesRoutedByKey: %v", err)
	}
	if len(entries) != 2 || entries[0].Name != "v1" || entries[1].Name != "v2" {
		t.Fatalf("entries %v, want the owner's full listing", entries)
	}
	if s3a.ownerRecentlyUnreachable(ownerAddr) {
		t.Fatal("an owner that answered should not be flagged unreachable")
	}
}
