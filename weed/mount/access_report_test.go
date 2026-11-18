package mount

import (
	"context"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweedfs/weed/mount/meta_cache"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// accessReportTestFiler records the reads reported to it. With implemented
// false it answers TouchAccessTime as a filer without a handler does.
type accessReportTestFiler struct {
	*lookupCacheTestFiler
	implemented bool
	touches     chan string
}

func (s *accessReportTestFiler) TouchAccessTime(ctx context.Context, req *filer_pb.TouchAccessTimeRequest) (*filer_pb.TouchAccessTimeResponse, error) {
	s.touches <- string(util.JoinPath(req.Directory, req.Name))
	if !s.implemented {
		return nil, status.Error(codes.Unimplemented, "method TouchAccessTime not implemented")
	}
	return &filer_pb.TouchAccessTimeResponse{}, nil
}

// expectTouches waits for the wanted reports, in any order since several
// workers send them, and then checks that no more arrive.
func (s *accessReportTestFiler) expectTouches(t *testing.T, want ...string) {
	t.Helper()
	var got []string
	for len(got) < len(want) {
		select {
		case path := <-s.touches:
			got = append(got, path)
		case <-time.After(5 * time.Second):
			t.Fatalf("got reports %v, want %v", got, want)
		}
	}
	select {
	case path := <-s.touches:
		t.Fatalf("extra report %s after %v, want only %v", path, got, want)
	case <-time.After(200 * time.Millisecond):
	}
	sort.Strings(got)
	want = append([]string(nil), want...)
	sort.Strings(want)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("reports = %v, want %v", got, want)
	}
}

func newAccessReportTestWFS(t *testing.T, implemented, accessReport bool) (*WFS, *accessReportTestFiler) {
	t.Helper()
	wfs := newLookupCacheTestWFS(t, 60)
	wfs.openMtimeCache = make(map[uint64][2]int64)
	wfs.inodeToPath.Lookup("/dir", time.Now().Unix(), true, false, 0, true)
	fake := &accessReportTestFiler{
		lookupCacheTestFiler: &lookupCacheTestFiler{
			entries: []*filer_pb.Entry{
				{Name: "a", Attributes: &filer_pb.FuseAttributes{FileSize: 1, FileMode: 0100644}},
				{Name: "b", Attributes: &filer_pb.FuseAttributes{FileSize: 2, FileMode: 0100644}},
			},
			snapshotTsNs: 5000,
		},
		implemented: implemented,
		touches:     make(chan string, 16),
	}
	startFakeFiler(t, wfs, fake)
	if accessReport {
		wfs.accessReporter = newAccessReporter(wfs.touchAccessTime)
	}
	return wfs, fake
}

func openForTest(t *testing.T, wfs *WFS, name string, flags uint32) {
	t.Helper()
	inode := wfs.inodeToPath.Lookup(util.FullPath("/dir").Child(name), time.Now().Unix(), false, false, 0, true)
	out := &fuse.OpenOut{}
	if status := wfs.Open(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: inode}, Flags: flags}, out); status != fuse.OK {
		t.Fatalf("open %s: %v", name, status)
	}
	wfs.fhMap.ReleaseByHandle(FileHandleId(out.Fh))
}

// TestEachOpenForReadingIsReportedOnce checks that every open of a file for
// reading is reported once, also when the open is answered from the metadata
// cache and the filer sees no lookup, and that a write-only open is not.
func TestEachOpenForReadingIsReportedOnce(t *testing.T) {
	wfs, fake := newAccessReportTestWFS(t, true, true)

	openForTest(t, wfs, "a", syscall.O_RDONLY)
	fake.expectTouches(t, "/dir/a")

	if err := meta_cache.EnsureVisited(context.Background(), wfs.metaCache, wfs, "/dir", 0); err != nil {
		t.Fatalf("EnsureVisited: %v", err)
	}
	openForTest(t, wfs, "b", syscall.O_RDONLY)
	openForTest(t, wfs, "b", syscall.O_RDONLY)
	openForTest(t, wfs, "a", syscall.O_RDWR)
	openForTest(t, wfs, "a", syscall.O_WRONLY)
	fake.expectTouches(t, "/dir/b", "/dir/b", "/dir/a")
	if lookups := fake.lookupCalls.Load(); lookups != 1 {
		t.Fatalf("lookups = %d, want 1: opens in a cached directory are answered locally", lookups)
	}
}

func TestNoReportsWithoutAccessReport(t *testing.T) {
	wfs, fake := newAccessReportTestWFS(t, true, false)

	openForTest(t, wfs, "a", syscall.O_RDONLY)
	fake.expectTouches(t)
}

// TestReportingPausesWhenTheFilerHasNoHandler checks that a filer answering
// Unimplemented is not sent a report for every later open.
func TestReportingPausesWhenTheFilerHasNoHandler(t *testing.T) {
	wfs, fake := newAccessReportTestWFS(t, false, true)

	openForTest(t, wfs, "a", syscall.O_RDONLY)
	fake.expectTouches(t, "/dir/a")
	deadline := time.Now().Add(5 * time.Second)
	for !wfs.accessReporter.paused() {
		if time.Now().After(deadline) {
			t.Fatal("reporting did not pause after Unimplemented")
		}
		time.Sleep(10 * time.Millisecond)
	}

	openForTest(t, wfs, "b", syscall.O_RDONLY)
	fake.expectTouches(t)
}

// TestTouchAccessTimeStopsAtUnimplemented checks that an Unimplemented answer
// ends the attempt instead of trying the next filer.
func TestTouchAccessTimeStopsAtUnimplemented(t *testing.T) {
	wfs, old := newAccessReportTestWFS(t, false, false)
	oldAddress := wfs.option.FilerAddresses[0]
	other := &accessReportTestFiler{lookupCacheTestFiler: &lookupCacheTestFiler{}, implemented: true, touches: make(chan string, 1)}
	startFakeFiler(t, wfs, other)
	wfs.option.FilerAddresses = append([]pb.ServerAddress{oldAddress}, wfs.option.FilerAddresses...)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := wfs.touchAccessTime(ctx, "/dir/a", time.Now().UnixNano()); status.Code(err) != codes.Unimplemented {
		t.Fatalf("err = %v, want Unimplemented", err)
	}
	old.expectTouches(t, "/dir/a")
	other.expectTouches(t)
}

// TestReportDoesNotBlockOnAFullQueue checks that report drops and counts
// what does not fit while every worker is stuck on a slow filer.
func TestReportDoesNotBlockOnAFullQueue(t *testing.T) {
	release := make(chan struct{})
	r := newAccessReporter(func(ctx context.Context, path util.FullPath, atimeNs int64) error {
		<-release
		return nil
	})
	defer close(release)

	for i := 0; i < accessReportQueueSize+accessReportWorkers+10; i++ {
		r.report("/dir/a")
	}
	if dropped := r.dropped.Load(); dropped < 10 {
		t.Fatalf("dropped = %d, want at least 10", dropped)
	}
}
