package mount

import (
	"context"
	"slices"
	"sync/atomic"
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

// accessReportTestFiler records the reads reported to it. It answers
// TouchAccessTime with err, or, with stall set, not until the caller gives up.
type accessReportTestFiler struct {
	*lookupCacheTestFiler
	err     error
	stall   bool
	touches chan string
}

func newAccessReportTestFiler(err error) *accessReportTestFiler {
	return &accessReportTestFiler{
		lookupCacheTestFiler: &lookupCacheTestFiler{
			entries: []*filer_pb.Entry{
				{Name: "a", Attributes: &filer_pb.FuseAttributes{FileSize: 1, FileMode: 0100644}},
				{Name: "b", Attributes: &filer_pb.FuseAttributes{FileSize: 2, FileMode: 0100644}},
				{Name: "c", Attributes: &filer_pb.FuseAttributes{FileSize: 3, FileMode: 0100644}},
			},
			snapshotTsNs: 5000,
		},
		err:     err,
		touches: make(chan string, 16),
	}
}

func (s *accessReportTestFiler) TouchAccessTime(ctx context.Context, req *filer_pb.TouchAccessTimeRequest) (*filer_pb.TouchAccessTimeResponse, error) {
	s.touches <- string(util.JoinPath(req.Directory, req.Name))
	if s.stall {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if s.err != nil {
		return nil, s.err
	}
	return &filer_pb.TouchAccessTimeResponse{}, nil
}

// newAccessReportTestWFS returns a mount on fake with a reporter that is not
// started: its queue holds exactly what was reported until a test sends it.
func newAccessReportTestWFS(t *testing.T, fake *accessReportTestFiler) *WFS {
	t.Helper()
	wfs := newLookupCacheTestWFS(t, 60)
	wfs.openMtimeCache = make(map[uint64][2]int64)
	wfs.inodeToPath.Lookup("/dir", time.Now().Unix(), true, false, 0, true)
	startFakeFiler(t, wfs, fake)
	wfs.accessReporter = newAccessReporter(wfs.touchAccessTime)
	t.Cleanup(wfs.accessReporter.stop)
	return wfs
}

// addFiler serves fake as one more filer, after the ones wfs already has.
func addFiler(t *testing.T, wfs *WFS, fake *accessReportTestFiler) {
	t.Helper()
	existing := wfs.option.FilerAddresses
	startFakeFiler(t, wfs, fake)
	wfs.option.FilerAddresses = append(append([]pb.ServerAddress(nil), existing...), wfs.option.FilerAddresses...)
}

// queuedReports takes the reports waiting in r's queue.
func queuedReports(r *accessReporter) []string {
	var paths []string
	for {
		select {
		case report := <-r.queue:
			paths = append(paths, string(report.path))
		default:
			return paths
		}
	}
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
	fake := newAccessReportTestFiler(nil)
	wfs := newAccessReportTestWFS(t, fake)

	openForTest(t, wfs, "a", syscall.O_RDONLY)
	if got, want := queuedReports(wfs.accessReporter), []string{"/dir/a"}; !slices.Equal(got, want) {
		t.Fatalf("reports = %v, want %v", got, want)
	}

	if err := meta_cache.EnsureVisited(context.Background(), wfs.metaCache, wfs, "/dir", 0); err != nil {
		t.Fatalf("EnsureVisited: %v", err)
	}
	openForTest(t, wfs, "b", syscall.O_RDONLY)
	openForTest(t, wfs, "b", syscall.O_RDONLY)
	openForTest(t, wfs, "a", syscall.O_RDWR)
	openForTest(t, wfs, "c", syscall.O_WRONLY)
	if got, want := queuedReports(wfs.accessReporter), []string{"/dir/b", "/dir/b", "/dir/a"}; !slices.Equal(got, want) {
		t.Fatalf("reports = %v, want %v", got, want)
	}
	if lookups := fake.lookupCalls.Load(); lookups != 1 {
		t.Fatalf("lookups = %d, want 1: opens in a cached directory are answered locally", lookups)
	}
}

// TestCreateOfAnExistingFileIsReported checks the opens that reach Create,
// such as O_CREAT|O_RDWR of a file that already exists: they are reported
// like opens, and only with read access.
func TestCreateOfAnExistingFileIsReported(t *testing.T) {
	wfs := newAccessReportTestWFS(t, newAccessReportTestFiler(nil))
	dir := wfs.inodeToPath.Lookup("/dir", time.Now().Unix(), true, false, 0, true)

	for _, flags := range []uint32{syscall.O_RDWR | syscall.O_CREAT, syscall.O_WRONLY | syscall.O_CREAT} {
		out := &fuse.CreateOut{}
		if code := wfs.Create(nil, &fuse.CreateIn{InHeader: fuse.InHeader{NodeId: dir}, Flags: flags}, "a", out); code != fuse.OK {
			t.Fatalf("create with flags %o: %v", flags, code)
		}
		wfs.fhMap.ReleaseByHandle(FileHandleId(out.Fh))
	}
	if got, want := queuedReports(wfs.accessReporter), []string{"/dir/a"}; !slices.Equal(got, want) {
		t.Fatalf("reports = %v, want %v", got, want)
	}
}

func TestNoReportsWithoutAccessReport(t *testing.T) {
	fake := newAccessReportTestFiler(nil)
	wfs := newAccessReportTestWFS(t, fake)
	wfs.accessReporter = nil

	openForTest(t, wfs, "a", syscall.O_RDONLY)
	if n := len(fake.touches); n != 0 {
		t.Fatalf("filer got %d reports, want none", n)
	}
}

func TestReportsReachTheFiler(t *testing.T) {
	fake := newAccessReportTestFiler(nil)
	wfs := newAccessReportTestWFS(t, fake)
	wfs.accessReporter.start()

	openForTest(t, wfs, "a", syscall.O_RDONLY)
	select {
	case path := <-fake.touches:
		if path != "/dir/a" {
			t.Fatalf("report for %s, want /dir/a", path)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no report reached the filer")
	}
}

// TestReportingPausesWhenTheFilerHasNoHandler checks that a filer answering
// Unimplemented is not sent a report for every later open.
func TestReportingPausesWhenTheFilerHasNoHandler(t *testing.T) {
	fake := newAccessReportTestFiler(status.Error(codes.Unimplemented, "method TouchAccessTime not implemented"))
	wfs := newAccessReportTestWFS(t, fake)
	r := wfs.accessReporter

	openForTest(t, wfs, "a", syscall.O_RDONLY)
	select {
	case report := <-r.queue:
		r.send(report)
	default:
		t.Fatal("the open was not reported")
	}
	if !r.paused() {
		t.Fatal("reporting did not pause after Unimplemented")
	}
	openForTest(t, wfs, "b", syscall.O_RDONLY)
	if got := queuedReports(r); len(got) != 0 {
		t.Fatalf("reported %v while paused", got)
	}
	if n := len(fake.touches); n != 1 {
		t.Fatalf("filer got %d reports, want 1", n)
	}
}

// TestQueuedReportsAreDroppedWhilePaused checks that reports queued before the
// pause are not sent to the filer that answered Unimplemented.
func TestQueuedReportsAreDroppedWhilePaused(t *testing.T) {
	var sent atomic.Int32
	r := newAccessReporter(func(ctx context.Context, path util.FullPath, atimeNs int64) error {
		sent.Add(1)
		return nil
	})
	defer r.stop()
	r.report("/dir/a")
	r.report("/dir/b")
	r.pausedUntil.Store(time.Now().Add(time.Hour).UnixNano())

	close(r.queue) // work returns once the queue is drained
	r.work()
	if n := sent.Load(); n != 0 {
		t.Fatalf("sent %d queued reports during the pause, want 0", n)
	}
}

// TestReportDoesNotBlockOnAFullQueue checks that report drops and counts what
// does not fit instead of waiting for room.
func TestReportDoesNotBlockOnAFullQueue(t *testing.T) {
	r := newAccessReporter(func(ctx context.Context, path util.FullPath, atimeNs int64) error { return nil })
	defer r.stop()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < accessReportQueueSize+1; i++ {
			r.report("/dir/a")
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("report blocked on a full queue")
	}
	if dropped := r.dropped.Load(); dropped != 1 {
		t.Fatalf("dropped = %d, want 1", dropped)
	}
}

func TestStopCancelsTheReportInFlight(t *testing.T) {
	entered, cancelled := make(chan struct{}), make(chan struct{})
	r := newAccessReporter(func(ctx context.Context, path util.FullPath, atimeNs int64) error {
		close(entered)
		<-ctx.Done()
		close(cancelled)
		return ctx.Err()
	})
	r.start()
	r.report("/dir/a")
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the report was not sent")
	}

	r.stop()
	select {
	case <-cancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("stop did not cancel the report in flight")
	}
	r.report("/dir/b")
	if n := len(r.queue); n != 0 {
		t.Fatalf("queued %d reports after stop", n)
	}
}

// TestTouchAccessTimeDoesNotFailOverOnAnAnswer checks that a filer that
// answers, even with an error, is not followed by the next filer.
func TestTouchAccessTimeDoesNotFailOverOnAnAnswer(t *testing.T) {
	for _, code := range []codes.Code{codes.Unimplemented, codes.InvalidArgument} {
		t.Run(code.String(), func(t *testing.T) {
			first := newAccessReportTestFiler(status.Error(code, "refused"))
			wfs := newAccessReportTestWFS(t, first)
			second := newAccessReportTestFiler(nil)
			addFiler(t, wfs, second)

			ctx, cancel := context.WithTimeout(context.Background(), accessReportTimeout)
			defer cancel()
			if err := wfs.touchAccessTime(ctx, "/dir/a", time.Now().UnixNano()); status.Code(err) != code {
				t.Fatalf("err = %v, want %v", err, code)
			}
			if len(first.touches) != 1 || len(second.touches) != 0 {
				t.Fatalf("reports: first filer %d, second %d, want 1 and 0", len(first.touches), len(second.touches))
			}
		})
	}
}

// TestTouchAccessTimeFailsOverPastAStalledFiler checks that a filer that never
// answers uses up one attempt, not the whole report, even though one filer
// call may otherwise take longer than a report, and that the mount then keeps
// the filer that answered.
func TestTouchAccessTimeFailsOverPastAStalledFiler(t *testing.T) {
	stalled := newAccessReportTestFiler(nil)
	stalled.stall = true
	wfs := newAccessReportTestWFS(t, stalled)
	if wfs.filerRPCWait() < accessReportTimeout {
		t.Fatalf("filer call bound %v is below the report bound %v; the test would not stall", wfs.filerRPCWait(), accessReportTimeout)
	}
	healthy := newAccessReportTestFiler(nil)
	addFiler(t, wfs, healthy)

	ctx, cancel := context.WithTimeout(context.Background(), accessReportTimeout)
	defer cancel()
	if err := wfs.touchAccessTime(ctx, "/dir/a", time.Now().UnixNano()); err != nil {
		t.Fatalf("report did not reach the healthy filer: %v", err)
	}
	if len(healthy.touches) != 1 {
		t.Fatalf("healthy filer got %d reports, want 1", len(healthy.touches))
	}
	if i := atomic.LoadInt32(&wfs.option.filerIndex); i != 1 {
		t.Fatalf("filerIndex = %d, want 1, the filer that answered", i)
	}
}
