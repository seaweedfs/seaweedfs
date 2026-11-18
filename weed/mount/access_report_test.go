package mount

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweedfs/weed/filer"
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
	onTouch func()
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
	if s.onTouch != nil {
		s.onTouch()
	}
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
	wfs.accessReporter = newAccessReporter(wfs.touchAccessTime, 0)
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

// TestOpensWithinTheIntervalAreReportedOnce checks that a path is reported
// on its first open and again only once the interval has passed.
func TestOpensWithinTheIntervalAreReportedOnce(t *testing.T) {
	wfs := newAccessReportTestWFS(t, newAccessReportTestFiler(nil))
	wfs.accessReporter = newAccessReporter(wfs.touchAccessTime, time.Hour)
	t.Cleanup(wfs.accessReporter.stop)

	openForTest(t, wfs, "a", syscall.O_RDONLY)
	openForTest(t, wfs, "a", syscall.O_RDONLY)
	openForTest(t, wfs, "b", syscall.O_RDWR)
	openForTest(t, wfs, "a", syscall.O_RDONLY)
	if got, want := queuedReports(wfs.accessReporter), []string{"/dir/a", "/dir/b"}; !slices.Equal(got, want) {
		t.Fatalf("reports = %v, want %v", got, want)
	}

	last, _ := wfs.accessReporter.recent.Get("/dir/a")
	last.reportedNs -= int64(time.Hour)
	wfs.accessReporter.recent.Add("/dir/a", last)
	openForTest(t, wfs, "a", syscall.O_RDONLY)
	openForTest(t, wfs, "b", syscall.O_RDONLY)
	if got, want := queuedReports(wfs.accessReporter), []string{"/dir/a"}; !slices.Equal(got, want) {
		t.Fatalf("reports after the interval = %v, want %v", got, want)
	}
}

// TestOpenAfterAWriteIsReportedWithinTheInterval checks that a file modified
// since its last report is reported again on its next open within the
// interval, as relatime updates atime on the first read after a write, and
// that a writer clock ahead of the mount's does not make every open due.
func TestOpenAfterAWriteIsReportedWithinTheInterval(t *testing.T) {
	wfs := newAccessReportTestWFS(t, newAccessReportTestFiler(nil))
	wfs.accessReporter = newAccessReporter(wfs.touchAccessTime, time.Hour)
	t.Cleanup(wfs.accessReporter.stop)
	if err := meta_cache.EnsureVisited(context.Background(), wfs.metaCache, wfs, "/dir", 0); err != nil {
		t.Fatalf("EnsureVisited: %v", err)
	}
	modify := func(mtime time.Time, versionTsNs int64) {
		entry := &filer_pb.Entry{Name: "a", Attributes: &filer_pb.FuseAttributes{FileSize: 1, FileMode: 0100644,
			Mtime: mtime.Unix(), MtimeNs: int32(mtime.Nanosecond())}}
		if err := wfs.metaCache.InsertEntry(context.Background(), filer.FromPbEntry("/dir", entry), versionTsNs); err != nil {
			t.Fatalf("InsertEntry: %v", err)
		}
	}

	openForTest(t, wfs, "a", syscall.O_RDONLY)
	openForTest(t, wfs, "a", syscall.O_RDONLY)
	modify(time.Now().Add(time.Millisecond), 6000)
	openForTest(t, wfs, "a", syscall.O_RDONLY)
	openForTest(t, wfs, "a", syscall.O_RDONLY)
	if got, want := queuedReports(wfs.accessReporter), []string{"/dir/a", "/dir/a"}; !slices.Equal(got, want) {
		t.Fatalf("reports = %v, want %v", got, want)
	}

	modify(time.Now().Add(time.Minute), 7000)
	openForTest(t, wfs, "a", syscall.O_RDONLY)
	openForTest(t, wfs, "a", syscall.O_RDONLY)
	if got, want := queuedReports(wfs.accessReporter), []string{"/dir/a"}; !slices.Equal(got, want) {
		t.Fatalf("reports with the writer clock ahead = %v, want %v", got, want)
	}
}

// TestMtimeAheadDoesNotExtendTheInterval checks that a file dated in the
// future is reported again once the interval has passed by this mount's
// clock.
func TestMtimeAheadDoesNotExtendTheInterval(t *testing.T) {
	r := newAccessReporter(nil, time.Hour)
	t.Cleanup(r.stop)
	start := time.Now().UnixNano()
	mtime := start + int64(24*time.Hour)
	for _, step := range []struct {
		after time.Duration
		due   bool
	}{{0, true}, {time.Minute, false}, {time.Hour, true}, {time.Hour + time.Minute, false}} {
		if got := r.due("/dir/a", start+int64(step.after), mtime); got != step.due {
			t.Fatalf("due after %v = %v, want %v", step.after, got, step.due)
		}
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

// TestCreateThroughAHardLinkReportsItsName checks that a create opening an
// existing hard link reports the name it was given, not the inode's first
// known name.
func TestCreateThroughAHardLinkReportsItsName(t *testing.T) {
	fake := newAccessReportTestFiler(nil)
	for _, name := range []string{"h1", "h2"} {
		fake.entries = append(fake.entries, &filer_pb.Entry{Name: name, HardLinkId: []byte("link"),
			Attributes: &filer_pb.FuseAttributes{FileSize: 1, FileMode: 0100644, Inode: 4242}})
	}
	wfs := newAccessReportTestWFS(t, fake)
	dir := wfs.inodeToPath.Lookup("/dir", time.Now().Unix(), true, false, 0, true)
	wfs.inodeToPath.Lookup("/dir/h1", time.Now().Unix(), false, true, 4242, true)
	if err := meta_cache.EnsureVisited(context.Background(), wfs.metaCache, wfs, "/dir", 0); err != nil {
		t.Fatalf("EnsureVisited: %v", err)
	}

	out := &fuse.CreateOut{}
	if code := wfs.Create(nil, &fuse.CreateIn{InHeader: fuse.InHeader{NodeId: dir}, Flags: syscall.O_RDWR | syscall.O_CREAT}, "h2", out); code != fuse.OK {
		t.Fatalf("create: %v", code)
	}
	wfs.fhMap.ReleaseByHandle(FileHandleId(out.Fh))
	if out.NodeId != 4242 {
		t.Fatalf("create opened inode %d, want the hard link's 4242", out.NodeId)
	}
	if got, want := queuedReports(wfs.accessReporter), []string{"/dir/h2"}; !slices.Equal(got, want) {
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
	}, 0)
	defer r.stop()
	r.report("/dir/a", 0)
	r.report("/dir/b", 0)
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
	r := newAccessReporter(func(ctx context.Context, path util.FullPath, atimeNs int64) error { return nil }, 0)
	defer r.stop()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < accessReportQueueSize+1; i++ {
			r.report("/dir/a", 0)
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
	}, 0)
	r.start()
	r.report("/dir/a", 0)
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
}

// TestReportAfterStopIsDropped uses a reporter without workers, so a report
// that stop failed to refuse stays in the queue.
func TestReportAfterStopIsDropped(t *testing.T) {
	r := newAccessReporter(func(ctx context.Context, path util.FullPath, atimeNs int64) error { return nil }, 0)
	r.stop()
	r.report("/dir/b", 0)
	if got := queuedReports(r); len(got) != 0 {
		t.Fatalf("queued %v after stop", got)
	}
}

// TestTouchAccessTimeDoesNotFailOverOnAnAnswer checks that a filer that
// answers, even with an error, is not followed by the next filer.
func TestTouchAccessTimeDoesNotFailOverOnAnAnswer(t *testing.T) {
	for _, answer := range []struct {
		code    codes.Code
		message string
	}{
		{codes.Unimplemented, "refused"},
		{codes.Unimplemented, "rate limit reporting not implemented"},
		{codes.InvalidArgument, "refused"},
		{codes.InvalidArgument, "invalid path /transport"},
	} {
		code := answer.code
		t.Run(code.String()+" "+answer.message, func(t *testing.T) {
			first := newAccessReportTestFiler(status.Error(code, answer.message))
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

// TestRejectedReportKeepsTheFilerChannel checks that a filer rejecting a
// report with a message that reads like a transport failure does not close
// the channel other calls to that filer are still using.
func TestRejectedReportKeepsTheFilerChannel(t *testing.T) {
	fake := newAccessReportTestFiler(status.Error(codes.InvalidArgument, "invalid path /transport"))
	inFlight, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	fake.onTouch = func() {
		if calls.Add(1) == 1 {
			close(inFlight)
			<-release
		}
	}
	wfs := newAccessReportTestWFS(t, fake)
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock)

	ctx, cancel := context.WithTimeout(context.Background(), accessReportTimeout)
	defer cancel()
	// The call held in flight gets the whole test deadline, not a report
	// attempt's share, so a slow runner cannot time it out on a healthy channel.
	first := make(chan error, 1)
	go func() {
		first <- wfs.callFiler(ctx, false, accessReportTimeout, wfs.option.FilerAddresses[0].ToGrpcAddress(), func(ctx context.Context, client filer_pb.SeaweedFilerClient) error {
			_, err := client.TouchAccessTime(ctx, &filer_pb.TouchAccessTimeRequest{Directory: "/dir", Name: "a", ClientAtimeNs: time.Now().UnixNano()})
			return err
		})
	}()
	select {
	case <-inFlight:
	case err := <-first:
		t.Fatalf("first call returned %v before reaching the filer", err)
	case <-ctx.Done():
		t.Fatal("first call never reached the filer")
	}
	if err := wfs.touchAccessTime(ctx, "/dir/transport", time.Now().UnixNano()); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("rejected report: err = %v, want InvalidArgument", err)
	}
	unblock()
	if err := <-first; status.Code(err) != codes.InvalidArgument {
		t.Fatalf("call in flight on the same channel: err = %v, want the filer's InvalidArgument", err)
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
	if len(stalled.touches) != 1 || len(healthy.touches) != 1 {
		t.Fatalf("reports: stalled filer %d, healthy filer %d, want 1 and 1", len(stalled.touches), len(healthy.touches))
	}
	if i := atomic.LoadInt32(&wfs.option.filerIndex); i != 1 {
		t.Fatalf("filerIndex = %d, want 1, the filer that answered", i)
	}
}

// TestTouchAccessTimeKeepsANewerFilerSelection checks that a report that
// failed over does not move the mount back from a filer another call
// selected while the report was in flight.
func TestTouchAccessTimeKeepsANewerFilerSelection(t *testing.T) {
	down := newAccessReportTestFiler(status.Error(codes.Unavailable, "down"))
	wfs := newAccessReportTestWFS(t, down)
	answering := newAccessReportTestFiler(nil)
	addFiler(t, wfs, answering)
	addFiler(t, wfs, newAccessReportTestFiler(nil))
	answering.onTouch = func() { atomic.StoreInt32(&wfs.option.filerIndex, 2) }

	ctx, cancel := context.WithTimeout(context.Background(), accessReportTimeout)
	defer cancel()
	if err := wfs.touchAccessTime(ctx, "/dir/a", time.Now().UnixNano()); err != nil {
		t.Fatalf("report: %v", err)
	}
	if len(down.touches) != 1 || len(answering.touches) != 1 {
		t.Fatalf("reports: unavailable filer %d, answering filer %d, want 1 and 1", len(down.touches), len(answering.touches))
	}
	if i := atomic.LoadInt32(&wfs.option.filerIndex); i != 2 {
		t.Fatalf("filerIndex = %d, want 2, the filer selected during the report", i)
	}
}

// TestTouchAccessTimeLeavesTimeForTheLastFiler checks that stalled filers
// share the report's time with the filers after them instead of each taking
// a full attempt, so a healthy last filer is still reached.
func TestTouchAccessTimeLeavesTimeForTheLastFiler(t *testing.T) {
	first := newAccessReportTestFiler(nil)
	first.stall = true
	wfs := newAccessReportTestWFS(t, first)
	second := newAccessReportTestFiler(nil)
	second.stall = true
	addFiler(t, wfs, second)
	healthy := newAccessReportTestFiler(nil)
	addFiler(t, wfs, healthy)

	reportTime := 3 * accessReportAttemptTimeout / 4
	ctx, cancel := context.WithTimeout(context.Background(), reportTime)
	defer cancel()
	if err := wfs.touchAccessTime(ctx, "/dir/a", time.Now().UnixNano()); err != nil {
		t.Fatalf("report did not reach the last filer within %v: %v", reportTime, err)
	}
	if len(first.touches) != 1 || len(second.touches) != 1 || len(healthy.touches) != 1 {
		t.Fatalf("reports: %d, %d, %d, want one to each filer", len(first.touches), len(second.touches), len(healthy.touches))
	}
}
