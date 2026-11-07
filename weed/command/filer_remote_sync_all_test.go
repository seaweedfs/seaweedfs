package command

import (
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
)

// fakeMounts is a mount table and a record of the follows run against it.
type fakeMounts struct {
	mu       sync.Mutex
	table    map[string]*remote_pb.RemoteStorageLocation
	active   map[string]int // follows running per directory
	overlap  bool           // two follows ever ran at once for one directory, or a client id 0
	started  map[string]int
	clients  map[int32]string
	gone     map[string]chan struct{} // closing one makes that directory's follow reach an unmount
	noOffset map[string]bool          // directories without a saved offset
	failing  map[string]int           // follows of these directories return an error this many times
}

func newFakeMounts() *fakeMounts {
	return &fakeMounts{
		table:    map[string]*remote_pb.RemoteStorageLocation{},
		active:   map[string]int{},
		started:  map[string]int{},
		clients:  map[int32]string{},
		gone:     map[string]chan struct{}{},
		noOffset: map[string]bool{},
		failing:  map[string]int{},
	}
}

func (f *fakeMounts) mount(dir string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.table[dir] = &remote_pb.RemoteStorageLocation{Name: "r", Bucket: "zone", Path: dir}
}

func (f *fakeMounts) unmount(dir string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.table, dir)
}

// reachUnmount makes the running follow of dir receive a table without it.
func (f *fakeMounts) reachUnmount(dir string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	close(f.gone[dir])
}

func (f *fakeMounts) supervisor() *mountSupervisor {
	interval := 10 * time.Millisecond
	m := newMountSupervisor(&RemoteSyncOptions{mountsCheck: &interval}, nil)
	m.readMappings = func() (*remote_pb.RemoteStorageMapping, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		out := &remote_pb.RemoteStorageMapping{Mappings: map[string]*remote_pb.RemoteStorageLocation{}}
		for k, v := range f.table {
			out.Mappings[k] = v
		}
		return out, nil
	}
	m.readOffset = func(dir string) (int64, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.noOffset[dir] {
			return 0, nil
		}
		return 100, nil
	}
	m.follow = func(o *RemoteSyncOptions, dir string, watch *mountWatch) error {
		f.mu.Lock()
		f.active[dir]++
		if f.active[dir] > 1 || o.clientId == 0 {
			f.overlap = true
		}
		f.started[dir]++
		f.clients[o.clientId] = dir
		gone := make(chan struct{})
		f.gone[dir] = gone
		if f.table[dir] == nil { // DetectMountInfo
			f.active[dir]--
			f.mu.Unlock()
			return errors.New(dir + " is not mounted")
		}
		if f.failing[dir] > 0 {
			f.failing[dir]--
			f.active[dir]--
			f.mu.Unlock()
			return errors.New("stream reset")
		}
		f.mu.Unlock()
		var err error
		select {
		case <-watch.stop:
			err = errFollowStopped
		case <-gone:
			err = errMountGone
		}
		f.mu.Lock()
		f.active[dir]--
		f.mu.Unlock()
		return err
	}
	return m
}

func (f *fakeMounts) snapshot() (active map[string]int, started map[string]int, clients int, overlap bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	active, started = map[string]int{}, map[string]int{}
	for k, v := range f.active {
		active[k] = v
	}
	for k, v := range f.started {
		started[k] = v
	}
	return active, started, len(f.clients), f.overlap
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func runSupervisor(m *mountSupervisor) (stop func()) {
	quit := make(chan struct{})
	stopped := make(chan struct{})
	go func() { m.run(quit); close(stopped) }()
	return func() { close(quit); <-stopped }
}

func TestAllMountsFollowsEachMountOnceUnderItsOwnClient(t *testing.T) {
	f := newFakeMounts()
	for _, d := range []string{"/buckets/a", "/buckets/b", "/buckets/c"} {
		f.mount(d)
	}
	m := f.supervisor()
	stop := runSupervisor(m)

	waitFor(t, "three follows", func() bool {
		a, _, _, _ := f.snapshot()
		return a["/buckets/a"] == 1 && a["/buckets/b"] == 1 && a["/buckets/c"] == 1
	})
	time.Sleep(50 * time.Millisecond) // more reconciles change nothing
	_, started, clients, _ := f.snapshot()
	for d, n := range started {
		if n != 1 {
			t.Errorf("%s started %d times, want 1", d, n)
		}
	}
	// the filer keeps one subscription per client id: a shared id would make
	// each follow replace the others
	if clients != 3 {
		t.Errorf("%d distinct client ids for 3 follows", clients)
	}

	f.mount("/buckets/d")
	waitFor(t, "a new mount followed", func() bool { a, _, _, _ := f.snapshot(); return a["/buckets/d"] == 1 })

	stop()
	active, _, _, overlap := f.snapshot()
	for d, n := range active {
		if n != 0 {
			t.Errorf("%s still has %d follows after the supervisor stopped", d, n)
		}
	}
	if overlap {
		t.Error("two follows at once for one directory, or a client id 0")
	}
}

// An unmount in the live table does not stop a write-back still behind in
// the log: the changes made before the unmount would never reach the remote
// (remote.unmount then sets the offset past them). It ends where it reaches
// the unmount, and is not started again while the table does not mount it.
func TestAllMountsWriteBackEndsAtTheUnmountNotBefore(t *testing.T) {
	f := newFakeMounts()
	f.mount("/buckets/a")
	m := f.supervisor()
	stop := runSupervisor(m)
	defer stop()
	waitFor(t, "a followed", func() bool { a, _, _, _ := f.snapshot(); return a["/buckets/a"] == 1 })

	f.unmount("/buckets/a")
	time.Sleep(80 * time.Millisecond) // several reconciles
	if a, _, _, _ := f.snapshot(); a["/buckets/a"] != 1 {
		t.Fatal("the live unmount stopped a write-back that had not reached it")
	}
	f.reachUnmount("/buckets/a")
	waitFor(t, "a ended", func() bool { a, _, _, _ := f.snapshot(); return a["/buckets/a"] == 0 })
	time.Sleep(80 * time.Millisecond)
	if _, s, _, _ := f.snapshot(); s["/buckets/a"] != 1 {
		t.Errorf("an unmounted directory was started again (%d starts)", s["/buckets/a"])
	}
	// its restart pause goes with the mount, or the maps grow with every
	// directory ever mounted
	m.mu.Lock()
	_, paused := m.notBefore["/buckets/a"]
	_, counted := m.ends["/buckets/a"]
	m.mu.Unlock()
	if paused || counted {
		t.Errorf("an unmounted directory keeps its retry state: notBefore=%v ends=%v", paused, counted)
	}
}

// A write-back that ended at an unmount while the directory is mounted again
// (or ends at once, replaying an old table) is started again after a pause,
// never beside the one that ended.
func TestAllMountsRestartsAnEndedWriteBackWhileMounted(t *testing.T) {
	f := newFakeMounts()
	f.mount("/buckets/a")
	m := f.supervisor()
	clock := time.Now()
	var clockMu sync.Mutex
	m.now = func() time.Time { clockMu.Lock(); defer clockMu.Unlock(); return clock }
	stop := runSupervisor(m)
	defer stop()
	waitFor(t, "a followed", func() bool { a, _, _, _ := f.snapshot(); return a["/buckets/a"] == 1 })

	f.reachUnmount("/buckets/a")
	// its pause runs from when the supervisor recorded the end on the frozen clock
	waitFor(t, "a ended", func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		_, paused := m.notBefore["/buckets/a"]
		return paused
	})
	time.Sleep(60 * time.Millisecond)
	if _, s, _, _ := f.snapshot(); s["/buckets/a"] != 1 {
		t.Fatalf("restarted before its pause: %d starts", s["/buckets/a"])
	}
	clockMu.Lock()
	clock = clock.Add(allMountsRetryMin)
	clockMu.Unlock()
	waitFor(t, "a restarted", func() bool { a, s, _, _ := f.snapshot(); return a["/buckets/a"] == 1 && s["/buckets/a"] == 2 })
	if _, _, _, overlap := f.snapshot(); overlap {
		t.Error("two follows at once for one directory")
	}
}

func TestAllMountsGuards(t *testing.T) {
	f := newFakeMounts()
	f.mount("/buckets/a")
	f.failing["/buckets/a"] = 2 // two stream errors, then it runs
	m := f.supervisor()
	clock := time.Now()
	var clockMu sync.Mutex
	m.now = func() time.Time { clockMu.Lock(); defer clockMu.Unlock(); return clock }
	stop := runSupervisor(m)
	defer stop()

	// a follow that ends on a stream error is started again, never twice at once
	waitFor(t, "a running after its errors", func() bool {
		a, s, _, _ := f.snapshot()
		return a["/buckets/a"] == 1 && s["/buckets/a"] == 3
	})

	// a new mount without a saved offset waits for one, then starts
	f.mu.Lock()
	f.noOffset["/buckets/c"] = true
	f.mu.Unlock()
	f.mount("/buckets/c")
	// the wait runs from when a reconcile first saw it on the frozen clock
	waitFor(t, "c seen without an offset", func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		_, seen := m.firstSeen["/buckets/c"]
		return seen
	})
	time.Sleep(60 * time.Millisecond)
	if _, s, _, _ := f.snapshot(); s["/buckets/c"] != 0 {
		t.Fatalf("started before its offset was written")
	}
	clockMu.Lock()
	clock = clock.Add(allMountsOffsetWait)
	clockMu.Unlock()
	waitFor(t, "c started after the wait", func() bool { a, _, _, _ := f.snapshot(); return a["/buckets/c"] == 1 })

	if _, _, _, overlap := f.snapshot(); overlap {
		t.Error("two follows at once for one directory, or a client id 0")
	}
}

// A follow that fails while no stream runs, for a directory the live table no
// longer mounts, is retired instead of retried forever, and the directory is
// followed again once it is mounted again.
func TestAllMountsRetiresAFailedFollowWhoseMountIsGone(t *testing.T) {
	f := newFakeMounts()
	f.mount("/buckets/a")
	f.failing["/buckets/a"] = 1
	m := f.supervisor()
	stop := runSupervisor(m)
	defer stop()
	waitFor(t, "the first follow failed", func() bool { _, s, _, _ := f.snapshot(); return s["/buckets/a"] == 1 })

	f.unmount("/buckets/a")
	waitFor(t, "the follower retired", func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		_, running := m.followers["/buckets/a"]
		return !running
	})
	_, started, _, _ := f.snapshot()
	time.Sleep(80 * time.Millisecond)
	if _, s, _, _ := f.snapshot(); s["/buckets/a"] != started["/buckets/a"] {
		t.Fatalf("a retired follow was tried again: %d starts, then %d", started["/buckets/a"], s["/buckets/a"])
	}

	f.mount("/buckets/a")
	waitFor(t, "a followed again", func() bool { a, _, _, _ := f.snapshot(); return a["/buckets/a"] == 1 })
	if _, _, _, overlap := f.snapshot(); overlap {
		t.Error("two follows at once for one directory")
	}
}

func fileEvent(tsNs int64, dir, name string, deleted bool) *filer_pb.SubscribeMetadataResponse {
	e := &filer_pb.Entry{Name: name, Attributes: &filer_pb.FuseAttributes{}}
	n := &filer_pb.EventNotification{NewEntry: e, NewParentPath: dir}
	if deleted {
		n = &filer_pb.EventNotification{OldEntry: e}
	}
	return &filer_pb.SubscribeMetadataResponse{Directory: dir, TsNs: tsNs, EventNotification: n}
}

// runFence feeds events, in arrival order, through a -allMounts follow's
// stream callback and returns the positions whose jobs ran, and whether the
// write-back ended and where to.
func runFence(events []*filer_pb.SubscribeMetadataResponse) (admitted []int64, ended bool, moved *remote_pb.RemoteStorageLocation) {
	loc := &remote_pb.RemoteStorageLocation{Name: "r", Bucket: "zone", Path: "/a"}
	var mu sync.Mutex
	processor := NewMetadataProcessor(func(resp *filer_pb.SubscribeMetadataResponse) error {
		mu.Lock()
		defer mu.Unlock()
		admitted = append(admitted, resp.TsNs)
		return nil
	}, 128, 0)
	admit := admitUntilMountEnds(processor, "/buckets/a", "/buckets/a", loc, func(to *remote_pb.RemoteStorageLocation) {
		ended, moved = true, to
	})
	for _, e := range events {
		_ = admit(e)
	}
	processor.Drain()
	slices.Sort(admitted)
	return admitted, ended, moved
}

// remote.unmount deletes the mapping, then purges the directory: nothing
// arriving after the table without the directory may be admitted, or the
// purge deletes the remote objects. Not when the directory is mounted again
// at the same place right after (a -allMounts follow keeps receiving events
// until its stream ends, unlike the -dir process, which exits), nor for an
// event stamped earlier that arrives later.
func TestNothingIsAdmittedAfterAnUnmount(t *testing.T) {
	loc := &remote_pb.RemoteStorageLocation{Name: "r", Bucket: "zone", Path: "/a"}
	mapped := map[string]*remote_pb.RemoteStorageLocation{"/buckets/a": loc}
	none := map[string]*remote_pb.RemoteStorageLocation{}
	admitted, ended, moved := runFence([]*filer_pb.SubscribeMetadataResponse{
		mountTableEvent(t, 1, none, mapped),
		fileEvent(2, "/buckets/a/ns/t/data", "f1.parquet", false),
		mountTableEvent(t, 3, mapped, none),                        // unmounted
		fileEvent(2, "/buckets/a/ns/t/data", "late.parquet", true), // stamped before, arrives after
		fileEvent(4, "/buckets/a/ns/t/data", "f1.parquet", true),   // the purge
		mountTableEvent(t, 5, none, mapped),
		fileEvent(6, "/buckets/a/ns/t/data", "f2.parquet", false),
	})
	if want := []int64{1, 2}; !slices.Equal(admitted, want) {
		t.Errorf("jobs run %v, want %v", admitted, want)
	}
	if !ended || moved != nil {
		t.Errorf("ended=%v moved=%v, want an unmount", ended, moved)
	}

	// the mount table file removed outright is an unmount too
	admitted, ended, moved = runFence([]*filer_pb.SubscribeMetadataResponse{
		mountTableEvent(t, 2, mapped, nil),
		fileEvent(3, "/buckets/a/x", "f.parquet", true),
	})
	if len(admitted) != 0 || !ended || moved != nil {
		t.Errorf("mount table removed: jobs run %v, ended=%v moved=%v", admitted, ended, moved)
	}
}

func TestNothingIsAdmittedAfterAMove(t *testing.T) {
	loc := &remote_pb.RemoteStorageLocation{Name: "r", Bucket: "zone", Path: "/a"}
	other := &remote_pb.RemoteStorageLocation{Name: "r", Bucket: "zone2", Path: "/a"}
	admitted, ended, moved := runFence([]*filer_pb.SubscribeMetadataResponse{
		fileEvent(1, "/buckets/a/ns", "f.parquet", false),
		mountTableEvent(t, 2, map[string]*remote_pb.RemoteStorageLocation{"/buckets/a": loc}, map[string]*remote_pb.RemoteStorageLocation{"/buckets/a": other}),
		fileEvent(3, "/buckets/a/ns", "g.parquet", false),
	})
	if !slices.Equal(admitted, []int64{1}) || !ended || moved.GetBucket() != "zone2" {
		t.Errorf("moved: jobs run %v, ended=%v moved=%v", admitted, ended, moved)
	}
}

// A panic in one directory's job is that job's error, not the end of the
// process (and of every other directory's write-back).
func TestAPanickingJobIsAnError(t *testing.T) {
	job := recoverJob("/buckets/a", func(*filer_pb.SubscribeMetadataResponse) error {
		var m map[string]int
		m["x"] = 1 // nil map write
		return nil
	})
	if err := job(fileEvent(7, "/buckets/a", "f.parquet", false)); err == nil {
		t.Fatal("a panicking job returned no error")
	}
}

// A write-back behind in the log must not save its older position over the
// offset remote.unmount (or a mount) wrote: its successor would replay the
// unmount, end there, and never write the directory back.
func TestAFollowNeverMovesAnOffsetSomeoneElseWrote(t *testing.T) {
	stored := int64(100)
	g := &offsetGuard{dir: "/buckets/a", last: 100,
		read:  func() (int64, error) { return stored, nil },
		write: func(ts int64) error { stored = ts; return nil },
	}
	if err := g.save(150); err != nil || stored != 150 {
		t.Fatalf("own offset not saved: stored=%d err=%v", stored, err)
	}
	stored = 900 // remote.unmount, after its purge
	if err := g.save(160); err != nil || stored != 900 || !g.moved {
		t.Fatalf("saved over someone else's offset: stored=%d moved=%v err=%v", stored, g.moved, err)
	}
	if err := g.save(170); err != nil || stored != 900 {
		t.Fatalf("saved later over someone else's offset: stored=%d err=%v", stored, err)
	}
}

// A resume position kept for the next attempt in this process is only good
// for the mount and the offset it was earned under: a mount that now maps
// the directory elsewhere drops it (one that cannot be read leaves it to the
// next attempt), and so does an offset someone else moved since (a
// same-location unmount and remount):
// the attempt then starts from the stored offset. Its own exit save, landed
// although the call failed, is not someone else's.
func TestAResumePositionStaysWithItsMount(t *testing.T) {
	locA := &remote_pb.RemoteStorageLocation{Name: "r", Bucket: "zone", Path: "/a"}
	locB := &remote_pb.RemoteStorageLocation{Name: "r", Bucket: "zone", Path: "/b"}
	kept := func() *RemoteSyncOptions {
		return &RemoteSyncOptions{resumeTsNs: 50, resumeLoc: locA, resumeStoredNs: 100}
	}

	o := kept()
	if err := o.resumeFor("/buckets/a", "/buckets/a", locA, nil, 100); err != nil || o.resumeTsNs != 50 {
		t.Fatalf("same mount, same offset: resume=%d err=%v", o.resumeTsNs, err)
	}
	o = kept()
	if err := o.resumeFor("/buckets/a", "/buckets/a", locA, nil, 50); err != nil || o.resumeTsNs != 50 {
		t.Fatalf("its own exit save read back: resume=%d err=%v", o.resumeTsNs, err)
	}
	o = kept()
	if err := o.resumeFor("/buckets/a", "/buckets/a", locB, nil, 100); err != nil || o.resumeTsNs != 0 {
		t.Fatalf("remounted elsewhere: resume=%d err=%v", o.resumeTsNs, err)
	}
	// an unreadable mount keeps it: with -timeAgo and no checkpoint yet, the
	// retry would otherwise start at a later now minus timeAgo, past the
	// event the follow failed on
	o = kept()
	if err := o.resumeFor("/buckets/a", "", nil, errors.New("no table"), 100); err == nil || o.resumeTsNs != 50 {
		t.Fatalf("unreadable mount: resume=%d err=%v", o.resumeTsNs, err)
	}
	if err := o.resumeFor("/buckets/a", "/buckets/a", locB, nil, 100); err != nil || o.resumeTsNs != 0 {
		t.Fatalf("remounted elsewhere while the table was unreadable: resume=%d err=%v", o.resumeTsNs, err)
	}
	// a directory below a mount is judged by its own remote location: the
	// same remote mounted one level up maps it to another key
	below := &RemoteSyncOptions{resumeTsNs: 50, resumeLoc: dirRemoteLocation("/buckets/a", "/buckets/a/x", locA), resumeStoredNs: 100}
	if err := below.resumeFor("/buckets/a/x", "/buckets/a", locA, nil, 100); err != nil || below.resumeTsNs != 50 {
		t.Fatalf("below the same mount: resume=%d err=%v", below.resumeTsNs, err)
	}
	if err := below.resumeFor("/buckets/a/x", "/buckets", locA, nil, 100); err != nil || below.resumeTsNs != 0 {
		t.Fatalf("the same remote mounted above the old mount: resume=%d err=%v", below.resumeTsNs, err)
	}
	o = kept()
	if err := o.resumeFor("/buckets/a", "/buckets/a", locA, nil, 900); err != nil || o.resumeTsNs != 0 {
		t.Fatalf("offset moved since, same mount: resume=%d err=%v", o.resumeTsNs, err)
	}
	// with the position dropped, the attempt starts at the stored offset
	mounted := func() (*filer_pb.Entry, error) {
		return &filer_pb.Entry{Attributes: &filer_pb.FuseAttributes{Crtime: 1}}, nil
	}
	if start, err := startPosition("/buckets/a", 0, o.resumeTsNs, 900, mounted); err != nil || start.UnixNano() != 900 {
		t.Fatalf("start = %v (%v), want the stored offset 900", start.UnixNano(), err)
	}
	// positive control: a kept position is where the attempt starts
	if start, err := startPosition("/buckets/a", 0, 50, 900, mounted); err != nil || start.UnixNano() != 50 {
		t.Fatalf("start with a kept position = %v (%v), want 50", start.UnixNano(), err)
	}
}

// A save whose write failed may have landed: read back, that value is the
// guard's own, and it keeps saving. Anything else is someone else's move.
func TestAGuardsOwnWriteThatFailedIsNotAMove(t *testing.T) {
	stored := int64(100)
	failNext := true
	g := &offsetGuard{dir: "/buckets/a", last: 100,
		read: func() (int64, error) { return stored, nil },
		write: func(ts int64) error {
			stored = ts
			if failNext {
				failNext = false
				return errors.New("deadline exceeded")
			}
			return nil
		},
	}
	if err := g.save(150); err == nil {
		t.Fatal("the failed write reported no error")
	}
	if err := g.save(160); err != nil || stored != 160 || g.moved {
		t.Fatalf("own landed write taken as a move: stored=%d moved=%v err=%v", stored, g.moved, err)
	}
	stored = 900 // remote.unmount
	if err := g.save(170); err != nil || stored != 900 || !g.moved {
		t.Fatalf("someone else's offset: stored=%d moved=%v err=%v", stored, g.moved, err)
	}
}

// A directory whose write-back keeps ending at an unmount while the live
// table still mounts it at the same location, and its stored offset stays
// put, is warned about once per run of ends; nothing else changes (it is
// started again each time).
func TestAllMountsWarnsAboutAWriteBackThatKeepsEndingAtAnUnmount(t *testing.T) {
	loc := &remote_pb.RemoteStorageLocation{Name: "r", Bucket: "zone", Path: "/a"}
	table := &remote_pb.RemoteStorageMapping{Mappings: map[string]*remote_pb.RemoteStorageLocation{"/buckets/a": loc}}
	var mu sync.Mutex
	clock := time.Unix(1000, 0)
	follows, warns := 0, 0
	interval := time.Hour
	m := newMountSupervisor(&RemoteSyncOptions{mountsCheck: &interval}, nil)
	m.readMappings = func() (*remote_pb.RemoteStorageMapping, error) { mu.Lock(); defer mu.Unlock(); return table, nil }
	offset := int64(100)
	m.readOffset = func(string) (int64, error) { mu.Lock(); defer mu.Unlock(); return offset, nil }
	m.now = func() time.Time { mu.Lock(); defer mu.Unlock(); return clock }
	m.warn = func(string, ...interface{}) { mu.Lock(); warns++; mu.Unlock() }
	m.follow = func(*RemoteSyncOptions, string, *mountWatch) error {
		mu.Lock()
		follows++
		mu.Unlock()
		return errMountGone
	}
	round := func() {
		t.Helper()
		if err := m.reconcile(); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(5 * time.Second)
		for {
			m.mu.Lock()
			_, running := m.followers["/buckets/a"]
			m.mu.Unlock()
			if !running {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("the follow did not end")
			}
			time.Sleep(time.Millisecond)
		}
		mu.Lock()
		clock = clock.Add(time.Minute) // past any pause
		mu.Unlock()
	}
	for i := 0; i < allMountsGoneWarnAfter; i++ {
		round()
	}
	mu.Lock()
	if follows != allMountsGoneWarnAfter || warns != 0 {
		t.Fatalf("after %d ends: follows=%d warns=%d", allMountsGoneWarnAfter, follows, warns)
	}
	mu.Unlock()
	round()
	round()
	mu.Lock()
	if warns != 1 {
		t.Fatalf("warns = %d after the run reached %d, want exactly 1", warns, allMountsGoneWarnAfter)
	}
	// mounted elsewhere: a new run, warned about again only when it too
	// reaches the count
	table = &remote_pb.RemoteStorageMapping{Mappings: map[string]*remote_pb.RemoteStorageLocation{
		"/buckets/a": {Name: "r", Bucket: "zone", Path: "/b"}}}
	mu.Unlock()
	for i := 0; i < allMountsGoneWarnAfter; i++ {
		round()
	}
	mu.Lock()
	if warns != 1 {
		t.Fatalf("warns = %d before the new run reached the count", warns)
	}
	mu.Unlock()
	round()
	mu.Lock()
	if warns != 2 {
		t.Fatalf("warns = %d after the new run reached the count, want 2", warns)
	}

	// ends whose stored offset moves in between are not one replay: each
	// follow saved progress, or a remote.unmount and remount ran between
	table = &remote_pb.RemoteStorageMapping{Mappings: map[string]*remote_pb.RemoteStorageLocation{
		"/buckets/a": {Name: "r", Bucket: "zone", Path: "/c"}}}
	mu.Unlock()
	for i := 0; i < 2*allMountsGoneWarnAfter; i++ {
		mu.Lock()
		offset += 10
		mu.Unlock()
		round()
	}
	mu.Lock()
	defer mu.Unlock()
	if warns != 2 {
		t.Fatalf("warns = %d after ends from moving offsets, want still 2", warns)
	}
}
