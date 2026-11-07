package command

import (
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
	"github.com/seaweedfs/seaweedfs/weed/replication/source"
)

// The write-backs of -allMounts start this many at a time, this far apart:
// each one replays the filer's log from its own offset, and a process that
// starts a hundred of them at once makes the filer replay a hundred logs
// together.
const (
	allMountsStartBatch = 10
	allMountsStartPause = 500 * time.Millisecond
	allMountsRetryMin   = time.Second
	allMountsRetryMax   = 30 * time.Second
	// A directory mounted while this process runs, with no offset saved yet,
	// waits this long for one: remote.mount saves it right after the
	// mapping, and other mount tooling may save it later.
	allMountsOffsetWait = 2 * time.Minute
	// A directory whose write-back keeps ending at an unmount this many times
	// in a row, while the live table still mounts it at the same location, is
	// warned about: its replayed log holds an unmount it stops at every time.
	allMountsGoneWarnAfter = 5
)

// mountSupervisor runs one write-back per mounted directory in this process.
// Each is the -dir write-back: its own subscription, offset, processor and
// retry loop, under its own client id (the filer keeps one subscription per
// client id, so a shared id would make each new follow replace the last).
//
// The supervisor only starts write-backs. A write-back ends itself when the
// mount table it receives, in log order, no longer maps its directory to its
// location, exactly where the -dir process exits: everything before that
// point is written back, nothing after it (remote.unmount's purge) is. The
// supervisor never stops a running write-back because the live table changed,
// which would cut off a write-back still behind in the log. A directory whose
// write-back ended is started again, from its saved offset, while the live
// table still mounts it (with a growing pause if it keeps ending at once).
type mountSupervisor struct {
	option      *RemoteSyncOptions
	filerSource *source.FilerSource

	// readMappings, follow, readOffset, now and warn are the seams the tests
	// replace.
	readMappings func() (*remote_pb.RemoteStorageMapping, error)
	follow       func(option *RemoteSyncOptions, dir string, watch *mountWatch) error
	readOffset   func(dir string) (int64, error)
	now          func() time.Time
	warn         func(format string, args ...interface{})

	mu        sync.Mutex
	followers map[string]*mountFollower
	// notBefore holds a directory whose write-back ended until its next start;
	// ends counts its consecutive quick ends, for the pause.
	notBefore map[string]time.Time
	ends      map[string]int
	// firstSeen is when a mounted directory without a saved offset was first
	// read in the table (allMountsOffsetWait).
	firstSeen map[string]time.Time
	// gone counts, per directory, the write-backs in a row that ended at an
	// unmount while each was started at the same location and stored offset
	// (goneStreak).
	gone map[string]*goneStreak
	wake chan struct{}
	// clientIds hands each follow its own non-zero client id.
	clientIds atomic.Int32
}

// goneStreak is a directory's run of write-backs that ended at an unmount,
// each started at loc from the stored offset; warned is set once the run has
// been reported.
type goneStreak struct {
	loc    *remote_pb.RemoteStorageLocation
	offset int64
	ends   int
	warned bool
}

type mountFollower struct {
	stop chan struct{}
	done chan struct{}
}

func newMountSupervisor(option *RemoteSyncOptions, filerSource *source.FilerSource) *mountSupervisor {
	m := &mountSupervisor{
		option:      option,
		filerSource: filerSource,
		followers:   map[string]*mountFollower{},
		notBefore:   map[string]time.Time{},
		ends:        map[string]int{},
		firstSeen:   map[string]time.Time{},
		gone:        map[string]*goneStreak{},
		wake:        make(chan struct{}, 1),
		now:         time.Now,
		warn:        glog.Warningf,
	}
	m.clientIds.Store(int32(time.Now().UnixNano()))
	m.readMappings = func() (*remote_pb.RemoteStorageMapping, error) { return readMountMappings(option) }
	m.follow = func(o *RemoteSyncOptions, dir string, watch *mountWatch) error {
		return followMountedDir(o, filerSource, dir, watch)
	}
	m.readOffset = func(dir string) (int64, error) {
		return remote_storage.GetSyncOffset(option.grpcDialOption, pb.ServerAddress(*option.filerAddress), dir)
	}
	return m
}

// run starts write-backs until quit closes (never, for the command), then
// stops every write-back and waits for them.
func (m *mountSupervisor) run(quit <-chan struct{}) {
	interval := 5 * time.Second
	if m.option.mountsCheck != nil && *m.option.mountsCheck > 0 {
		interval = *m.option.mountsCheck
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := m.reconcile(); err != nil {
			glog.Errorf("filer.remote.sync -allMounts: read mount table: %v", err)
		}
		select {
		case <-quit:
			m.stopAll()
			return
		case <-ticker.C:
		case <-m.wake:
		}
	}
}

// reconcile starts a write-back for every mounted directory that has none
// running, is past its pause, and has a saved offset (or waited for one).
func (m *mountSupervisor) reconcile() error {
	mappings, err := m.readMappings()
	if err != nil {
		return err
	}
	now := m.now()
	var starting []string
	m.mu.Lock()
	for dir := range m.firstSeen {
		if mappings == nil || mappings.Mappings[dir] == nil {
			delete(m.firstSeen, dir)
		}
	}
	for dir := range m.gone {
		if mappings == nil || mappings.Mappings[dir] == nil {
			delete(m.gone, dir)
		}
	}
	for dir := range m.notBefore {
		if _, running := m.followers[dir]; !running && (mappings == nil || mappings.Mappings[dir] == nil) {
			delete(m.notBefore, dir)
			delete(m.ends, dir)
		}
	}
	if mappings != nil {
		for dir := range mappings.Mappings {
			if _, running := m.followers[dir]; running {
				continue
			}
			if t, paused := m.notBefore[dir]; paused && now.Before(t) {
				continue
			}
			starting = append(starting, dir)
		}
	}
	m.mu.Unlock()

	sort.Strings(starting)
	ready := starting[:0]
	offsets := map[string]int64{}
	for _, dir := range starting {
		if settled, offset := m.offsetSettled(dir, now); settled {
			ready = append(ready, dir)
			offsets[dir] = offset
		}
	}
	for i, dir := range ready {
		if i > 0 && i%allMountsStartBatch == 0 {
			time.Sleep(allMountsStartPause)
		}
		m.noteStart(dir, mappings.Mappings[dir], offsets[dir])
		m.start(dir)
	}
	return nil
}

// noteStart keeps a directory's run of ends at an unmount going while it is
// started again at the same location from the same stored offset (an end
// saves none; a write-back that saved progress, or remote.unmount, moved
// it), and warns once when the run reaches
// allMountsGoneWarnAfter: a write-back that keeps replaying an unmount from
// its saved offset ends there each time and never writes the directory back.
// Nothing changes but the log; the operator decides (setting the directory's
// sync offset to now skips the replay).
func (m *mountSupervisor) noteStart(dir string, loc *remote_pb.RemoteStorageLocation, offset int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	streak := m.gone[dir]
	if streak == nil || !sameMountLocation(streak.loc, loc) || streak.offset != offset {
		m.gone[dir] = &goneStreak{loc: loc, offset: offset}
		return
	}
	if streak.ends >= allMountsGoneWarnAfter && !streak.warned {
		streak.warned = true
		m.warn("filer.remote.sync -allMounts: the write-back of %s has ended %d times in a row at an unmount in its replayed log while the mount table still mounts it at %+v; it is not writing the directory back. Set its sync offset to now to skip the replay", dir, streak.ends, loc)
	}
}

// offsetSettled reports whether a directory may start, and its stored
// offset: it has a saved offset, or has waited allMountsOffsetWait for one.
// An unreadable offset waits too (the follow could not start from it
// anyway).
func (m *mountSupervisor) offsetSettled(dir string, now time.Time) (bool, int64) {
	offset, err := m.readOffset(dir)
	m.mu.Lock()
	defer m.mu.Unlock()
	if err == nil && offset > 0 {
		delete(m.firstSeen, dir)
		return true, offset
	}
	seen, ok := m.firstSeen[dir]
	if !ok {
		m.firstSeen[dir] = now
		seen = now
	}
	if err != nil {
		glog.Errorf("filer.remote.sync -allMounts: read sync offset of %s: %v", dir, err)
		return false, 0
	}
	if now.Sub(seen) >= allMountsOffsetWait {
		glog.V(0).Infof("filer.remote.sync -allMounts: %s has no saved offset after %v; starting from its creation", dir, allMountsOffsetWait)
		delete(m.firstSeen, dir)
		return true, 0
	}
	return false, 0
}

func (m *mountSupervisor) poke() {
	select {
	case m.wake <- struct{}{}:
	default:
	}
}

func (m *mountSupervisor) start(dir string) {
	f := &mountFollower{stop: make(chan struct{}), done: make(chan struct{})}
	m.mu.Lock()
	m.followers[dir] = f
	m.mu.Unlock()

	option := *m.option
	option.clientId = m.clientIds.Add(1)
	if option.clientId == 0 { // the filer does not register client id 0
		option.clientId = m.clientIds.Add(1)
	}
	option.clientEpoch = 0
	watch := &mountWatch{stop: f.stop}

	go func() {
		defer close(f.done)
		glog.V(0).Infof("filer.remote.sync -allMounts: synchronize %s to remote storage", dir)
		began := m.now()
		wait := allMountsRetryMin
		for {
			attempt := time.Now()
			err := m.follow(&option, dir, watch)
			if errors.Is(err, errFollowStopped) || isClosed(f.stop) {
				m.forget(dir, f)
				return
			}
			if errors.Is(err, errMountGone) {
				m.mu.Lock()
				if streak := m.gone[dir]; streak != nil {
					streak.ends++
				}
				m.mu.Unlock()
				// ended where the -dir process exits; the next reconcile
				// starts it again if the live table still mounts it
				m.ended(dir, f, m.now().Sub(began))
				return
			}
			if err != nil {
				glog.Errorf("synchronize %s: %v", dir, err)
			}
			// no stream is running here: a directory the live table no longer
			// mounts is retired, and started again if it is mounted again
			if !m.stillMounted(dir) {
				m.forget(dir, f)
				return
			}
			if time.Since(attempt) > allMountsRetryMax {
				wait = allMountsRetryMin
			}
			select {
			case <-f.stop:
				m.forget(dir, f)
				return
			case <-time.After(wait):
			}
			if wait *= 2; wait > allMountsRetryMax {
				wait = allMountsRetryMax
			}
		}
	}()
}

// ended records a write-back that ended itself: its directory waits before
// its next start, longer each time it ends again within a minute.
func (m *mountSupervisor) ended(dir string, f *mountFollower, ran time.Duration) {
	m.mu.Lock()
	if m.followers[dir] == f {
		delete(m.followers, dir)
	}
	if ran > time.Minute {
		m.ends[dir] = 0
	}
	pause := allMountsRetryMin << m.ends[dir]
	if pause > allMountsRetryMax || pause <= 0 {
		pause = allMountsRetryMax
	} else {
		m.ends[dir]++
	}
	m.notBefore[dir] = m.now().Add(pause)
	m.mu.Unlock()
	m.poke()
}

// stillMounted reports whether the live table maps dir; an unreadable table
// counts as mapped.
func (m *mountSupervisor) stillMounted(dir string) bool {
	mappings, err := m.readMappings()
	return err != nil || (mappings != nil && mappings.Mappings[dir] != nil)
}

func (m *mountSupervisor) forget(dir string, f *mountFollower) {
	m.mu.Lock()
	if m.followers[dir] == f {
		delete(m.followers, dir)
	}
	m.mu.Unlock()
}

func (m *mountSupervisor) stopAll() {
	m.mu.Lock()
	var all []*mountFollower
	for _, f := range m.followers {
		close(f.stop)
		all = append(all, f)
	}
	m.mu.Unlock()
	for _, f := range all {
		<-f.done
	}
}
