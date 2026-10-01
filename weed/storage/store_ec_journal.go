package storage

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// ecjMergeAttempts bounds how often an unmounted merge re-reads a journal
// that changed under it. Only a mount-delete-unmount between the read and the
// append changes it, so one retry is nearly always enough.
const ecjMergeAttempts = 5

// ecjMergeLocks serializes merges into the same journal path, so concurrent
// copies of one volume (a balance racing a rebuild) cannot both append the
// same delta.
var ecjMergeLocks = struct {
	sync.Mutex
	byPath map[string]*ecjPathLock
}{byPath: make(map[string]*ecjPathLock)}

type ecjPathLock struct {
	sync.Mutex
	refs int
}

func lockEcjPath(path string) (unlock func()) {
	ecjMergeLocks.Lock()
	l := ecjMergeLocks.byPath[path]
	if l == nil {
		l = &ecjPathLock{}
		ecjMergeLocks.byPath[path] = l
	}
	l.refs++
	ecjMergeLocks.Unlock()

	l.Lock()
	return func() {
		l.Unlock()
		ecjMergeLocks.Lock()
		if l.refs--; l.refs == 0 {
			delete(ecjMergeLocks.byPath, path)
		}
		ecjMergeLocks.Unlock()
	}
}

// ecjMergeIO is the disk I/O an unmounted merge does outside every lock,
// injectable so tests can act between the steps.
type ecjMergeIO struct {
	read func(path string) (ids map[types.NeedleId]struct{}, size int64, err error)
	sync func(*erasure_coding.EcjAppend) error
}

var defaultEcjMergeIO = ecjMergeIO{
	read: erasure_coding.ReadEcjIds,
	sync: (*erasure_coding.EcjAppend).Sync,
}

// MergeEcJournal folds a peer's deletion ids into the local journal of EC
// volume vid on the receiving disk, the one whose data directory is dataDir;
// ecjPath is that journal's path in the disk's index directory. It appends
// only the ids the journal lacks and returns how many it added.
//
// A mounted volume owns its journal: the merge goes through its open handle
// and in-memory set. That is the receiving disk's own runtime for vid,
// wherever its journal lives (it may sit in the data dir rather than
// ecjPath's index dir), else a sibling runtime journaling into ecjPath itself:
// disks sharing one index directory, or reconciliation mounting vid on a disk
// that journals into another's (#9212). Otherwise ecjPath is written while
// every disk's EC lock is held, so no mount anywhere can open it mid-append,
// and synced after they are released.
func (s *Store) MergeEcJournal(vid needle.VolumeId, dataDir, ecjPath string, ids map[types.NeedleId]struct{}) (int, error) {
	return s.mergeEcJournal(vid, dataDir, ecjPath, ids, defaultEcjMergeIO)
}

func (s *Store) mergeEcJournal(vid needle.VolumeId, dataDir, ecjPath string, ids map[types.NeedleId]struct{}, mio ecjMergeIO) (int, error) {
	unlock := lockEcjPath(ecjPath)
	defer unlock()

	var owner *DiskLocation
	for _, loc := range s.Locations {
		if filepath.Clean(loc.Directory) == filepath.Clean(dataDir) {
			owner = loc
			break
		}
	}
	if owner == nil {
		return 0, fmt.Errorf("ec volume %d: no disk at %s owns journal %s", vid, dataDir, ecjPath)
	}
	for attempt := 0; attempt < ecjMergeAttempts; attempt++ {
		if added, merged, err := s.mergeIntoMountedEcJournal(owner, vid, ecjPath, ids); merged {
			return added, err
		}
		// Read outside the locks: a bloated journal can take a while, and a
		// queued mount would otherwise stall every EC read on its disk.
		local, size, err := mio.read(ecjPath)
		if err != nil {
			return 0, fmt.Errorf("read %s: %w", ecjPath, err)
		}
		added, mounted, err := s.appendUnmountedEcJournal(owner, vid, ecjPath, local, ids, size, mio.sync)
		if mounted || errors.Is(err, erasure_coding.ErrEcjChanged) {
			continue
		}
		return added, err
	}
	return 0, fmt.Errorf("ec volume %d: journal %s kept changing during merge", vid, ecjPath)
}

// rLockEcVolumes read-locks every disk's EC volume map in location order. A
// mount registers its EcVolume, and reads its journal, under its own disk's
// write lock, so holding all of them excludes a mount on any disk. No path
// holds two disks' EC locks at once, so the fixed order cannot deadlock.
// Holders must not wait on disk I/O beyond a page-cache write: a queued mount
// on any disk stalls that disk's EC reads until they let go.
func (s *Store) rLockEcVolumes() (unlock func()) {
	for _, loc := range s.Locations {
		loc.ecVolumesLock.RLock()
	}
	return func() {
		for _, loc := range s.Locations {
			loc.ecVolumesLock.RUnlock()
		}
	}
}

// mountedEcJournal returns the runtime a merge into ecjPath on owner must go
// through and the disk it is mounted on, or nil when there is none: owner's
// own runtime for vid, else the first sibling's whose journal is ecjPath.
// Callers hold rLockEcVolumes.
func (s *Store) mountedEcJournal(owner *DiskLocation, vid needle.VolumeId, ecjPath string) (*erasure_coding.EcVolume, *DiskLocation) {
	if ev, found := owner.ecVolumes[vid]; found {
		return ev, owner
	}
	for _, loc := range s.Locations {
		if ev, found := loc.ecVolumes[vid]; found && filepath.Clean(ev.FileName(".ecj")) == filepath.Clean(ecjPath) {
			return ev, loc
		}
	}
	return nil, nil
}

// mergeIntoMountedEcJournal merges ids through the runtime mountedEcJournal
// picks. merged reports whether there was one. Only that runtime's disk stays
// locked across the merge's fsync, which keeps it mounted.
func (s *Store) mergeIntoMountedEcJournal(owner *DiskLocation, vid needle.VolumeId, ecjPath string, ids map[types.NeedleId]struct{}) (added int, merged bool, err error) {
	unlock := s.rLockEcVolumes()
	ev, mountedOn := s.mountedEcJournal(owner, vid, ecjPath)
	if ev == nil {
		unlock()
		return 0, false, nil
	}
	for _, loc := range s.Locations {
		if loc != mountedOn {
			loc.ecVolumesLock.RUnlock()
		}
	}
	defer mountedOn.ecVolumesLock.RUnlock()
	added, err = ev.MergeJournal(ids)
	return added, true, err
}

// appendUnmountedEcJournal writes the missing ids under every disk's EC lock,
// after confirming no runtime has mounted the journal since it was read, and
// syncs them once the locks are released: a slow fsync must not hold off
// mounts, and the EC reads queued behind them, on every disk. A mount after
// the write reads the new records like any others. mounted reports that a
// runtime appeared before the write; the caller then merges through it.
func (s *Store) appendUnmountedEcJournal(owner *DiskLocation, vid needle.VolumeId, ecjPath string, local, ids map[types.NeedleId]struct{}, size int64, sync func(*erasure_coding.EcjAppend) error) (added int, mounted bool, err error) {
	pending, mounted, err := s.writeUnmountedEcJournal(owner, vid, ecjPath, local, ids, size)
	if pending == nil {
		return 0, mounted, err
	}
	if err := sync(pending); err != nil {
		s.rollbackUnmountedEcJournal(vid, ecjPath, pending)
		return 0, false, err
	}
	pending.Close()
	return pending.Added, false, nil
}

func (s *Store) writeUnmountedEcJournal(owner *DiskLocation, vid needle.VolumeId, ecjPath string, local, ids map[types.NeedleId]struct{}, size int64) (pending *erasure_coding.EcjAppend, mounted bool, err error) {
	unlock := s.rLockEcVolumes()
	defer unlock()
	if ev, _ := s.mountedEcJournal(owner, vid, ecjPath); ev != nil {
		return nil, true, nil
	}
	pending, err = erasure_coding.WriteEcjIds(ecjPath, local, ids, size)
	return pending, false, err
}

// rollbackUnmountedEcJournal undoes an append whose fsync failed. A runtime
// that mounted the journal after the write has loaded those records and holds
// the file open, so the rollback goes through it: truncate and drop the ids
// from its deleted set, as its own failed journal fsync would. If it has
// journaled since, the records stay rather than lose that delete. No fsync
// runs here, so the locks are held only for in-memory work and a truncate.
func (s *Store) rollbackUnmountedEcJournal(vid needle.VolumeId, ecjPath string, pending *erasure_coding.EcjAppend) {
	unlock := s.rLockEcVolumes()
	defer unlock()
	var holders []*erasure_coding.EcVolume
	for _, loc := range s.Locations {
		if ev, found := loc.ecVolumes[vid]; found && filepath.Clean(ev.FileName(".ecj")) == filepath.Clean(ecjPath) {
			holders = append(holders, ev)
		}
	}
	if len(holders) == 0 {
		pending.Rollback()
		return
	}
	defer pending.Close()
	for _, ev := range holders {
		if !ev.UndoUnsyncedAppend(pending) {
			glog.Errorf("ec volume %d: keeping unsynced merge records in %s: journaled since the failed fsync", vid, ecjPath)
		}
	}
}
