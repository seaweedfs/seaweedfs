package storage

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"

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
// that journals into another's (#9212). Otherwise ecjPath is appended to while
// every disk's EC lock is held, so no mount anywhere can open it mid-append.
func (s *Store) MergeEcJournal(vid needle.VolumeId, dataDir, ecjPath string, ids map[types.NeedleId]struct{}) (int, error) {
	return s.mergeEcJournal(vid, dataDir, ecjPath, ids, erasure_coding.ReadEcjIds)
}

// mergeEcJournal is MergeEcJournal with the unlocked journal read injected, so
// a test can mount the volume between that read and the append.
func (s *Store) mergeEcJournal(vid needle.VolumeId, dataDir, ecjPath string, ids map[types.NeedleId]struct{}, read func(string) (map[types.NeedleId]struct{}, int64, error)) (int, error) {
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
		local, size, err := read(ecjPath)
		if err != nil {
			return 0, fmt.Errorf("read %s: %w", ecjPath, err)
		}
		added, mounted, err := s.appendUnmountedEcJournal(owner, vid, ecjPath, local, ids, size)
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
// through, or nil when there is none: owner's own runtime for vid, else the
// first sibling's whose journal is ecjPath. Callers hold rLockEcVolumes.
func (s *Store) mountedEcJournal(owner *DiskLocation, vid needle.VolumeId, ecjPath string) *erasure_coding.EcVolume {
	if ev, found := owner.ecVolumes[vid]; found {
		return ev
	}
	for _, loc := range s.Locations {
		if ev, found := loc.ecVolumes[vid]; found && filepath.Clean(ev.FileName(".ecj")) == filepath.Clean(ecjPath) {
			return ev
		}
	}
	return nil
}

// mergeIntoMountedEcJournal merges ids through the runtime mountedEcJournal
// picks. merged reports whether there was one.
func (s *Store) mergeIntoMountedEcJournal(owner *DiskLocation, vid needle.VolumeId, ecjPath string, ids map[types.NeedleId]struct{}) (added int, merged bool, err error) {
	unlock := s.rLockEcVolumes()
	defer unlock()
	ev := s.mountedEcJournal(owner, vid, ecjPath)
	if ev == nil {
		return 0, false, nil
	}
	added, err = ev.MergeJournal(ids)
	return added, true, err
}

// appendUnmountedEcJournal appends under every disk's EC lock after
// confirming no runtime has mounted the journal since it was read. mounted
// reports that one has; the caller then merges through it.
func (s *Store) appendUnmountedEcJournal(owner *DiskLocation, vid needle.VolumeId, ecjPath string, local, ids map[types.NeedleId]struct{}, size int64) (added int, mounted bool, err error) {
	unlock := s.rLockEcVolumes()
	defer unlock()
	if s.mountedEcJournal(owner, vid, ecjPath) != nil {
		return 0, true, nil
	}
	added, err = erasure_coding.AppendEcjIds(ecjPath, local, ids, size)
	return added, false, err
}
