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
// volume vid, whose on-disk path on the receiving disk is ecjPath. It appends
// only the ids the journal lacks and returns how many it added.
//
// A mounted volume owns its journal: the merge goes through its open handle
// and in-memory set, wherever that journal lives (it may sit in the data dir
// rather than ecjPath's index dir). Otherwise ecjPath is appended to while the
// disk's EC lock is held, so no mount can open it mid-append.
func (s *Store) MergeEcJournal(vid needle.VolumeId, ecjPath string, ids map[types.NeedleId]struct{}) (int, error) {
	unlock := lockEcjPath(ecjPath)
	defer unlock()

	dir := filepath.Clean(filepath.Dir(ecjPath))
	var owner *DiskLocation
	for _, loc := range s.Locations {
		if filepath.Clean(loc.IdxDirectory) == dir || filepath.Clean(loc.Directory) == dir {
			owner = loc
			continue
		}
		// Reconciliation can mount vid on a sibling disk whose runtime
		// journals into this disk's index directory (#9212).
		if added, merged, err := loc.mergeIntoMountedEcJournal(vid, ids, func(ev *erasure_coding.EcVolume) bool {
			return ev.FileName(".ecj") == ecjPath
		}); merged {
			return added, err
		}
	}
	if owner == nil {
		return 0, fmt.Errorf("ec volume %d: no disk owns journal %s", vid, ecjPath)
	}
	return owner.mergeEcJournal(vid, ecjPath, ids)
}

// mergeIntoMountedEcJournal merges ids into vid's mounted volume on this disk
// if owns accepts it. merged reports whether such a volume was found.
func (l *DiskLocation) mergeIntoMountedEcJournal(vid needle.VolumeId, ids map[types.NeedleId]struct{}, owns func(*erasure_coding.EcVolume) bool) (added int, merged bool, err error) {
	l.ecVolumesLock.RLock()
	defer l.ecVolumesLock.RUnlock()
	ev, found := l.ecVolumes[vid]
	if !found || !owns(ev) {
		return 0, false, nil
	}
	added, err = ev.MergeJournal(ids)
	return added, true, err
}

func (l *DiskLocation) mergeEcJournal(vid needle.VolumeId, ecjPath string, ids map[types.NeedleId]struct{}) (int, error) {
	anyMount := func(*erasure_coding.EcVolume) bool { return true }
	for attempt := 0; attempt < ecjMergeAttempts; attempt++ {
		if added, merged, err := l.mergeIntoMountedEcJournal(vid, ids, anyMount); merged {
			return added, err
		}
		// Read outside the lock: a bloated journal can take a while, and a
		// queued mount would otherwise stall every EC read on this disk.
		local, size, err := erasure_coding.ReadEcjIds(ecjPath)
		if err != nil {
			return 0, fmt.Errorf("read %s: %w", ecjPath, err)
		}
		added, mounted, err := l.appendUnmountedEcJournal(vid, ecjPath, local, ids, size)
		if mounted || errors.Is(err, erasure_coding.ErrEcjChanged) {
			continue
		}
		return added, err
	}
	return 0, fmt.Errorf("ec volume %d: journal %s kept changing during merge", vid, ecjPath)
}

// appendUnmountedEcJournal appends under the EC lock, which mounts take, after
// confirming vid is still unmounted here.
func (l *DiskLocation) appendUnmountedEcJournal(vid needle.VolumeId, ecjPath string, local, ids map[types.NeedleId]struct{}, size int64) (added int, mounted bool, err error) {
	l.ecVolumesLock.RLock()
	defer l.ecVolumesLock.RUnlock()
	if _, found := l.ecVolumes[vid]; found {
		return 0, true, nil
	}
	added, err = erasure_coding.AppendEcjIds(ecjPath, local, ids, size)
	return added, false, err
}
