package erasure_coding

import (
	"path/filepath"
	"sync"
)

// Process-wide coordination of everything that touches one .ecj path.
//
// Mount-time compaction replaces a deletion journal with a new inode. That is
// only safe while nothing else in this process can write the old one:
//
//   - Holders are mounted EcVolumes with a handle on the path. A store keeps one
//     EcVolume per disk location, and a shard mount or a cross-disk reconcile
//     can point one disk's volume at another disk's .ecj, so several holders of
//     one path are normal. A holder that keeps appending to a replaced inode
//     acknowledges deletes that are gone at the next mount.
//   - Writers append to or replace the path by name without holding it open
//     across calls: VolumeEcShardsCopy (CopyEcjFile), EC index recovery and
//     ReceiveFile of an EC .ecj.
//     Bytes they write after the compactor sized the journal would be dropped
//     by the rename.
//
// Compaction therefore runs only while its caller is the sole holder and no
// writer is active, and while it runs no holder may open the path and no writer
// may start. Both wait instead; a compaction rewrites only the distinct id set,
// so the wait is short.
//
// No writer active at the reservation is not enough: one that ran while the
// holder loaded the journal, or after, and has finished may have rewritten it
// in place to the same length (ReceiveFile truncates and refills), which the
// inode-and-size re-check cannot see. So each writer bumps the path's write
// generation as it starts, and a holder may compact only if no writer was
// active when it registered and the generation has not moved since.
//
// Paths are keyed by their resolved parent directory, so two disk locations
// that spell one directory differently still meet here.

type ecjPathState struct {
	holders    int
	writers    int
	compacting bool
	// writeGen counts writers that have started on the path. It survives as
	// long as the entry does, and a registered holder keeps the entry.
	writeGen uint64
}

var ecjPaths = struct {
	sync.Mutex
	changed *sync.Cond
	state   map[string]*ecjPathState
}{state: map[string]*ecjPathState{}}

func init() {
	ecjPaths.changed = sync.NewCond(&ecjPaths.Mutex)
}

// ecjPathKey resolves the parent directory of path. The file itself may not
// exist yet (a copy creates it), so only the directory is resolved.
func ecjPathKey(path string) string {
	dir, name := filepath.Split(path)
	if dir == "" {
		dir = "."
	}
	if abs, err := filepath.Abs(dir); err == nil {
		dir = abs
	}
	if resolved, err := filepath.EvalSymlinks(dir); err == nil {
		dir = resolved
	}
	return filepath.Join(dir, name)
}

// ecjUpdateWhenNotCompacting blocks until no compaction is running on key,
// then applies f to its state.
func ecjUpdateWhenNotCompacting(key string, f func(*ecjPathState)) {
	ecjPaths.Lock()
	defer ecjPaths.Unlock()
	for {
		st := ecjPaths.state[key]
		if st == nil {
			st = &ecjPathState{}
			ecjPaths.state[key] = st
		}
		if !st.compacting {
			f(st)
			return
		}
		ecjPaths.changed.Wait()
	}
}

func ecjRelease(key string, f func(*ecjPathState)) {
	ecjPaths.Lock()
	if st := ecjPaths.state[key]; st != nil {
		f(st)
		if st.holders == 0 && st.writers == 0 && !st.compacting {
			delete(ecjPaths.state, key)
		}
	}
	ecjPaths.Unlock()
	ecjPaths.changed.Broadcast()
}

// ecjHold is a mounted EcVolume's registration as a holder of its .ecj.
type ecjHold struct {
	key  string
	once sync.Once
	// The path's write generation when the hold was taken, and whether a
	// writer was active then. Taken before the journal is opened and loaded,
	// so they cover every write the load might have missed.
	writeGen      uint64
	writerAtStart bool
}

// acquireEcjHold registers a holder of ecjPath, first waiting out any
// compaction in progress so the handle opened afterwards is on the final inode.
func acquireEcjHold(ecjPath string) *ecjHold {
	h := &ecjHold{key: ecjPathKey(ecjPath)}
	ecjUpdateWhenNotCompacting(h.key, func(st *ecjPathState) {
		st.holders++
		h.writeGen = st.writeGen
		h.writerAtStart = st.writers > 0
	})
	return h
}

func (h *ecjHold) release() {
	h.once.Do(func() {
		ecjRelease(h.key, func(st *ecjPathState) {
			if st.holders > 0 {
				st.holders--
			}
		})
	})
}

// tryBeginCompaction reserves the path for a compaction, or reports false when
// another holder or an active writer could still reach the current inode, or
// when a writer has run on the path since the hold was taken, so the journal
// may no longer be what the holder loaded. The returned func ends the
// reservation.
func (h *ecjHold) tryBeginCompaction() (end func(), ok bool) {
	ecjPaths.Lock()
	defer ecjPaths.Unlock()
	st := ecjPaths.state[h.key]
	if st == nil || st.holders != 1 || st.writers != 0 || st.compacting {
		return nil, false
	}
	if h.writerAtStart || st.writeGen != h.writeGen {
		return nil, false
	}
	st.compacting = true
	var once sync.Once
	return func() {
		once.Do(func() {
			ecjRelease(h.key, func(st *ecjPathState) { st.compacting = false })
		})
	}, true
}

// BeginEcjWrite registers an out-of-band writer (shard copy, index recovery,
// ReceiveFile) of ecjPath, waiting out any compaction in progress. Compaction
// does not start until the returned func is called, so call it once the write,
// and any cleanup of a partial file, is done.
func BeginEcjWrite(ecjPath string) (done func()) {
	key := ecjPathKey(ecjPath)
	ecjUpdateWhenNotCompacting(key, func(st *ecjPathState) {
		st.writers++
		st.writeGen++
	})
	var once sync.Once
	return func() {
		once.Do(func() {
			ecjRelease(key, func(st *ecjPathState) {
				if st.writers > 0 {
					st.writers--
				}
			})
		})
	}
}
