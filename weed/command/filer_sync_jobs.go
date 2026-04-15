package command

import (
	"container/heap"
	"path"
	"sync"
	"sync/atomic"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// tsMinHeap implements heap.Interface for int64 timestamps.
type tsMinHeap []int64

func (h tsMinHeap) Len() int           { return len(h) }
func (h tsMinHeap) Less(i, j int) bool { return h[i] < h[j] }
func (h tsMinHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *tsMinHeap) Push(x any)        { *h = append(*h, x.(int64)) }
func (h *tsMinHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// jobKind classifies a sync job for conflict detection. Directory events are
// split into "barrier" (create/delete/rename) and "non-barrier" (in-place
// attribute update) so that attribute-only directory updates — which do not
// reshape the namespace — no longer serialize every file operation in the
// subtree.
type jobKind int

const (
	// kindFile is a regular file event.
	kindFile jobKind = iota
	// kindBarrierDir is a directory create, delete, or rename. It acts as a
	// subtree barrier: it waits for all active descendants to drain, and it
	// blocks every event under it from being admitted until it completes.
	kindBarrierDir
	// kindNonBarrierDir is a directory attribute update (mtime/xattr/chmod
	// with the same parent and name). It does not block descendants and is
	// not blocked by ancestor directories, but it still bumps the ancestor
	// descendant counters so an incoming barrier dir on an ancestor path
	// still waits for it to drain.
	kindNonBarrierDir
)

type syncJobPaths struct {
	path    util.FullPath
	newPath util.FullPath // empty for non-renames
	kind    jobKind
}

type MetadataProcessor struct {
	activeJobs           map[int64]*syncJobPaths
	activeJobsLock       sync.Mutex
	activeJobsCond       *sync.Cond
	concurrencyLimit     int
	fn                   pb.ProcessMetadataFunc
	processedTsWatermark atomic.Int64

	// Indexes for O(depth) conflict detection, replacing O(n) linear scan.
	// activeFilePaths counts active file jobs at each exact path.
	activeFilePaths map[util.FullPath]int
	// activeBarrierDirPaths counts active barrier-dir jobs at each exact
	// path. Only barrier dirs are tracked here; non-barrier dir updates are
	// deliberately invisible to the ancestor check so that they don't
	// serialize every file descendant.
	activeBarrierDirPaths map[util.FullPath]int
	// descendantCount counts active jobs (of any kind) strictly under each
	// directory. Read by incoming barrier dirs so they wait for their whole
	// subtree to drain before running, regardless of descendant kind.
	descendantCount map[util.FullPath]int

	// tsHeap is a min-heap of active job timestamps with lazy deletion,
	// used for O(log n) amortized watermark tracking.
	tsHeap tsMinHeap
}

func NewMetadataProcessor(fn pb.ProcessMetadataFunc, concurrency int, offsetTsNs int64) *MetadataProcessor {
	t := &MetadataProcessor{
		fn:                    fn,
		activeJobs:            make(map[int64]*syncJobPaths),
		concurrencyLimit:      concurrency,
		activeFilePaths:       make(map[util.FullPath]int),
		activeBarrierDirPaths: make(map[util.FullPath]int),
		descendantCount:       make(map[util.FullPath]int),
	}
	t.processedTsWatermark.Store(offsetTsNs)
	t.activeJobsCond = sync.NewCond(&t.activeJobsLock)
	return t
}

// pathAncestors returns all proper ancestor directories of p.
// For "/a/b/c", returns ["/a/b", "/a", "/"].
func pathAncestors(p util.FullPath) []util.FullPath {
	var ancestors []util.FullPath
	s := string(p)
	for {
		parent := path.Dir(s)
		if parent == s {
			break
		}
		ancestors = append(ancestors, util.FullPath(parent))
		s = parent
	}
	return ancestors
}

// addPathToIndex registers a path in the conflict detection indexes.
// Must be called under activeJobsLock.
func (t *MetadataProcessor) addPathToIndex(p util.FullPath, kind jobKind) {
	switch kind {
	case kindFile:
		t.activeFilePaths[p]++
	case kindBarrierDir:
		t.activeBarrierDirPaths[p]++
	case kindNonBarrierDir:
		// Not tracked in any exact-path index: attribute-only dir updates
		// never cause ancestor blocking. They only contribute to
		// descendantCount below so barrier ancestors still wait for them.
	}
	for _, ancestor := range pathAncestors(p) {
		t.descendantCount[ancestor]++
	}
}

// removePathFromIndex unregisters a path from the conflict detection indexes.
// Must be called under activeJobsLock.
func (t *MetadataProcessor) removePathFromIndex(p util.FullPath, kind jobKind) {
	switch kind {
	case kindFile:
		if t.activeFilePaths[p] <= 1 {
			delete(t.activeFilePaths, p)
		} else {
			t.activeFilePaths[p]--
		}
	case kindBarrierDir:
		if t.activeBarrierDirPaths[p] <= 1 {
			delete(t.activeBarrierDirPaths, p)
		} else {
			t.activeBarrierDirPaths[p]--
		}
	case kindNonBarrierDir:
		// Mirrors addPathToIndex: nothing to undo at the exact path.
	}
	for _, ancestor := range pathAncestors(p) {
		if t.descendantCount[ancestor] <= 1 {
			delete(t.descendantCount, ancestor)
		} else {
			t.descendantCount[ancestor]--
		}
	}
}

// pathConflicts checks if a single path conflicts with any active job.
// Conflict rules:
//   - any kind vs same-path barrier dir: wait (a create/delete/rename on p
//     must fully serialize against any other operation touching p, including
//     non-barrier attribute updates and files at the same path)
//   - file vs same-path file: wait
//   - file vs same-path barrier dir: wait (covered by the barrier-at-p check
//     above; also serializes a file-to-dir / dir-to-file promotion)
//   - barrier dir vs same-path file: wait
//   - barrier dir vs any descendant (file or dir, barrier or not): wait
//   - barrier ancestor: always wait, regardless of incoming kind
//   - non-barrier dir vs descendants: never conflicts
//   - non-barrier dir vs same-path non-barrier dir: never conflicts (attribute
//     bumps are "last writer wins"; this intentionally lets rapid mtime /
//     xattr updates overlap)
func (t *MetadataProcessor) pathConflicts(p util.FullPath, kind jobKind) bool {
	// A barrier dir in flight at p serializes every new job at p. This is the
	// strictest same-path rule and applies regardless of incoming kind.
	if t.activeBarrierDirPaths[p] > 0 {
		return true
	}
	// A file in flight at p blocks new file or barrier-dir jobs at p. A
	// non-barrier dir update at p is allowed through — by construction files
	// and dirs at the same path only coexist across a promotion, which is a
	// barrier event handled by the check above.
	if t.activeFilePaths[p] > 0 && (kind == kindFile || kind == kindBarrierDir) {
		return true
	}
	// Barrier dirs additionally wait for their whole in-flight subtree.
	if kind == kindBarrierDir && t.descendantCount[p] > 0 {
		return true
	}
	// Any barrier dir on a proper ancestor blocks everything under it.
	for _, ancestor := range pathAncestors(p) {
		if t.activeBarrierDirPaths[ancestor] > 0 {
			return true
		}
	}
	return false
}

func (t *MetadataProcessor) conflictsWith(resp *filer_pb.SubscribeMetadataResponse) bool {
	p, newPath, kind := extractJobInfo(resp)
	if t.pathConflicts(p, kind) {
		return true
	}
	if newPath != "" && t.pathConflicts(newPath, kind) {
		return true
	}
	return false
}

func (t *MetadataProcessor) AddSyncJob(resp *filer_pb.SubscribeMetadataResponse) {
	if filer_pb.IsEmpty(resp) {
		return
	}

	t.activeJobsLock.Lock()
	defer t.activeJobsLock.Unlock()

	for len(t.activeJobs) >= t.concurrencyLimit || t.conflictsWith(resp) {
		t.activeJobsCond.Wait()
	}

	p, newPath, kind := extractJobInfo(resp)
	jobPaths := &syncJobPaths{path: p, newPath: newPath, kind: kind}

	t.activeJobs[resp.TsNs] = jobPaths
	t.addPathToIndex(p, kind)
	if newPath != "" {
		t.addPathToIndex(newPath, kind)
	}

	heap.Push(&t.tsHeap, resp.TsNs)

	go func() {

		if err := util.Retry("metadata processor", func() error {
			return t.fn(resp)
		}); err != nil {
			glog.Errorf("process %v: %v", resp, err)
		}

		t.activeJobsLock.Lock()
		defer t.activeJobsLock.Unlock()

		delete(t.activeJobs, resp.TsNs)
		t.removePathFromIndex(jobPaths.path, jobPaths.kind)
		if jobPaths.newPath != "" {
			t.removePathFromIndex(jobPaths.newPath, jobPaths.kind)
		}

		// Lazy-clean stale entries from heap top (already-completed jobs).
		// Each entry is pushed once and popped once: O(log n) amortized.
		for t.tsHeap.Len() > 0 {
			if _, active := t.activeJobs[t.tsHeap[0]]; active {
				break
			}
			heap.Pop(&t.tsHeap)
		}
		// If this was the oldest job, advance the watermark.
		if t.tsHeap.Len() == 0 || resp.TsNs < t.tsHeap[0] {
			t.processedTsWatermark.Store(resp.TsNs)
		}
		t.activeJobsCond.Signal()
	}()
}

// extractJobInfo derives the conflict-detection path(s) and job kind for a
// metadata event. A rename returns both the source and destination paths; all
// other event shapes return only the primary path.
func extractJobInfo(resp *filer_pb.SubscribeMetadataResponse) (p, newPath util.FullPath, kind jobKind) {
	oldEntry := resp.EventNotification.OldEntry
	newEntry := resp.EventNotification.NewEntry
	// create
	if filer_pb.IsCreate(resp) {
		p = util.FullPath(resp.Directory).Child(newEntry.Name)
		kind = classifyDirEvent(newEntry.IsDirectory, false)
		return
	}
	if filer_pb.IsDelete(resp) {
		p = util.FullPath(resp.Directory).Child(oldEntry.Name)
		kind = classifyDirEvent(oldEntry.IsDirectory, false)
		return
	}
	if filer_pb.IsUpdate(resp) {
		p = util.FullPath(resp.Directory).Child(newEntry.Name)
		// In-place attribute update: non-barrier when the entry is a dir.
		kind = classifyDirEvent(newEntry.IsDirectory, true)
		return
	}
	// renaming: the namespace is reshaped on both sides, so a directory
	// rename is a barrier on both source and destination.
	p = util.FullPath(resp.Directory).Child(oldEntry.Name)
	newPath = util.FullPath(resp.EventNotification.NewParentPath).Child(newEntry.Name)
	kind = classifyDirEvent(oldEntry.IsDirectory, false)
	return
}

// classifyDirEvent maps an entry's (isDirectory, isAttributeUpdate) pair to a
// jobKind. Attribute-only updates on directories are the only non-barrier
// case; everything else on a directory (create/delete/rename) is a barrier,
// and everything on a file is kindFile.
func classifyDirEvent(isDirectory, isAttributeUpdate bool) jobKind {
	if !isDirectory {
		return kindFile
	}
	if isAttributeUpdate {
		return kindNonBarrierDir
	}
	return kindBarrierDir
}
