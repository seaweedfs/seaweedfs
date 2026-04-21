package weed_server

import (
	"path"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// mutateJobKind classifies a streamed mutation for conflict detection. This is
// the same taxonomy MetadataProcessor uses in weed/command/filer_sync_jobs.go:
// separate a "barrier" directory event (create/delete/rename) from an in-place
// attribute-only directory update so that mtime/xattr bumps do not serialize
// every file operation in the subtree.
type mutateJobKind int

const (
	// kindMutateFile: a regular file event, or a non-recursive delete whose
	// target type is unknown. Conflicts only with same-path file jobs and with
	// any barrier directory on the same path or on an ancestor.
	kindMutateFile mutateJobKind = iota
	// kindMutateBarrierDir: a directory create, a directory rename, or a
	// recursive delete. Acts as a subtree barrier: must drain every active
	// descendant, and blocks every new job under it until it completes.
	kindMutateBarrierDir
	// kindMutateNonBarrierDir: an in-place directory attribute update
	// (mode / xattr / mtime with unchanged name). Conflicts at the same path
	// with barrier dirs, but never with descendants.
	kindMutateNonBarrierDir
)

// mutateScheduler serializes mutations by path while allowing cross-path work
// to run in parallel. It is a direct adaptation of filer.sync's
// MetadataProcessor (weed/command/filer_sync_jobs.go):
//
//   - the same four indexes (activeFilePaths, activeBarrierDirPaths,
//     activeNonBarrierDirPaths, descendantCount)
//   - the same conflict taxonomy (pathConflicts)
//   - the same cond.Wait admission loop bounded by concurrencyLimit
//
// It differs in three small ways: it takes already-extracted (path, kind)
// instead of a SubscribeMetadataResponse; it has no watermark/heap because
// mutations are not timestamp-ordered events; and Admit/Done are split so the
// caller can run the mutation in its own goroutine.
type mutateScheduler struct {
	concurrencyLimit int

	mu   sync.Mutex
	cond *sync.Cond

	totalActive              int
	activeFilePaths          map[util.FullPath]int
	activeBarrierDirPaths    map[util.FullPath]int
	activeNonBarrierDirPaths map[util.FullPath]int
	descendantCount          map[util.FullPath]int
}

func newMutateScheduler(concurrency int) *mutateScheduler {
	s := &mutateScheduler{
		concurrencyLimit:         concurrency,
		activeFilePaths:          make(map[util.FullPath]int),
		activeBarrierDirPaths:    make(map[util.FullPath]int),
		activeNonBarrierDirPaths: make(map[util.FullPath]int),
		descendantCount:          make(map[util.FullPath]int),
	}
	s.cond = sync.NewCond(&s.mu)
	return s
}

// Admit blocks until this (primary, secondary, kind) tuple can be admitted
// without violating the conflict rules and without exceeding concurrencyLimit.
// On return it has registered the job in the indexes; the caller must call
// Done with the same arguments when the work is finished.
//
// For single-path operations (create / update / delete) pass secondary="".
// For rename, pass old path as primary and new path as secondary; kind is
// kindMutateBarrierDir for directory renames, kindMutateFile for file renames.
func (s *mutateScheduler) Admit(primary, secondary util.FullPath, kind mutateJobKind) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for s.totalActive >= s.concurrencyLimit ||
		s.pathConflictsLocked(primary, kind) ||
		(secondary != "" && s.pathConflictsLocked(secondary, kind)) {
		s.cond.Wait()
	}

	s.addPathLocked(primary, kind)
	if secondary != "" {
		s.addPathLocked(secondary, kind)
	}
	s.totalActive++
}

// Done releases the slot reserved by Admit and wakes any waiters whose
// conflicts may have cleared. Must be called exactly once per successful
// Admit with the same arguments.
func (s *mutateScheduler) Done(primary, secondary util.FullPath, kind mutateJobKind) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.removePathLocked(primary, kind)
	if secondary != "" {
		s.removePathLocked(secondary, kind)
	}
	s.totalActive--
	s.cond.Broadcast()
}

// pathConflictsLocked mirrors MetadataProcessor.pathConflicts exactly.
func (s *mutateScheduler) pathConflictsLocked(p util.FullPath, kind mutateJobKind) bool {
	if s.activeBarrierDirPaths[p] > 0 {
		return true
	}
	if kind == kindMutateBarrierDir && s.activeNonBarrierDirPaths[p] > 0 {
		return true
	}
	if s.activeFilePaths[p] > 0 && (kind == kindMutateFile || kind == kindMutateBarrierDir) {
		return true
	}
	if kind == kindMutateBarrierDir && s.descendantCount[p] > 0 {
		return true
	}
	for _, ancestor := range mutatePathAncestors(p) {
		if s.activeBarrierDirPaths[ancestor] > 0 {
			return true
		}
	}
	return false
}

func (s *mutateScheduler) addPathLocked(p util.FullPath, kind mutateJobKind) {
	switch kind {
	case kindMutateFile:
		s.activeFilePaths[p]++
	case kindMutateBarrierDir:
		s.activeBarrierDirPaths[p]++
	case kindMutateNonBarrierDir:
		s.activeNonBarrierDirPaths[p]++
	}
	for _, ancestor := range mutatePathAncestors(p) {
		s.descendantCount[ancestor]++
	}
}

func (s *mutateScheduler) removePathLocked(p util.FullPath, kind mutateJobKind) {
	switch kind {
	case kindMutateFile:
		if s.activeFilePaths[p] <= 1 {
			delete(s.activeFilePaths, p)
		} else {
			s.activeFilePaths[p]--
		}
	case kindMutateBarrierDir:
		if s.activeBarrierDirPaths[p] <= 1 {
			delete(s.activeBarrierDirPaths, p)
		} else {
			s.activeBarrierDirPaths[p]--
		}
	case kindMutateNonBarrierDir:
		if s.activeNonBarrierDirPaths[p] <= 1 {
			delete(s.activeNonBarrierDirPaths, p)
		} else {
			s.activeNonBarrierDirPaths[p]--
		}
	}
	for _, ancestor := range mutatePathAncestors(p) {
		if s.descendantCount[ancestor] <= 1 {
			delete(s.descendantCount, ancestor)
		} else {
			s.descendantCount[ancestor]--
		}
	}
}

// mutatePathAncestors mirrors filer_sync_jobs.go:pathAncestors — returns the
// proper ancestor directories of p. For "/a/b/c", returns ["/a/b", "/a", "/"].
func mutatePathAncestors(p util.FullPath) []util.FullPath {
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

// classifyMutation extracts the admission key(s) and kind for a mutation
// request. It returns primary, secondary (empty unless rename), and kind.
//
// Malformed requests (missing oneof payload, nil Entry, missing rename fields)
// are routed to a barrier at "/" so admission still runs under the full stream
// lock; the handler will then send EINVAL to the client. This keeps the
// scheduler and Recv loop crash-free regardless of client-side validation.
//
// Deletes are always classified as kindMutateBarrierDir because a
// DeleteEntryRequest can target a directory (empty or, with IsRecursive, with
// contents) but does not carry IsDirectory on the wire. Treating every delete
// as a barrier at its target path makes it conflict with an in-flight
// non-barrier directory update on the same path (e.g. chmod), which a
// kindMutateFile classification would miss.
func classifyMutation(req *filer_pb.StreamMutateEntryRequest) (primary, secondary util.FullPath, kind mutateJobKind) {
	// Default fallback for any shape we cannot classify safely.
	primary = util.FullPath("/")
	kind = kindMutateBarrierDir

	switch r := req.Request.(type) {

	case *filer_pb.StreamMutateEntryRequest_CreateRequest:
		cr := r.CreateRequest
		if cr == nil || cr.Entry == nil {
			return
		}
		primary = util.FullPath(cr.Directory).Child(cr.Entry.Name)
		kind = classifyEntry(cr.Entry.IsDirectory, false)
		return

	case *filer_pb.StreamMutateEntryRequest_UpdateRequest:
		ur := r.UpdateRequest
		if ur == nil || ur.Entry == nil {
			return
		}
		primary = util.FullPath(ur.Directory).Child(ur.Entry.Name)
		// UpdateEntry never changes the name, so directory updates are always
		// in-place attribute updates. File updates (chunk manifests, xattrs)
		// are kindMutateFile and thus serialize against same-path file ops.
		kind = classifyEntry(ur.Entry.IsDirectory, true)
		return

	case *filer_pb.StreamMutateEntryRequest_DeleteRequest:
		dr := r.DeleteRequest
		if dr == nil {
			return
		}
		primary = util.FullPath(dr.Directory).Child(dr.Name)
		// Barrier regardless of IsRecursive: the request does not carry the
		// target's IsDirectory, and barrier classification correctly blocks
		// concurrent non-barrier dir updates at the same path. Descendant
		// wait for a non-recursive delete of a non-empty dir is wasted but
		// not wrong — that call fails at the store anyway.
		kind = kindMutateBarrierDir
		return

	case *filer_pb.StreamMutateEntryRequest_RenameRequest:
		rr := r.RenameRequest
		if rr == nil {
			return
		}
		primary = util.FullPath(rr.OldDirectory).Child(rr.OldName)
		secondary = util.FullPath(rr.NewDirectory).Child(rr.NewName)
		// Renames reshape the namespace on both sides; conservatively treat as
		// a subtree barrier so any in-flight descendant drains before the move
		// and no new descendant is admitted until it completes.
		kind = kindMutateBarrierDir
		return

	default:
		return
	}
}

func classifyEntry(isDirectory, isAttributeUpdate bool) mutateJobKind {
	if !isDirectory {
		return kindMutateFile
	}
	if isAttributeUpdate {
		return kindMutateNonBarrierDir
	}
	return kindMutateBarrierDir
}
