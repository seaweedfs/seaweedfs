package filer

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// Replay applies the delete and the insert as separate, non-transactional
// store calls, so retrying it after a partial failure is not atomic; both
// operations are idempotent and the timestamp check tolerates re-delivery.
//
// A replayed event can be older than local state: a peer resuming from a
// stale offset replays everything since the offset, and two writes through
// different filers race within the replication lag. Apply each path's change
// only when no newer local change is recorded, so a late event cannot revert
// or resurrect entries.
//
// Ordering uses the per-path commit timestamp recorded for every emitted
// mutation (meta_path_ts.go), which survives deletes as a tombstone and is
// not client-controllable the way entry mtime is — a mounted client can set
// mtime into the future, and a rename keeps the source file's mtime on the
// destination entry. Entries written before recording existed fall back to
// their mtime.
//
// The check and the mutation run under the same per-path entry lock the
// filer server's local writes hold, so they cannot be split by a concurrent
// local update or by another peer's replay.
func (f *Filer) Replay(resp *filer_pb.SubscribeMetadataResponse) error {
	message := resp.EventNotification
	dir := resp.Directory
	if message.NewParentPath != "" {
		dir = message.NewParentPath
	}
	var oldPath, newPath util.FullPath
	if message.OldEntry != nil {
		oldPath = util.NewFullPath(resp.Directory, message.OldEntry.Name)
	}
	if message.NewEntry != nil {
		newPath = util.NewFullPath(dir, message.NewEntry.Name)
	}
	ctx := context.Background()

	unlock := f.lockReplayPaths(oldPath, newPath)
	defer unlock()

	samePath := oldPath != "" && oldPath == newPath
	if samePath {
		newer, err := f.replayBlockedByLocal(ctx, oldPath, resp.TsNs)
		if err != nil || newer {
			return err
		}
		glog.V(4).Infof("deleting %v", oldPath)
		if err := f.Store.DeleteEntry(ctx, oldPath); err != nil {
			return err
		}
		// Record the commit before the insert too: if the insert fails and
		// the event is retried later, the path sits deleted in between —
		// without the record a stale peer create could resurrect it in the
		// gap. The entry still exists afterward, so no delete record.
		f.notePathChange(oldPath, resp.TsNs, false)
		glog.V(4).Infof("creating %v", newPath)
		newEntry := FromPbEntry(dir, message.NewEntry)
		if err := f.Store.InsertEntry(ctx, newEntry); err != nil {
			return err
		}
		f.notePathChange(oldPath, resp.TsNs, false)
		return nil
	}

	if message.OldEntry != nil {
		newer, err := f.replayBlockedByLocal(ctx, oldPath, resp.TsNs)
		if err != nil {
			return err
		}
		if !newer {
			// A directory delete on the origin removed the whole subtree, but
			// only the directory's own key replicates. Sweep local children the
			// origin never listed, or they orphan under a missing parent.
			keepDir := false
			if message.OldEntry.IsDirectory {
				var err error
				keepDir, err = f.deleteReplayedFolderChildren(ctx, oldPath, message.DeleteChunks, resp.TsNs)
				if err != nil {
					return err
				}
			}
			if !keepDir {
				glog.V(4).Infof("deleting %v", oldPath)
				if err := f.Store.DeleteEntry(ctx, oldPath); err != nil {
					return err
				}
				f.notePathChange(oldPath, resp.TsNs, true)
			}
		}
	}

	if message.NewEntry != nil {
		newer, err := f.replayBlockedByLocal(ctx, newPath, resp.TsNs)
		if err != nil {
			return err
		}
		if !newer {
			glog.V(4).Infof("creating %v", newPath)
			newEntry := FromPbEntry(dir, message.NewEntry)
			if err := f.Store.InsertEntry(ctx, newEntry); err != nil {
				return err
			}
			f.notePathChange(newPath, resp.TsNs, false)
		}
	}

	return nil
}

// deleteReplayedFolderChildren removes local entries under dir, and queues
// the chunks of removed files for deletion. Children this filer committed
// after the event stay — a newer local write wins over a delayed delete —
// along with the directories that hold them.
func (f *Filer) deleteReplayedFolderChildren(ctx context.Context, dir util.FullPath, deleteChunks bool, tsNs int64) (bool, error) {
	chunksByPath := make(map[util.FullPath][]*filer_pb.FileChunk)
	kept, err := f.sweepReplayedFolder(ctx, dir, deleteChunks, tsNs, chunksByPath)
	if err != nil {
		return false, err
	}
	for path, chunks := range chunksByPath {
		f.DeleteChunks(ctx, path, chunks)
	}
	return kept, nil
}

// sweepReplayedFolder deletes every child of dir whose local commit is not
// newer than the event, working subtree-first so nested entries go too.
// It reports whether a newer descendant keeps dir alive — the caller then
// keeps the dir entry itself.
func (f *Filer) sweepReplayedFolder(ctx context.Context, dir util.FullPath, deleteChunks bool, tsNs int64, chunks map[util.FullPath][]*filer_pb.FileChunk) (kept bool, err error) {
	lastFileName := ""
	for {
		entries, _, lerr := f.ListDirectoryEntries(ctx, dir, lastFileName, false, PaginationSize, "", "", "")
		if lerr != nil {
			return false, fmt.Errorf("list folder %s: %w", dir, lerr)
		}
		for _, sub := range entries {
			lastFileName = sub.Name()
			childKept := false
			if tsNs > 0 {
				localTsNs, tsErr := f.localCommitTsNs(ctx, sub.FullPath)
				if tsErr != nil {
					return false, tsErr
				}
				childKept = localTsNs > tsNs
			}
			if !childKept && sub.IsDirectory() {
				childKept, err = f.sweepReplayedFolder(ctx, sub.FullPath, deleteChunks, tsNs, chunks)
				if err != nil {
					return false, err
				}
			}
			if childKept {
				kept = true
				continue
			}
			// hard-linked chunks are owned by the shared inode record; the
			// origin's own events decide their deletion. A directory entry
			// can still carry chunks of its own — an S3 child promotes an
			// existing file to a directory without dropping them — so the
			// entry's chunks are keyed to its own path whatever it is.
			if deleteChunks && len(sub.HardLinkId) == 0 && len(sub.GetChunks()) > 0 {
				chunks[sub.FullPath] = append(chunks[sub.FullPath], sub.GetChunks()...)
			}
			if derr := f.Store.DeleteEntry(ctx, sub.FullPath); derr != nil {
				return false, fmt.Errorf("delete %s: %w", sub.FullPath, derr)
			}
			f.notePathChange(sub.FullPath, tsNs, true)
		}
		if len(entries) < PaginationSize {
			break
		}
	}
	return kept, nil
}

// replayBlockedByLocal reports whether the local store already holds a
// version of path committed after the event's timestamp. Lookup failures are
// propagated: a replayed change must not be applied on the guess that the
// path was simply absent, or a transient read error could let a stale event
// overwrite a newer entry past the point of no return.
func (f *Filer) replayBlockedByLocal(ctx context.Context, path util.FullPath, tsNs int64) (bool, error) {
	if tsNs <= 0 {
		return false, nil
	}
	localTsNs, err := f.localCommitTsNs(ctx, path)
	if err != nil {
		return false, err
	}
	return localTsNs > tsNs, nil
}

// lockReplayPaths holds the shared entry locks for every path the event
// touches, in the same sorted order the rename handler takes them.
// lockReplayPaths takes the per-path entry locks for the event's paths and
// every ancestor of them, in lexicographic order — parents sort before
// children, the same ordering the local rename handler's two-path lock uses,
// so lock acquisition is deadlock-free across operations. Ancestors are
// included because the replay check consults ancestor delete records: a
// local recursive delete or directory rename holds only the ancestor's lock,
// and without contending on it a replay could reinsert a stale child after
// its check. Ancestors are locked shared — they only need to exclude an
// exclusive holder, not each other, or every replay under a busy parent
// would serialize — while a path the event actually changes stays exclusive
// even when it is also the other path's ancestor. The root is never locked:
// it cannot be deleted, and a shared root lock would still be excluded by
// every top-level delete.
func (f *Filer) lockReplayPaths(oldPath, newPath util.FullPath) func() {
	lt := f.EntryLockTable
	if lt == nil {
		return func() {}
	}
	var paths []util.FullPath
	addAncestors := func(p util.FullPath) {
		for i := strings.LastIndexByte(string(p), '/'); i > 0; i = strings.LastIndexByte(string(p), '/') {
			p = p[:i]
			paths = append(paths, p)
		}
	}
	addAncestors(oldPath)
	addAncestors(newPath)
	if oldPath != "" {
		paths = append(paths, oldPath)
	}
	if newPath != "" {
		paths = append(paths, newPath)
	}
	if len(paths) == 0 {
		return func() {}
	}
	slices.Sort(paths)
	paths = slices.Compact(paths)
	locks := make([]*util.ActiveLock, len(paths))
	for i, p := range paths {
		lockType := util.SharedLock
		if p == oldPath || p == newPath {
			lockType = util.ExclusiveLock
		}
		locks[i] = lt.AcquireLock("MetaReplay", p, lockType)
	}
	return func() {
		for i := len(paths) - 1; i >= 0; i-- {
			lt.ReleaseLock(paths[i], locks[i])
		}
	}
}
