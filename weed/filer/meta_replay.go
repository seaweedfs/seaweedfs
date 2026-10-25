package filer

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/protobuf/proto"
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
		newEntry := FromPbEntry(dir, message.NewEntry)
		newEntry.CommitTsNs = resp.TsNs
		if message.OldEntry.IsDirectory {
			glog.V(4).Infof("updating %v", newPath)
			return f.updateExistingEntry(ctx, oldPath, newEntry)
		}
		glog.V(4).Infof("deleting %v", oldPath)
		if err := f.Store.DeleteEntry(ctx, oldPath); err != nil {
			return err
		}
		glog.V(4).Infof("creating %v", newPath)
		if err := f.Store.InsertEntry(ctx, newEntry); err != nil {
			return err
		}
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
				keepDir, err = f.deleteReplayedFolderChildren(ctx, oldPath, message, resp.TsNs)
				if err != nil {
					return err
				}
			}
			if !keepDir {
				glog.V(4).Infof("deleting %v", oldPath)
				if err := f.Store.DeleteEntry(ctx, oldPath); err != nil {
					return err
				}
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
			newEntry.CommitTsNs = resp.TsNs
			if err := f.Store.InsertEntry(ctx, newEntry); err != nil {
				return err
			}
		}
	}

	return nil
}

// updateExistingEntry applies a same-path update without deleting the row in
// between: the path never sits absent, so a failed retry cannot leave it
// unordered — a tombstone would instead read as a subtree delete that
// suppresses a directory's children. UpdateEntry goes on a present row and
// InsertEntry on an absent one (InsertEntry is not an upsert on stores
// configured without upsert support, and UpdateEntry touches no row that is
// not there).
//
// A peer filer sharing the store can still delete the row between the find
// and the update — the UPDATE then changes zero rows yet reports success,
// silently dropping the replay while its subscription offset advances. The
// write is verified by re-reading the entry, and each pass re-checks the
// recorded commit order so a newer peer change wins instead of being
// resurrected over.
func (f *Filer) updateExistingEntry(ctx context.Context, path util.FullPath, newEntry *Entry) error {
	var lastErr error
	for attempt := 0; attempt < 4; attempt++ {
		if attempt > 0 {
			newer, err := f.replayBlockedByLocal(ctx, path, newEntry.CommitTsNs)
			if err != nil {
				return err
			}
			if newer {
				return nil
			}
		}
		if _, findErr := f.Store.FindEntry(ctx, path); findErr == nil {
			if err := f.Store.UpdateEntry(ctx, newEntry); err != nil {
				return err
			}
			if _, err := f.Store.FindEntry(ctx, path); err == nil {
				return nil
			} else if !errors.Is(err, filer_pb.ErrNotFound) {
				return err
			} else {
				lastErr = err
			}
		} else if errors.Is(findErr, filer_pb.ErrNotFound) {
			// An insert colliding with a racing peer create retries as an
			// update on the next pass.
			if err := f.Store.InsertEntry(ctx, newEntry); err != nil {
				lastErr = err
			} else {
				return nil
			}
		} else {
			return findErr
		}
	}
	return fmt.Errorf("update %s: %w", path, lastErr)
}

// deleteReplayedFolderChildren removes local entries under dir, and queues
// the chunks of removed files for deletion. Children this filer committed
// after the event stay — a newer local write wins over a delayed delete —
// along with the directories that hold them.
func (f *Filer) deleteReplayedFolderChildren(ctx context.Context, dir util.FullPath, message *filer_pb.EventNotification, tsNs int64) (bool, error) {
	sweep := &folderSweep{chunksByPath: make(map[util.FullPath][]*filer_pb.FileChunk)}
	kept, err := f.sweepReplayedFolder(ctx, dir, message, tsNs, sweep)
	// Flush what was collected even on error: the entries already deleted
	// cannot be listed again on retry, so their chunks must be queued now.
	sweep.flush(ctx, f)
	if err != nil {
		return false, err
	}
	return kept, nil
}

// folderSweep accumulates the chunks of removed children for deletion. It
// flushes in batches so a large subtree cannot hold every chunk reference in
// memory at once, and so chunk queueing overlaps the listing work.
type folderSweep struct {
	chunksByPath map[util.FullPath][]*filer_pb.FileChunk
	count        int
}

const folderSweepChunkBatch = 4096

func (s *folderSweep) add(path util.FullPath, chunks []*filer_pb.FileChunk) {
	s.chunksByPath[path] = append(s.chunksByPath[path], chunks...)
	s.count += len(chunks)
}

func (s *folderSweep) flush(ctx context.Context, f *Filer) {
	for path, chunks := range s.chunksByPath {
		f.DeleteChunks(ctx, path, chunks)
	}
	s.chunksByPath = make(map[util.FullPath][]*filer_pb.FileChunk)
	s.count = 0
}

// sweepReplayedFolder deletes every child of dir whose local commit is not
// newer than the event, working subtree-first so nested entries go too.
// It reports whether a newer descendant keeps dir alive — the caller then
// keeps the dir entry itself.
func (f *Filer) sweepReplayedFolder(ctx context.Context, dir util.FullPath, message *filer_pb.EventNotification, tsNs int64, sweep *folderSweep) (kept bool, err error) {
	keepRemote := IsMetadataOnlyDelete(message.OldEntry)
	deleteRemote := !keepRemote && !message.GetIsFromOtherCluster()
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
				childKept, err = f.sweepReplayedFolder(ctx, sub.FullPath, message, tsNs, sweep)
				if err != nil {
					return false, err
				}
			}
			if childKept {
				kept = true
				continue
			}
			childKept, err = f.deleteSweptChild(ctx, sub, message, tsNs, sweep, keepRemote, deleteRemote)
			if err != nil {
				return false, err
			}
			if childKept {
				kept = true
			}
		}
		if len(entries) < PaginationSize {
			break
		}
	}
	return kept, nil
}

// deleteSweptChild removes sub under its entry lock and reports whether the
// child was kept instead. The listing and the first commit-time check run
// without the lock, so a local write can land in between: the check runs
// again under the lock before anything is removed. A remote-only entry that
// has taken the path since — one the commit record may not cover, such as a
// lazy fetch materialized from the remote — reads the object at the key as
// its only copy: the write-back daemon's processDeleteEvent keeps such an
// object, and the sweep does too.
func (f *Filer) deleteSweptChild(ctx context.Context, sub *Entry, message *filer_pb.EventNotification, tsNs int64, sweep *folderSweep, keepRemote, deleteRemote bool) (bool, error) {
	var childLock *util.ActiveLock
	if lt := f.EntryLockTable; lt != nil {
		// The sweep already holds locks on the event's paths; waiting for a
		// descendant here can deadlock against a local rename that holds the
		// child and waits for one of them. ErrLockContended is transient, so
		// the replication retry picks the event up once the write settles.
		var acquired bool
		childLock, acquired = lt.TryAcquireLock("MetaReplay", sub.FullPath, util.ExclusiveLock)
		if !acquired {
			return false, fmt.Errorf("sweep %s: %w", sub.FullPath, util.ErrLockContended)
		}
		defer lt.ReleaseLock(sub.FullPath, childLock)
	}
	if tsNs > 0 {
		localTsNs, tsErr := f.localCommitTsNs(ctx, sub.FullPath)
		if tsErr != nil {
			return false, tsErr
		}
		if localTsNs > tsNs {
			return true, nil
		}
	}
	current, findErr := f.Store.FindEntry(ctx, sub.FullPath)
	if findErr != nil && !errors.Is(findErr, filer_pb.ErrNotFound) {
		return false, fmt.Errorf("find %s: %w", sub.FullPath, findErr)
	}
	if current != nil && len(current.Content) == 0 && current.IsInRemoteOnly() &&
		!proto.Equal(current.ToProtoEntry(), sub.ToProtoEntry()) {
		glog.V(0).InfofCtx(ctx, "keep %s: a remote-only entry has taken the path since the listing", sub.FullPath)
		return true, nil
	}
	if !keepRemote && f.RemoteStorage != nil && f.remoteTombstones != nil {
		if _, remoteLoc := f.RemoteStorage.FindMountDirectory(sub.FullPath); remoteLoc != nil {
			f.remoteTombstones.addFromEvent(string(sub.FullPath), sub.IsDirectory(), tsNs)
		}
	}
	if deleteRemote && !sub.IsDirectory() {
		if remoteErr := f.deleteSweptEntryRemote(ctx, sub); remoteErr != nil {
			return false, fmt.Errorf("remote delete %s: %w", sub.FullPath, remoteErr)
		}
	}
	if derr := f.Store.DeleteEntry(ctx, sub.FullPath); derr != nil {
		return false, fmt.Errorf("delete %s: %w", sub.FullPath, derr)
	}
	// chunks of a hard-linked child belong to the shared inode record; free
	// them only once that record is gone. A directory entry can still carry
	// chunks of its own — an S3 child promotes an existing file to a
	// directory without dropping them — so the entry's chunks are keyed to
	// its own path whatever it is.
	if message.DeleteChunks && len(sub.GetChunks()) > 0 && (len(sub.HardLinkId) == 0 || f.hardLinkIsOrphaned(ctx, sub.HardLinkId)) {
		sweep.add(sub.FullPath, sub.GetChunks())
		if sweep.count >= folderSweepChunkBatch {
			sweep.flush(ctx, f)
		}
	}
	return false, nil
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
