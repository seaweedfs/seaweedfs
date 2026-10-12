package filer

import (
	"context"

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
		glog.V(4).Infof("creating %v", newPath)
		newEntry := FromPbEntry(dir, message.NewEntry)
		if err := f.Store.InsertEntry(ctx, newEntry); err != nil {
			return err
		}
		// An update is not a delete: the entry still exists, so only the
		// commit record is stamped.
		f.notePathChange(oldPath, resp.TsNs, false)
		return nil
	}

	if message.OldEntry != nil {
		newer, err := f.replayBlockedByLocal(ctx, oldPath, resp.TsNs)
		if err != nil {
			return err
		}
		if !newer {
			glog.V(4).Infof("deleting %v", oldPath)
			if err := f.Store.DeleteEntry(ctx, oldPath); err != nil {
				return err
			}
			f.notePathChange(oldPath, resp.TsNs, true)
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
func (f *Filer) lockReplayPaths(oldPath, newPath util.FullPath) func() {
	lt := f.EntryLockTable
	if lt == nil {
		return func() {}
	}
	if oldPath == "" {
		oldPath = newPath
		newPath = ""
	}
	if oldPath == "" {
		return func() {}
	}
	firstPath, secondPath := oldPath, newPath
	if secondPath != "" && secondPath < firstPath {
		firstPath, secondPath = secondPath, firstPath
	}
	firstLock := lt.AcquireLock("MetaReplay", firstPath, util.ExclusiveLock)
	if secondPath == "" || secondPath == firstPath {
		return func() { lt.ReleaseLock(firstPath, firstLock) }
	}
	secondLock := lt.AcquireLock("MetaReplay", secondPath, util.ExclusiveLock)
	return func() {
		lt.ReleaseLock(secondPath, secondLock)
		lt.ReleaseLock(firstPath, firstLock)
	}
}
