package weed_server

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"path/filepath"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// acquireRenamePathLocks holds both paths across commit and notification so a
// fenced lookup cannot read renamed state under a fence preceding its events.
// Path order avoids deadlock with a reverse rename; descendants are not
// locked. The returned func releases both.
func (fs *FilerServer) acquireRenamePathLocks(intention string, oldPath, newPath util.FullPath) func() {
	firstPath, secondPath := oldPath, newPath
	if secondPath < firstPath {
		firstPath, secondPath = secondPath, firstPath
	}
	firstLock := fs.entryLockTable.AcquireLock(intention, firstPath, util.ExclusiveLock)
	if secondPath == firstPath {
		return func() { fs.entryLockTable.ReleaseLock(firstPath, firstLock) }
	}
	secondLock := fs.entryLockTable.AcquireLock(intention, secondPath, util.ExclusiveLock)
	return func() {
		fs.entryLockTable.ReleaseLock(secondPath, secondLock)
		fs.entryLockTable.ReleaseLock(firstPath, firstLock)
	}
}

func (fs *FilerServer) AtomicRenameEntry(ctx context.Context, req *filer_pb.AtomicRenameEntryRequest) (*filer_pb.AtomicRenameEntryResponse, error) {

	glog.V(1).Infof("AtomicRenameEntry %v", req)

	oldParent := util.FullPath(filepath.ToSlash(req.OldDirectory))
	newParent := util.FullPath(filepath.ToSlash(req.NewDirectory))

	if err := fs.filer.CanRename(ctx, oldParent, newParent, req.OldName); err != nil {
		return nil, err
	}

	defer fs.acquireRenamePathLocks("AtomicRenameEntry", oldParent.Child(req.OldName), newParent.Child(req.NewName))()

	// a SQL store keeps the transaction in the context and refuses calls
	// through it once committed; the work after the commit uses this one
	afterCommitCtx := ctx
	ctx, err := fs.filer.BeginTransaction(ctx)
	if err != nil {
		return nil, err
	}

	oldEntry, err := fs.filer.FindEntry(ctx, oldParent.Child(req.OldName))
	if err != nil {
		fs.filer.RollbackTransaction(ctx)
		return nil, fmt.Errorf("%s/%s not found: %v", req.OldDirectory, req.OldName, err)
	}

	var metadataEvents []metadataEvent
	var pendingChunkDeletes []*filer_pb.FileChunk
	var pendingRemoteCopies []*pendingRemoteCopy
	moveErr := fs.moveEntry(ctx, nil, oldParent, oldEntry, newParent, req.NewName, req.Signatures, false, &metadataEvents, &pendingChunkDeletes, &pendingRemoteCopies)
	if moveErr != nil {
		fs.filer.RollbackTransaction(ctx)
		return nil, fmt.Errorf("%s/%s move error: %v", req.OldDirectory, req.OldName, moveErr)
	} else {
		if commitError := fs.filer.CommitTransaction(ctx); commitError != nil {
			fs.filer.RollbackTransaction(ctx)
			return nil, fmt.Errorf("%s/%s move commit error: %v", req.OldDirectory, req.OldName, commitError)
		}
	}
	// Chunks from an overwritten rename target are only deletable after the
	// rename transaction has committed: anything that fails mid-rename (move,
	// child moves, oldPath delete, CommitTransaction) would otherwise leave
	// live metadata pointing at freshly-deleted chunks.
	if len(pendingChunkDeletes) > 0 {
		fs.filer.DeleteChunksNotRecursive(pendingChunkDeletes)
	}
	fs.finishRemoteCopies(afterCommitCtx, pendingRemoteCopies)
	applyRemoteCopies(metadataEvents, pendingRemoteCopies)
	for _, event := range metadataEvents {
		event.notify(fs.filer, afterCommitCtx, req.Signatures)
	}

	return &filer_pb.AtomicRenameEntryResponse{}, nil
}

func (fs *FilerServer) StreamRenameEntry(req *filer_pb.StreamRenameEntryRequest, stream filer_pb.SeaweedFiler_StreamRenameEntryServer) (err error) {

	glog.V(1).Infof("StreamRenameEntry %v", req)

	oldParent := util.FullPath(filepath.ToSlash(req.OldDirectory))
	newParent := util.FullPath(filepath.ToSlash(req.NewDirectory))

	if err := fs.filer.CanRename(stream.Context(), oldParent, newParent, req.OldName); err != nil {
		return err
	}

	defer fs.acquireRenamePathLocks("StreamRenameEntry", oldParent.Child(req.OldName), newParent.Child(req.NewName))()

	ctx := context.Background()

	afterCommitCtx := ctx
	ctx, err = fs.filer.BeginTransaction(ctx)
	if err != nil {
		return err
	}

	oldEntry, err := fs.filer.FindEntry(ctx, oldParent.Child(req.OldName))
	if err != nil {
		fs.filer.RollbackTransaction(ctx)
		return fmt.Errorf("%s/%s not found: %v", req.OldDirectory, req.OldName, err)
	}

	if oldEntry.IsDirectory() {
		// follow https://pubs.opengroup.org/onlinepubs/000095399/functions/rename.html
		targetDir := newParent.Child(req.NewName)
		newEntry, err := fs.filer.FindEntry(ctx, targetDir)
		if err == nil {
			if !newEntry.IsDirectory() {
				fs.filer.RollbackTransaction(ctx)
				return fmt.Errorf("%s is not directory", targetDir)
			}
			if entries, _, _ := fs.filer.ListDirectoryEntries(context.Background(), targetDir, "", false, 1, "", "", ""); len(entries) > 0 {
				return fmt.Errorf("%s is not empty", targetDir)
			}
		}
	}

	var metadataEvents []metadataEvent
	var pendingChunkDeletes []*filer_pb.FileChunk
	var pendingRemoteCopies []*pendingRemoteCopy
	moveErr := fs.moveEntry(ctx, stream, oldParent, oldEntry, newParent, req.NewName, req.Signatures, false, &metadataEvents, &pendingChunkDeletes, &pendingRemoteCopies)
	if moveErr != nil {
		fs.filer.RollbackTransaction(ctx)
		return fmt.Errorf("%s/%s move error: %v", req.OldDirectory, req.OldName, moveErr)
	} else {
		if commitError := fs.filer.CommitTransaction(ctx); commitError != nil {
			fs.filer.RollbackTransaction(ctx)
			return fmt.Errorf("%s/%s move commit error: %v", req.OldDirectory, req.OldName, commitError)
		}
	}
	if len(pendingChunkDeletes) > 0 {
		fs.filer.DeleteChunksNotRecursive(pendingChunkDeletes)
	}
	fs.finishRemoteCopies(afterCommitCtx, pendingRemoteCopies)
	applyRemoteCopies(metadataEvents, pendingRemoteCopies)
	for _, event := range metadataEvents {
		event.notify(fs.filer, afterCommitCtx, req.Signatures)
	}

	return nil
}

type metadataEvent struct {
	oldEntry     *filer.Entry
	newEntry     *filer.Entry
	deleteChunks bool
	// the old entry's remote object is deleted once the rename has committed:
	// it was copied to the new key, and a rename that fails must not lose it
	deleteOldRemote bool
}

// pendingRemoteCopy is a remote-only rename over a remote-only target whose
// copy runs after the commit: the store transaction cannot roll a remote
// write back, so the new entry keeps the old key's stamp until the copy lands.
type pendingRemoteCopy struct {
	source  *filer.Entry
	oldPath util.FullPath
	newPath util.FullPath
	failed  bool
	copied  *filer_pb.RemoteEntry
}

// finishRemoteCopies runs the deferred remote copies after the commit. A
// failure leaves the new entry pointing at the old key, whose object the
// caller then keeps: remote.sync completes the copy the same way it does a
// rename whose event it finds unfinished.
func (fs *FilerServer) finishRemoteCopies(ctx context.Context, pendings []*pendingRemoteCopy) {
	for _, pending := range pendings {
		remoteEntry, err := fs.filer.CopyRemoteOnlyEntry(ctx, pending.source, pending.oldPath, pending.newPath)
		if err != nil {
			glog.WarningfCtx(ctx, "remote rename copy of %s: %v", pending.newPath, err)
			pending.failed = true
			continue
		}
		entry, findErr := fs.filer.FindEntry(ctx, pending.newPath)
		if findErr != nil {
			glog.WarningfCtx(ctx, "stamp remote rename copy of %s: %v", pending.newPath, findErr)
			pending.failed = true
			continue
		}
		entry.Remote = remoteEntry
		delete(entry.Extended, filer.ExtRemoteRenameSourceKey)
		if updateErr := fs.filer.UpdateEntry(ctx, entry, entry, false); updateErr != nil {
			glog.WarningfCtx(ctx, "stamp remote rename copy of %s: %v", pending.newPath, updateErr)
			pending.failed = true
			continue
		}
		pending.copied = remoteEntry
	}
}

// applyRemoteCopies brings the queued rename events in line with the deferred
// copies. The stamp is written with a plain UpdateEntry, which publishes no
// event, so the rename event carries it, without the pending-source marker,
// to peer filers and subscribers. A failed copy keeps the old object, which
// the entry still reads through the marker.
func applyRemoteCopies(events []metadataEvent, pendings []*pendingRemoteCopy) {
	for i := range events {
		event := &events[i]
		for _, pending := range pendings {
			if event.oldEntry.FullPath != pending.oldPath {
				continue
			}
			if pending.failed {
				event.deleteOldRemote = false
			} else if pending.copied != nil && event.newEntry != nil {
				newEntry := event.newEntry.ShallowClone()
				newEntry.Remote = pending.copied
				newEntry.Extended = maps.Clone(newEntry.Extended)
				delete(newEntry.Extended, filer.ExtRemoteRenameSourceKey)
				event.newEntry = newEntry
			}
		}
	}
}

func (event metadataEvent) notify(f *filer.Filer, ctx context.Context, signatures []int32) {
	if event.deleteOldRemote {
		deleteRenamedRemoteObject(f, ctx, event.oldEntry)
	}
	f.NotifyUpdateEvent(ctx, event.oldEntry, event.newEntry, event.deleteChunks, false, signatures)
}

// deleteRenamedRemoteObject deletes the remote object a committed rename no
// longer needs. A child path is not locked by its directory's rename, so
// another rename may have put a remote-only entry at the key meanwhile; the
// store is asked directly, a lazy fetch would revive the key. When in doubt
// the object stays: an extra object loses nothing.
func deleteRenamedRemoteObject(f *filer.Filer, ctx context.Context, entry *filer.Entry) {
	current, findErr := f.Store.FindEntry(ctx, entry.FullPath)
	switch {
	case findErr != nil && !errors.Is(findErr, filer_pb.ErrNotFound):
		glog.WarningfCtx(ctx, "keep remote object of renamed %s: look up its old path: %v", entry.FullPath, findErr)
	case findErr == nil && current.IsInRemoteOnly():
		glog.V(0).InfofCtx(ctx, "keep remote object of renamed %s: a remote-only entry has taken its path", entry.FullPath)
	default:
		if err := f.DeleteFromRemote(ctx, entry); err != nil {
			glog.WarningfCtx(ctx, "delete remote object of renamed %s: %v", entry.FullPath, err)
		}
	}
}

func (fs *FilerServer) moveEntry(ctx context.Context, stream filer_pb.SeaweedFiler_StreamRenameEntryServer, oldParent util.FullPath, entry *filer.Entry, newParent util.FullPath, newName string, signatures []int32, skipTargetLookup bool, metadataEvents *[]metadataEvent, pendingChunkDeletes *[]*filer_pb.FileChunk, pendingRemoteCopies *[]*pendingRemoteCopy) error {

	if err := fs.moveSelfEntry(ctx, stream, oldParent, entry, newParent, newName, func() error {
		if entry.IsDirectory() {
			if err := fs.moveFolderSubEntries(ctx, stream, oldParent, entry, newParent, newName, signatures, metadataEvents, pendingChunkDeletes, pendingRemoteCopies); err != nil {
				return err
			}
		}
		return nil
	}, signatures, skipTargetLookup, metadataEvents, pendingChunkDeletes, pendingRemoteCopies); err != nil {
		return fmt.Errorf("fail to move %s => %s: %v", oldParent.Child(entry.Name()), newParent.Child(newName), err)
	}

	return nil
}

func (fs *FilerServer) moveFolderSubEntries(ctx context.Context, stream filer_pb.SeaweedFiler_StreamRenameEntryServer, oldParent util.FullPath, entry *filer.Entry, newParent util.FullPath, newName string, signatures []int32, metadataEvents *[]metadataEvent, pendingChunkDeletes *[]*filer_pb.FileChunk, pendingRemoteCopies *[]*pendingRemoteCopy) error {

	currentDirPath := oldParent.Child(entry.Name())
	newDirPath := newParent.Child(newName)

	glog.V(1).Infof("moving folder %s => %s", currentDirPath, newDirPath)

	lastFileName := ""
	includeLastFile := false
	for {

		entries, hasMore, err := fs.filer.ListDirectoryEntries(ctx, currentDirPath, lastFileName, includeLastFile, 1024, "", "", "")
		if err != nil {
			return err
		}

		// println("found", len(entries), "entries under", currentDirPath)

		for _, item := range entries {
			lastFileName = item.Name()
			// println("processing", lastFileName)
			newChildPath := newDirPath.Child(item.Name())
			skipTarget := fs.filer.Store.SameActualStore(newDirPath, newChildPath)
			err := fs.moveEntry(ctx, stream, currentDirPath, item, newDirPath, item.Name(), signatures, skipTarget, metadataEvents, pendingChunkDeletes, pendingRemoteCopies)
			if err != nil {
				return err
			}
		}
		if !hasMore {
			break
		}
	}
	return nil
}

func (fs *FilerServer) moveSelfEntry(ctx context.Context, stream filer_pb.SeaweedFiler_StreamRenameEntryServer, oldParent util.FullPath, entry *filer.Entry, newParent util.FullPath, newName string, moveFolderSubEntries func() error, signatures []int32, skipTargetLookup bool, metadataEvents *[]metadataEvent, pendingChunkDeletes *[]*filer_pb.FileChunk, pendingRemoteCopies *[]*pendingRemoteCopy) error {

	oldPath, newPath := oldParent.Child(entry.Name()), newParent.Child(newName)

	glog.V(1).Infof("moving entry %s => %s", oldPath, newPath)

	if oldPath == newPath {
		glog.V(1).Infof("skip moving entry %s => %s", oldPath, newPath)
		return nil
	}

	sourceEntry := entry.ShallowClone()
	sourceEntry.FullPath = oldPath

	var existingTarget *filer.Entry
	if !skipTargetLookup {
		if targetEntry, findErr := fs.filer.FindEntry(ctx, newPath); findErr == nil {
			existingTarget = targetEntry.ShallowClone()
		} else if findErr != filer_pb.ErrNotFound {
			return findErr
		}
	}
	if existingTarget != nil {
		switch {
		case existingTarget.IsDirectory() && !entry.IsDirectory():
			return fmt.Errorf("%s: %w", existingTarget.FullPath, filer_pb.ErrExistingIsDirectory)
		case !existingTarget.IsDirectory() && entry.IsDirectory():
			return fmt.Errorf("%s: %w", existingTarget.FullPath, filer_pb.ErrExistingIsFile)
		}
	}

	// a remote-only entry is copied on the remote before anything changes:
	// deleting the old entry deletes the old object. Over a remote-only
	// target the copy runs after the commit instead — it overwrites the
	// target's only content, which a rolled-back commit cannot restore —
	// and the new entry reads the old key until the copy stamps it.
	deferredCopy := false
	var copiedRemote *filer_pb.RemoteEntry
	if existingTarget != nil && existingTarget.IsInRemoteOnly() && entry.IsInRemoteOnly() {
		if _, _, copier, copyErr := fs.filer.RemoteCopyLocations(ctx, entry, oldPath, newPath); copyErr != nil {
			return copyErr
		} else if copier != nil {
			deferredCopy = true
			*pendingRemoteCopies = append(*pendingRemoteCopies, &pendingRemoteCopy{source: sourceEntry, oldPath: oldPath, newPath: newPath})
		}
	}
	if !deferredCopy {
		var copyErr error
		copiedRemote, copyErr = fs.filer.CopyRemoteOnlyEntry(ctx, entry, oldPath, newPath)
		if copyErr != nil {
			return copyErr
		}
	}
	remoteEntry := entry.Remote
	if copiedRemote != nil {
		remoteEntry = copiedRemote
	}

	if existingTarget != nil {
		targetCtx := filer.WithSuppressedMetadataEvents(ctx)
		if copiedRemote != nil || deferredCopy {
			// the copy has replaced or still needs the target's object; keep it
			targetCtx = filer.WithKeepRemoteObject(targetCtx)
			existingTarget.Extended = maps.Clone(existingTarget.Extended)
			if existingTarget.Extended == nil {
				existingTarget.Extended = map[string][]byte{}
			}
			existingTarget.Extended[filer.ExtKeepRemoteObjectKey] = []byte("true")
		}
		if deleteErr := fs.filer.DeleteEntryMetaAndData(
			targetCtx,
			newPath,
			false,
			false,
			false,
			false,
			signatures,
			0,
		); deleteErr != nil {
			return deleteErr
		}
	}

	// add to new directory
	newEntry := &filer.Entry{
		FullPath:        newPath,
		Attr:            entry.Attr,
		Chunks:          entry.GetChunks(),
		Extended:        entry.Extended,
		Content:         entry.Content,
		HardLinkCounter: entry.HardLinkCounter,
		HardLinkId:      entry.HardLinkId,
		Remote:          remoteEntry,
		Quota:           entry.Quota,
	}
	if deferredCopy {
		// a pending entry renamed again still reads its original source
		newEntry.Extended = maps.Clone(newEntry.Extended)
		if newEntry.Extended == nil {
			newEntry.Extended = map[string][]byte{}
		}
		newEntry.Extended[filer.ExtRemoteRenameSourceKey] = []byte(fs.filer.RemoteSourcePath(entry, oldPath))
	} else if copiedRemote != nil && len(newEntry.Extended[filer.ExtRemoteRenameSourceKey]) > 0 {
		// the copy came from the pending source; the new key holds it now.
		// The source itself is left to remote.sync, which deletes it only
		// while it is still the object the rename event recorded
		newEntry.Extended = maps.Clone(newEntry.Extended)
		delete(newEntry.Extended, filer.ExtRemoteRenameSourceKey)
	}
	if skipTargetLookup {
		if newEntry.FullPath.IsLongerFileName(fs.filer.MaxFilenameLength) {
			return filer_pb.ErrEntryNameTooLong
		}
		if createErr := fs.filer.Store.InsertEntryKnownAbsent(filer.WithSuppressedMetadataEvents(ctx), newEntry); createErr != nil {
			return fmt.Errorf("insert entry %s: %v", newEntry.FullPath, createErr)
		}
	} else {
		if createErr := fs.filer.CreateEntry(filer.WithSuppressedMetadataEvents(ctx), newEntry, nil, false, false, signatures, false, fs.filer.MaxFilenameLength); createErr != nil {
			return createErr
		}
	}
	if existingTarget != nil {
		toDelete, err := filer.MinusChunks(ctx, fs.filer.MasterClient.GetLookupFileIdFunction(), existingTarget.GetChunks(), newEntry.GetChunks(), fs.filer.MasterClient)
		if err != nil {
			glog.ErrorfCtx(ctx, "Failed to resolve overwrite target chunks during rename. new: %v, old: %v", newEntry.GetChunks(), existingTarget.GetChunks())
		} else if len(toDelete) > 0 {
			// Defer chunk deletion until after CommitTransaction so that a
			// failure in any subsequent step (child moves, oldPath delete,
			// stream send, or the commit itself) leaves the chunks intact for
			// the rolled-back rename.
			*pendingChunkDeletes = append(*pendingChunkDeletes, toDelete...)
		}
	}
	if stream != nil {
		if err := stream.Send(&filer_pb.StreamRenameEntryResponse{
			Directory: string(oldParent),
			EventNotification: &filer_pb.EventNotification{
				OldEntry: &filer_pb.Entry{
					Name: entry.Name(),
				},
				NewEntry:           newEntry.ToProtoEntry(),
				DeleteChunks:       false,
				NewParentPath:      string(newParent),
				IsFromOtherCluster: false,
				Signatures:         nil,
			},
			TsNs: time.Now().UnixNano(),
		}); err != nil {
			return err
		}
	}

	if existingTarget != nil {
		*metadataEvents = append(*metadataEvents, metadataEvent{
			oldEntry:     existingTarget,
			deleteChunks: true,
		})
	}
	*metadataEvents = append(*metadataEvents, metadataEvent{
		oldEntry:        sourceEntry,
		newEntry:        newEntry,
		deleteOldRemote: copiedRemote != nil || deferredCopy,
	})
	selfEvent := len(*metadataEvents) - 1
	keepOldRemote := copiedRemote != nil || deferredCopy

	if moveFolderSubEntries != nil {
		if moveChildrenErr := moveFolderSubEntries(); moveChildrenErr != nil {
			return moveChildrenErr
		}
		// removing the old directory on the remote removes every object under
		// its prefix, the copied children's old keys included, which must wait
		// for the commit. Those are deleted one by one after it instead, and
		// nothing else under the prefix is touched.
		for _, event := range (*metadataEvents)[selfEvent+1:] {
			if event.deleteOldRemote {
				keepOldRemote = true
				break
			}
		}
	}

	// delete old entry
	ctx = context.WithValue(ctx, "OP", "MV")
	deleteCtx := filer.WithSuppressedMetadataEvents(ctx)
	if keepOldRemote {
		deleteCtx = filer.WithKeepRemoteObject(deleteCtx)
	}
	deleteErr := fs.filer.DeleteEntryMetaAndData(deleteCtx, oldPath, false, false, false, false, signatures, 0)
	if deleteErr != nil {
		return deleteErr
	}
	if stream != nil {
		if err := stream.Send(&filer_pb.StreamRenameEntryResponse{
			Directory: string(oldParent),
			EventNotification: &filer_pb.EventNotification{
				OldEntry: &filer_pb.Entry{
					Name: entry.Name(),
				},
				NewEntry:           nil,
				DeleteChunks:       false,
				NewParentPath:      "",
				IsFromOtherCluster: false,
				Signatures:         nil,
			},
			TsNs: time.Now().UnixNano(),
		}); err != nil {
			return err
		}
	}

	return nil

}
