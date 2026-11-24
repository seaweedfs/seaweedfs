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
	effects, err := fs.runRenameTransaction(ctx, fmt.Sprintf("%s/%s", req.OldDirectory, req.OldName), func(ctx context.Context, effects *renameEffects) (bool, error) {
		oldEntry, err := fs.filer.FindEntry(ctx, oldParent.Child(req.OldName))
		if err != nil {
			return false, fmt.Errorf("%s/%s not found: %v", req.OldDirectory, req.OldName, err)
		}
		if err := fs.checkScopeWorm(ctx, string(oldParent.Child(req.OldName))); err != nil {
			return false, err
		}
		if err := fs.checkScopeWorm(ctx, string(newParent.Child(req.NewName))); err != nil {
			return false, err
		}
		if oldEntry.IsDirectory() {
			if err := fs.checkScopeWormDirectory(ctx, string(oldParent.Child(req.OldName))); err != nil {
				return false, err
			}
			if err := fs.checkScopeWormDirectory(ctx, string(newParent.Child(req.NewName))); err != nil {
				return false, err
			}
		}

		// The check runs under the rename path locks, which CreateEntry on the same
		// filer also takes, so a target created concurrently cannot slip through.
		if req.FailIfTargetExists && newParent.Child(req.NewName) != oldParent.Child(req.OldName) {
			if _, findErr := fs.filer.FindEntry(ctx, newParent.Child(req.NewName)); findErr == nil {
				return false, fmt.Errorf("%s/%s: %w", req.NewDirectory, req.NewName, filer_pb.ErrEntryAlreadyExists)
			} else if findErr != filer_pb.ErrNotFound {
				return false, findErr
			}
		}

		if moveErr := fs.moveEntry(ctx, nil, oldParent, oldEntry, newParent, req.NewName, req.Signatures, false, &effects.metadataEvents, &effects.pendingChunkDeletes, &effects.pendingRemoteCopies); moveErr != nil {
			return false, fmt.Errorf("%s/%s move error: %v", req.OldDirectory, req.OldName, moveErr)
		}
		return fs.filer.TransactionCovers(ctx, oldParent.Child(req.OldName), newParent.Child(req.NewName)) && retryableRename(oldEntry, effects), nil
	})
	if err != nil {
		return nil, err
	}
	fs.finishRename(afterCommitCtx, effects, req.Signatures)

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

	afterCommitCtx := context.Background()
	if scopeKeepOnly(stream.Context()) {
		afterCommitCtx = withScopeKeepOnly(afterCommitCtx)
	}
	var held *renameResponseBuffer
	effects, err := fs.runRenameTransaction(afterCommitCtx, fmt.Sprintf("%s/%s", req.OldDirectory, req.OldName), func(ctx context.Context, effects *renameEffects) (bool, error) {
		held = nil
		oldEntry, err := fs.filer.FindEntry(ctx, oldParent.Child(req.OldName))
		if err != nil {
			return false, fmt.Errorf("%s/%s not found: %v", req.OldDirectory, req.OldName, err)
		}
		if err := fs.checkScopeWorm(ctx, string(oldParent.Child(req.OldName))); err != nil {
			return false, err
		}
		if err := fs.checkScopeWorm(ctx, string(newParent.Child(req.NewName))); err != nil {
			return false, err
		}
		if oldEntry.IsDirectory() {
			if err := fs.checkScopeWormDirectory(ctx, string(oldParent.Child(req.OldName))); err != nil {
				return false, err
			}
			if err := fs.checkScopeWormDirectory(ctx, string(newParent.Child(req.NewName))); err != nil {
				return false, err
			}
		}

		if oldEntry.IsDirectory() {
			// follow https://pubs.opengroup.org/onlinepubs/000095399/functions/rename.html
			targetDir := newParent.Child(req.NewName)
			newEntry, err := fs.filer.FindEntry(ctx, targetDir)
			if err == nil {
				if !newEntry.IsDirectory() {
					return false, fmt.Errorf("%s is not directory", targetDir)
				}
				if entries, _, _ := fs.filer.ListDirectoryEntries(ctx, targetDir, "", false, 1, "", "", ""); len(entries) > 0 {
					return false, fmt.Errorf("%s is not empty", targetDir)
				}
			}
		}

		// The client applies each response as it arrives. When the store's
		// transaction covers the rename's writes, a file's responses wait for
		// the commit: a rename the store refuses never reaches the client, and
		// one that runs again sends them once. A directory sends its
		// children's as it moves them, which keeps the client of a long rename
		// from timing out; having sent them, it does not run again. Without
		// such a transaction the writes show as they are made, and so do the
		// responses.
		responses := stream
		if !oldEntry.IsDirectory() && fs.filer.TransactionCovers(ctx, oldParent.Child(req.OldName), newParent.Child(req.NewName)) {
			held = &renameResponseBuffer{SeaweedFiler_StreamRenameEntryServer: stream}
			responses = held
		}
		if moveErr := fs.moveEntry(ctx, responses, oldParent, oldEntry, newParent, req.NewName, req.Signatures, false, &effects.metadataEvents, &effects.pendingChunkDeletes, &effects.pendingRemoteCopies); moveErr != nil {
			return false, fmt.Errorf("%s/%s move error: %v", req.OldDirectory, req.OldName, moveErr)
		}
		return held != nil && retryableRename(oldEntry, effects), nil
	})
	if err != nil {
		return err
	}
	if held == nil {
		fs.finishRename(afterCommitCtx, effects, req.Signatures)
		return nil
	}
	// Committed. The held responses go out while the rename finishes what the
	// commit leaves (events, chunk deletions, remote copies): a client that
	// stopped reading cannot hold that work up, and a slow remote copy does
	// not keep the client waiting for its responses. The call returns, and
	// gives up the rename's path locks, once both are done.
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		fs.finishRename(afterCommitCtx, effects, req.Signatures)
	}()
	err = held.release()
	<-finished
	return err
}

// renameAttempts bounds how often a rename runs its transaction while the
// store refuses the commit for a concurrent write to one of its keys.
const renameAttempts = 5

// renameEffects is what a rename's transaction leaves for after its commit.
type renameEffects struct {
	metadataEvents      []metadataEvent
	pendingChunkDeletes []*filer_pb.FileChunk
	pendingRemoteCopies []*pendingRemoteCopy
}

// runRenameTransaction runs attempt in a new store transaction and commits it.
// A store with optimistic transactions (TiKV) refuses the commit when another
// writer changed one of the transaction's keys after it began: a flush of the
// file being renamed, say. While attempt reports that nothing it did has left
// the transaction, it runs again in a new one, which reads that write and
// moves it along instead of failing the rename.
func (fs *FilerServer) runRenameTransaction(ctx context.Context, oldPath string, attempt func(ctx context.Context, effects *renameEffects) (retryable bool, err error)) (*renameEffects, error) {
	for n := 1; ; n++ {
		txCtx, err := fs.filer.BeginTransaction(ctx)
		if err != nil {
			return nil, err
		}
		effects := &renameEffects{}
		retryable, err := attempt(txCtx, effects)
		if err != nil {
			releaseMetadataEventStamps(fs.filer, effects.metadataEvents)
			fs.filer.RollbackTransaction(txCtx)
			return nil, err
		}
		commitErr := fs.filer.CommitTransaction(txCtx)
		if commitErr == nil {
			return effects, nil
		}
		releaseMetadataEventStamps(fs.filer, effects.metadataEvents)
		fs.filer.RollbackTransaction(txCtx)
		if !retryable || n == renameAttempts || !errors.Is(commitErr, filer.ErrTransactionConflict) {
			return nil, fmt.Errorf("%s move commit error: %v", oldPath, commitErr)
		}
		glog.V(0).Infof("rename %s: running it again (%d of %d) after a concurrent write: %v", oldPath, n+1, renameAttempts, commitErr)
		time.Sleep(time.Duration(n) * 10 * time.Millisecond)
	}
}

// retryableRename reports whether a rename's transaction may run again after
// the store refused its commit: a file move that did nothing outside the
// transaction. A remote object copied or deleted on the way, for the file or
// for the target it replaced, would stay copied or deleted. The caller also
// checks that the transaction covers the rename's writes (TransactionCovers).
func retryableRename(oldEntry *filer.Entry, effects *renameEffects) bool {
	if oldEntry.IsDirectory() || oldEntry.Remote != nil || len(effects.pendingRemoteCopies) > 0 {
		return false
	}
	for _, event := range effects.metadataEvents {
		if event.oldEntry != nil && event.oldEntry.Remote != nil {
			return false
		}
	}
	return true
}

// finishRename does what a committed rename leaves for after its commit.
func (fs *FilerServer) finishRename(ctx context.Context, effects *renameEffects, signatures []int32) {
	// Chunks from an overwritten rename target are only deletable after the
	// rename transaction has committed: anything that fails mid-rename (move,
	// child moves, oldPath delete, CommitTransaction) would otherwise leave
	// live metadata pointing at freshly-deleted chunks.
	if len(effects.pendingChunkDeletes) > 0 {
		fs.filer.DeleteChunksNotRecursive(effects.pendingChunkDeletes)
	}
	fs.finishRemoteCopies(ctx, effects.pendingRemoteCopies)
	applyRemoteCopies(effects.metadataEvents, effects.pendingRemoteCopies)
	for _, event := range effects.metadataEvents {
		event.notify(fs.filer, ctx, signatures)
	}
}

// renameResponseBuffer holds a rename's responses for its client until the
// transaction that produced them has committed.
type renameResponseBuffer struct {
	filer_pb.SeaweedFiler_StreamRenameEntryServer
	held []*filer_pb.StreamRenameEntryResponse
}

// Send keeps resp for after the commit. It fails as the stream would once the
// client has gone away, so a rename whose client canceled is rolled back, as
// it is when the responses go out at once, instead of committed unseen.
func (b *renameResponseBuffer) Send(resp *filer_pb.StreamRenameEntryResponse) error {
	if err := b.Context().Err(); err != nil {
		return err
	}
	b.held = append(b.held, resp)
	return nil
}

func (b *renameResponseBuffer) release() error {
	for _, resp := range b.held {
		if err := b.SeaweedFiler_StreamRenameEntryServer.Send(resp); err != nil {
			return err
		}
	}
	return nil
}

type metadataEvent struct {
	oldEntry     *filer.Entry
	newEntry     *filer.Entry
	deleteChunks bool
	// The old entry's remote object is deleted once the rename has committed:
	// it was copied to the new key, or belonged to an overwritten target.
	deleteOldRemote bool
}

// releaseMetadataEventStamps drops the commit stamps queued events would
// never adopt when the rename fails — left in flight they pin the delivered
// and flushed watermarks below the aborted rename forever.
func releaseMetadataEventStamps(f *filer.Filer, events []metadataEvent) {
	for _, event := range events {
		if event.newEntry != nil {
			f.ReleaseEntryCommitTsNs(event.newEntry.CommitTsNs)
		}
	}
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
			if event.oldEntry == nil || event.oldEntry.FullPath != pending.oldPath {
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

func (fs *FilerServer) moveSelfEntry(ctx context.Context, stream filer_pb.SeaweedFiler_StreamRenameEntryServer, oldParent util.FullPath, entry *filer.Entry, newParent util.FullPath, newName string, moveFolderSubEntries func() error, signatures []int32, skipTargetLookup bool, metadataEvents *[]metadataEvent, pendingChunkDeletes *[]*filer_pb.FileChunk, pendingRemoteCopies *[]*pendingRemoteCopy) (retErr error) {

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
		if targetEntry, findErr := fs.filer.FindEntryForOverwrite(ctx, newPath); findErr == nil {
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

	// A file renamed over a file is written over the target with one store
	// write when the store says that its insert replaces an entry at once and
	// whole (the Redis stores). Deleting the target first leaves its path empty
	// there for anyone reading it through another filer until the new entry
	// lands, since the rename's path locks fence this filer only. Everything
	// else keeps delete-then-create: any other store, among them those with
	// transactions, which hide the gap and fail cleanly if the target went
	// meanwhile; a target in a path-specific store; directories; and entries
	// with a remote object.
	inPlace := existingTarget != nil && !entry.IsDirectory() && !existingTarget.IsDirectory() &&
		entry.Remote == nil && existingTarget.Remote == nil && fs.filer.OverwritesAtOnce(newPath)

	if existingTarget != nil && !inPlace {
		targetCtx := filer.WithSuppressedMetadataEvents(ctx)
		// A remote delete cannot be rolled back with the target's metadata.
		// The queued target event performs it after commit unless a remote
		// copy replaces that object (in which case it must be kept).
		if existingTarget.Remote != nil {
			targetCtx = filer.WithKeepRemoteObject(targetCtx)
		}
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
			time.Time{}); deleteErr != nil {
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
	defer func() {
		// On failure no covering event adopts the insert's stamp.
		if retErr != nil {
			fs.filer.ReleaseEntryCommitTsNs(newEntry.CommitTsNs)
		}
	}()

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
	if inPlace {
		var createdParents []*filer.Entry
		fs.filer.StampEntryCommitTsNs(newEntry)
		replaceErr := fs.filer.ReplaceFileForRename(filer.WithCreatedParents(ctx, &createdParents), newEntry, fs.filer.MaxFilenameLength)
		// as for a create below: a parent the write had to create again
		// reaches peers through its own event, published before the move
		for _, parent := range createdParents {
			*metadataEvents = append(*metadataEvents, metadataEvent{newEntry: parent})
		}
		if replaceErr != nil {
			fs.filer.ReleaseEntryCommitTsNs(newEntry.CommitTsNs)
			return replaceErr
		}
	} else if skipTargetLookup {
		if newEntry.FullPath.IsLongerFileName(fs.filer.MaxFilenameLength) {
			return filer_pb.ErrEntryNameTooLong
		}
		fs.filer.StampEntryCommitTsNs(newEntry)
		if createErr := fs.filer.Store.InsertEntryKnownAbsent(filer.WithSuppressedMetadataEvents(ctx), newEntry); createErr != nil {
			fs.filer.ReleaseEntryCommitTsNs(newEntry.CommitTsNs)
			return fmt.Errorf("insert entry %s: %v", newEntry.FullPath, createErr)
		}
	} else {
		var createdParents []*filer.Entry
		createErr := fs.filer.CreateEntry(filer.WithCreatedParents(ctx, &createdParents), newEntry, nil, false, false, signatures, false, fs.filer.MaxFilenameLength)
		// Peers replay events into their own store, so a new parent only
		// reaches them through its own create event, published before the move.
		for _, parent := range createdParents {
			*metadataEvents = append(*metadataEvents, metadataEvent{newEntry: parent})
		}
		if createErr != nil {
			return createErr
		}
	}
	if existingTarget != nil {
		toDelete, err := filer.MinusChunks(ctx, fs.filer.MasterClient.GetLookupFileIdFunction(), existingTarget.GetChunks(), newEntry.GetChunks(), fs.filer.MasterClient)
		if err != nil {
			glog.ErrorfCtx(ctx, "Failed to resolve overwrite target chunks during rename. new: %v, old: %v", filer_pb.FileIdsString(newEntry.GetChunks()), filer_pb.FileIdsString(existingTarget.GetChunks()))
		} else {
			// the target may be one of several hard links to its file: the
			// rename unlinked that name only, and the other names still read
			// the file's chunks
			toDelete = fs.filer.KeepHardLinkedChunks(ctx, existingTarget, newEntry, toDelete)
			if len(toDelete) > 0 {
				// Defer chunk deletion until after CommitTransaction so that a
				// failure in any subsequent step (child moves, oldPath delete,
				// stream send, or the commit itself) leaves the chunks intact for
				// the rolled-back rename.
				*pendingChunkDeletes = append(*pendingChunkDeletes, toDelete...)
			}
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
			oldEntry:        existingTarget,
			deleteChunks:    true,
			deleteOldRemote: existingTarget.Remote != nil && copiedRemote == nil && !deferredCopy,
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
	deleteErr := fs.filer.DeleteEntryMetaAndData(deleteCtx, oldPath, false, false, false, false, signatures, time.Time{})
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
