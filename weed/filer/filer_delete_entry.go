package filer

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const (
	MsgFailDelNonEmptyFolder = "fail to delete non-empty folder"
	remoteMetadataDeletionPendingIndexKey = "filer.remote.metadata.deletion.pending.index"
	remoteMetadataDeletionReconcileInterval = 1 * time.Minute
	remoteMetadataDeletionMarkRetryAttempts = 3
	remoteMetadataDeletionMarkRetryBackoff = 100 * time.Millisecond
)

type OnChunksFunc func([]*filer_pb.FileChunk) error
type OnHardLinkIdsFunc func([]HardLinkId) error

func (f *Filer) DeleteEntryMetaAndData(ctx context.Context, p util.FullPath, isRecursive, ignoreRecursiveError, shouldDeleteChunks, isFromOtherCluster bool, signatures []int32, ifNotModifiedAfter int64) (err error) {
	if p == "/" {
		return nil
	}

	entry, findErr := f.FindEntry(ctx, p)
	if findErr != nil {
		return findErr
	}
	if ifNotModifiedAfter > 0 && entry.Attr.Mtime.Unix() > ifNotModifiedAfter {
		return nil
	}
	isDeleteCollection := f.IsBucket(entry)
	if entry.IsDirectory() {
		// delete the folder children, not including the folder itself
		err = f.doBatchDeleteFolderMetaAndData(ctx, entry, isRecursive, ignoreRecursiveError, shouldDeleteChunks && !isDeleteCollection, isDeleteCollection, isFromOtherCluster, signatures, func(hardLinkIds []HardLinkId) error {
			// A case not handled:
			// what if the chunk is in a different collection?
			if shouldDeleteChunks {
				f.maybeDeleteHardLinks(ctx, hardLinkIds)
			}
			return nil
		})
		if err != nil {
			glog.V(2).InfofCtx(ctx, "delete directory %s: %v", p, err)
			return fmt.Errorf("delete directory %s: %v", p, err)
		}
	}

	// delete the file or folder
	err = f.doDeleteEntryMetaAndData(ctx, entry, shouldDeleteChunks, isFromOtherCluster, signatures)
	if err != nil {
		return fmt.Errorf("delete file %s: %v", p, err)
	}

	if shouldDeleteChunks && !isDeleteCollection {
		if len(entry.HardLinkId) != 0 && entry.HardLinkCounter > 1 {
			// if the file is a hard link and there are other hard links, do not delete the chunks
		} else {
			f.DeleteChunks(ctx, p, entry.GetChunks())
		}
	}

	if isDeleteCollection {
		collectionName := entry.Name()
		f.DoDeleteCollection(collectionName)
	}

	return nil
}

func (f *Filer) doBatchDeleteFolderMetaAndData(ctx context.Context, entry *Entry, isRecursive, ignoreRecursiveError, shouldDeleteChunks, isDeletingBucket, isFromOtherCluster bool, signatures []int32, onHardLinkIdsFn OnHardLinkIdsFunc) (err error) {

	//collect all the chunks of this layer and delete them together at the end
	var chunksToDelete []*filer_pb.FileChunk
	lastFileName := ""
	includeLastFile := false
	if !isDeletingBucket || !f.Store.CanDropWholeBucket() {
		for {
			entries, _, err := f.ListDirectoryEntries(ctx, entry.FullPath, lastFileName, includeLastFile, PaginationSize, "", "", "")
			if err != nil {
				glog.ErrorfCtx(ctx, "list folder %s: %v", entry.FullPath, err)
				return fmt.Errorf("list folder %s: %v", entry.FullPath, err)
			}
			if lastFileName == "" && !isRecursive && len(entries) > 0 {
				// only for first iteration in the loop
				glog.V(2).InfofCtx(ctx, "deleting a folder %s has children: %+v ...", entry.FullPath, entries[0].Name())
				return fmt.Errorf("%s: %s", MsgFailDelNonEmptyFolder, entry.FullPath)
			}

			for _, sub := range entries {
				lastFileName = sub.Name()
				if sub.IsDirectory() {
					subIsDeletingBucket := f.IsBucket(sub)
					err = f.doBatchDeleteFolderMetaAndData(ctx, sub, isRecursive, ignoreRecursiveError, shouldDeleteChunks, subIsDeletingBucket, isFromOtherCluster, nil, onHardLinkIdsFn)
				} else {
					if !isFromOtherCluster {
						if _, remoteErr := f.maybeDeleteFromRemote(ctx, sub); remoteErr != nil {
							glog.Warningf("remote delete child %s: %v", sub.FullPath, remoteErr)
							if !ignoreRecursiveError {
								err = remoteErr
							}
						}
					}
					f.NotifyUpdateEvent(ctx, sub, nil, shouldDeleteChunks, isFromOtherCluster, nil)
					if len(sub.HardLinkId) != 0 {
						// hard link chunk data are deleted separately
						err = onHardLinkIdsFn([]HardLinkId{sub.HardLinkId})
					} else {
						if shouldDeleteChunks {
							chunksToDelete = append(chunksToDelete, sub.GetChunks()...)
						}
					}
				}
				if err != nil && !ignoreRecursiveError {
					return err
				}
			}

			if len(entries) < PaginationSize {
				break
			}
		}
	}

	glog.V(3).InfofCtx(ctx, "deleting directory %v delete chunks: %v", entry.FullPath, shouldDeleteChunks)

	if storeDeletionErr := f.Store.DeleteFolderChildren(ctx, entry.FullPath); storeDeletionErr != nil {
		return fmt.Errorf("filer store delete: %w", storeDeletionErr)
	}

	f.NotifyUpdateEvent(ctx, entry, nil, shouldDeleteChunks, isFromOtherCluster, signatures)
	f.DeleteChunks(ctx, entry.FullPath, chunksToDelete)

	return nil
}

func (f *Filer) doDeleteEntryMetaAndData(ctx context.Context, entry *Entry, shouldDeleteChunks bool, isFromOtherCluster bool, signatures []int32) (err error) {

	glog.V(3).InfofCtx(ctx, "deleting entry %v, delete chunks: %v", entry.FullPath, shouldDeleteChunks)

	var remoteDeleted bool
	if !isFromOtherCluster {
		var remoteDeletionErr error
		remoteDeleted, remoteDeletionErr = f.maybeDeleteFromRemote(ctx, entry)
		if remoteDeletionErr != nil {
			return remoteDeletionErr
		}
	}
	if remoteDeleted && !entry.IsDirectory() {
		// Use a detached context so that pending-mark writes survive
		// request cancellation — the remote object is already deleted at
		// this point and we must record the intent durably.
		markCtx, markCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer markCancel()
		markBackoff := remoteMetadataDeletionMarkRetryBackoff
		var markErr error
		for attempt := 1; attempt <= remoteMetadataDeletionMarkRetryAttempts; attempt++ {
			markErr = f.markRemoteMetadataDeletionPending(markCtx, entry.FullPath)
			if markErr == nil {
				break
			}
			if attempt < remoteMetadataDeletionMarkRetryAttempts {
				time.Sleep(markBackoff)
				markBackoff *= 2
			}
		}
		if markErr != nil {
			glog.Errorf("CRITICAL: failed to mark remote metadata deletion pending for %s after %d attempts: %v", entry.FullPath, remoteMetadataDeletionMarkRetryAttempts, markErr)
			return fmt.Errorf("mark remote metadata deletion pending %s after retries: %w", entry.FullPath, markErr)
		}
	}

	if storeDeletionErr := f.Store.DeleteOneEntry(ctx, entry); storeDeletionErr != nil {
		return fmt.Errorf("filer store delete: %w", storeDeletionErr)
	}
	if remoteDeleted && !entry.IsDirectory() {
		clearCtx, clearCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer clearCancel()
		if clearErr := f.clearRemoteMetadataDeletionPending(clearCtx, entry.FullPath); clearErr != nil {
			glog.Warningf("clear remote metadata deletion pending %s: %v", entry.FullPath, clearErr)
		}
	}
	if !entry.IsDirectory() {
		f.NotifyUpdateEvent(ctx, entry, nil, shouldDeleteChunks, isFromOtherCluster, signatures)
	}

	return nil
}

func (f *Filer) DoDeleteCollection(collectionName string) (err error) {

	return f.MasterClient.WithClient(false, func(client master_pb.SeaweedClient) error {
		_, err := client.CollectionDelete(context.Background(), &master_pb.CollectionDeleteRequest{
			Name: collectionName,
		})
		if err != nil {
			glog.Infof("delete collection %s: %v", collectionName, err)
		}
		return err
	})

}

func (f *Filer) maybeDeleteHardLinks(ctx context.Context, hardLinkIds []HardLinkId) {
	for _, hardLinkId := range hardLinkIds {
		if err := f.Store.DeleteHardLink(ctx, hardLinkId); err != nil {
			glog.ErrorfCtx(ctx, "delete hard link id %d : %v", hardLinkId, err)
		}
	}
}

func (f *Filer) loopProcessingRemoteMetadataDeletionPending() {
	ticker := time.NewTicker(remoteMetadataDeletionReconcileInterval)
	defer ticker.Stop()

	for {
		select {
		case <-f.deletionQuit:
			return
		case <-ticker.C:
			if err := f.reconcilePendingRemoteMetadataDeletions(context.Background()); err != nil {
				glog.Warningf("reconcile remote metadata deletion pendings: %v", err)
			}
		}
	}
}

func (f *Filer) reconcilePendingRemoteMetadataDeletions(ctx context.Context) error {
	if f.Store == nil {
		return nil
	}

	pendingPaths, err := f.listPendingRemoteMetadataDeletionPaths(ctx)
	if err != nil {
		return err
	}

	for _, pendingPath := range pendingPaths {
		entry, findErr := f.FindEntryLocal(ctx, pendingPath)
		if errors.Is(findErr, filer_pb.ErrNotFound) || entry == nil {
			if clearErr := f.clearRemoteMetadataDeletionPending(ctx, pendingPath); clearErr != nil {
				glog.Warningf("clear remote metadata deletion pending %s: %v", pendingPath, clearErr)
			}
			continue
		}
		if findErr != nil {
			glog.Warningf("find pending remote metadata deletion %s: %v", pendingPath, findErr)
			continue
		}

		if deleteErr := f.Store.DeleteOneEntry(ctx, entry); deleteErr != nil {
			glog.Warningf("retry local metadata deletion %s: %v", pendingPath, deleteErr)
			continue
		}
		f.NotifyUpdateEvent(ctx, entry, nil, true, false, nil)

		if clearErr := f.clearRemoteMetadataDeletionPending(ctx, pendingPath); clearErr != nil {
			glog.Warningf("clear remote metadata deletion pending %s: %v", pendingPath, clearErr)
		}
	}

	return nil
}

func (f *Filer) markRemoteMetadataDeletionPending(ctx context.Context, path util.FullPath) error {
	f.remoteMetadataDeletionIndexMu.Lock()
	defer f.remoteMetadataDeletionIndexMu.Unlock()

	pendings, err := f.listPendingRemoteMetadataDeletionPaths(ctx)
	if err != nil {
		return err
	}

	pendingSet := make(map[string]struct{}, len(pendings)+1)
	for _, pendingPath := range pendings {
		pendingSet[string(pendingPath)] = struct{}{}
	}
	pendingSet[string(path)] = struct{}{}

	return f.Store.KvPut(ctx, []byte(remoteMetadataDeletionPendingIndexKey), encodePendingRemoteMetadataDeletionIndex(pendingSet))
}

func (f *Filer) clearRemoteMetadataDeletionPending(ctx context.Context, path util.FullPath) error {
	f.remoteMetadataDeletionIndexMu.Lock()
	defer f.remoteMetadataDeletionIndexMu.Unlock()

	pendings, err := f.listPendingRemoteMetadataDeletionPaths(ctx)
	if err != nil {
		return err
	}

	pendingSet := make(map[string]struct{}, len(pendings))
	for _, pendingPath := range pendings {
		pendingSet[string(pendingPath)] = struct{}{}
	}
	delete(pendingSet, string(path))

	return f.Store.KvPut(ctx, []byte(remoteMetadataDeletionPendingIndexKey), encodePendingRemoteMetadataDeletionIndex(pendingSet))
}

func (f *Filer) listPendingRemoteMetadataDeletionPaths(ctx context.Context) ([]util.FullPath, error) {
	if f.Store == nil {
		return nil, nil
	}

	indexData, err := f.Store.KvGet(ctx, []byte(remoteMetadataDeletionPendingIndexKey))
	if err != nil {
		if err == ErrKvNotFound {
			return nil, nil
		}
		return nil, err
	}
	if len(indexData) == 0 {
		return nil, nil
	}

	var pendingPaths []util.FullPath
	for _, p := range strings.Split(string(indexData), "\n") {
		p = strings.TrimSpace(p)
		if p != "" {
			pendingPaths = append(pendingPaths, util.FullPath(p))
		}
	}
	return pendingPaths, nil
}

func encodePendingRemoteMetadataDeletionIndex(pendingSet map[string]struct{}) []byte {
	if len(pendingSet) == 0 {
		return []byte{}
	}

	paths := make([]string, 0, len(pendingSet))
	for path := range pendingSet {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	return []byte(strings.Join(paths, "\n"))
}
