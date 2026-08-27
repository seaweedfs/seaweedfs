package filer

import (
	"context"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const (
	MsgFailDelNonEmptyFolder = "fail to delete non-empty folder"
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
		// The bucket's metadata is already gone by this point and the result here
		// is advisory, so a request that hangs up now must not take the
		// collection's volumes with it: pass the values on without the
		// cancellation, or the volumes are stranded with nothing left to come back
		// for them. Same shape FilerStoreWrapper uses to keep a store write whole
		// once it has started. DoDeleteCollection still bounds the RPC itself and
		// logs what it could not delete.
		f.DoDeleteCollection(context.WithoutCancel(ctx), collectionName)
		// drop bucket-labeled series held by this process; the S3 gateway
		// only cleans its own registry
		stats.DeleteBucketMetrics(collectionName)
	}

	return nil
}

func (f *Filer) doBatchDeleteFolderMetaAndData(ctx context.Context, entry *Entry, isRecursive, ignoreRecursiveError, shouldDeleteChunks, isDeletingBucket, isFromOtherCluster bool, signatures []int32, onHardLinkIdsFn OnHardLinkIdsFunc) (err error) {

	//collect all the chunks of this layer and delete them together at the end
	var chunksToDelete []*filer_pb.FileChunk
	lastFileName := ""
	includeLastFile := false
	listedChildren := !isDeletingBucket || !f.Store.CanDropWholeBucket()
	if listedChildren {
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
					if err != nil && !ignoreRecursiveError {
						break
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

	// a non-recursive delete already proved the folder empty above, so sweeping the
	// children now can only remove entries that raced in after that listing
	if isRecursive || !listedChildren {
		if storeDeletionErr := f.Store.DeleteFolderChildren(ctx, entry.FullPath); storeDeletionErr != nil {
			return fmt.Errorf("filer store delete: %w", storeDeletionErr)
		}
	}

	f.NotifyUpdateEvent(ctx, entry, nil, shouldDeleteChunks, isFromOtherCluster, signatures)
	f.DeleteChunks(ctx, entry.FullPath, chunksToDelete)

	return nil
}

func (f *Filer) doDeleteEntryMetaAndData(ctx context.Context, entry *Entry, shouldDeleteChunks bool, isFromOtherCluster bool, signatures []int32) (err error) {

	glog.V(3).InfofCtx(ctx, "deleting entry %v, delete chunks: %v", entry.FullPath, shouldDeleteChunks)

	if !isFromOtherCluster {
		if _, remoteDeletionErr := f.maybeDeleteFromRemote(ctx, entry); remoteDeletionErr != nil {
			return remoteDeletionErr
		}
	}

	if storeDeletionErr := f.Store.DeleteOneEntry(ctx, entry); storeDeletionErr != nil {
		return fmt.Errorf("filer store delete: %w", storeDeletionErr)
	}

	if !entry.IsDirectory() {
		f.NotifyUpdateEvent(ctx, entry, nil, shouldDeleteChunks, isFromOtherCluster, signatures)
	}

	return nil
}

// collectionDeleteTimeout bounds the whole attempt to have a collection
// deleted: the wait for a master address, the util.Retry walk around it, and the
// RPC itself. The budget is taken before WithClientCtx, so a transient failure
// cannot restart the clock and a master that is down or mid-election cannot park
// the caller indefinitely (issue #7232).
//
// It bounds the wait, not the work. The master does not stop deleting when this
// expires: it runs its volume-server fan-out on a context carrying no deadline
// precisely so a caller giving up cannot strand it half done. A collection this
// gave up on is therefore still being removed, and a later delete of the same
// collection finds nothing left to do rather than repeating the work.
//
// 15s is fifteen times the bucket cleanup issue #7232 reports as healthy, and
// the same budget the filer gives its other master RPC (collectionListTimeout).
// A bucket deletion pays it at most twice -- once here, under the bucket entry's
// own delete, and once again for the S3 gateway's follow-up DeleteCollection --
// so the collection work stays inside roughly 25s of the 60s an S3 client waits.
const collectionDeleteTimeout = 15 * time.Second

func (f *Filer) DoDeleteCollection(ctx context.Context, collectionName string) (err error) {

	ctx, cancel := context.WithTimeout(ctx, collectionDeleteTimeout)
	defer cancel()

	return f.MasterClient.WithClientCtx(ctx, false, func(client master_pb.SeaweedClient) error {
		_, err := client.CollectionDelete(ctx, &master_pb.CollectionDeleteRequest{
			Name: collectionName,
		})
		if err != nil {
			glog.InfofCtx(ctx, "delete collection %s: %v", collectionName, err)
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
