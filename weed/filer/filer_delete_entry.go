package filer

import (
	"context"
	"fmt"
	"strings"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
)

type HardLinkId []byte

func (f *Filer) DeleteEntryMetaAndData(ctx context.Context, p util.FullPath, isRecursive, ignoreRecursiveError, shouldDeleteChunks, isFromOtherCluster bool, signatures []int32) (err error) {
	if p == "/" {
		return nil
	}

	entry, findErr := f.FindEntry(ctx, p)
	if findErr != nil {
		return findErr
	}

	isDeleteCollection := f.isBucket(entry)

	var chunks []*filer_pb.FileChunk
	var hardLinkIds []HardLinkId
	chunks = append(chunks, entry.Chunks...)
	if entry.IsDirectory() {
		// delete the folder children, not including the folder itself
		var dirChunks []*filer_pb.FileChunk
		var dirHardLinkIds []HardLinkId
		dirChunks, dirHardLinkIds, err = f.doBatchDeleteFolderMetaAndData(ctx, entry, isRecursive, ignoreRecursiveError, shouldDeleteChunks && !isDeleteCollection, isFromOtherCluster, signatures)
		if err != nil {
			glog.V(0).Infof("delete directory %s: %v", p, err)
			return fmt.Errorf("delete directory %s: %v", p, err)
		}
		chunks = append(chunks, dirChunks...)
		hardLinkIds = append(hardLinkIds, dirHardLinkIds...)
	}

	// delete the file or folder
	err = f.doDeleteEntryMetaAndData(ctx, entry, shouldDeleteChunks, isFromOtherCluster, signatures)
	if err != nil {
		return fmt.Errorf("delete file %s: %v", p, err)
	}

	if shouldDeleteChunks && !isDeleteCollection {
		f.DirectDeleteChunks(chunks)
	}
	// A case not handled:
	// what if the chunk is in a different collection?
	if shouldDeleteChunks {
		f.maybeDeleteHardLinks(hardLinkIds)
	}

	if isDeleteCollection {
		collectionName := entry.Name()
		f.doDeleteCollection(collectionName)
		f.deleteBucket(collectionName)
	} else {
		parent, _ := p.DirAndName()
		if err := f.removeEmptyParentFolder(ctx, util.FullPath(parent)); err != nil {
			glog.Errorf("clean up empty folders for %s : %v", p, err)
		}
	}

	return nil
}

func (f *Filer) doBatchDeleteFolderMetaAndData(ctx context.Context, entry *Entry, isRecursive, ignoreRecursiveError, shouldDeleteChunks, isFromOtherCluster bool, signatures []int32) (chunks []*filer_pb.FileChunk, hardlinkIds []HardLinkId, err error) {

	lastFileName := ""
	includeLastFile := false
	for {
		entries, err := f.ListDirectoryEntries(ctx, entry.FullPath, lastFileName, includeLastFile, PaginationSize, "")
		if err != nil {
			glog.Errorf("list folder %s: %v", entry.FullPath, err)
			return nil, nil, fmt.Errorf("list folder %s: %v", entry.FullPath, err)
		}
		if lastFileName == "" && !isRecursive && len(entries) > 0 {
			// only for first iteration in the loop
			glog.Errorf("deleting a folder %s has children: %+v ...", entry.FullPath, entries[0].Name())
			return nil, nil, fmt.Errorf("fail to delete non-empty folder: %s", entry.FullPath)
		}

		for _, sub := range entries {
			lastFileName = sub.Name()
			var dirChunks []*filer_pb.FileChunk
			var dirHardLinkIds []HardLinkId
			if sub.IsDirectory() {
				dirChunks, dirHardLinkIds, err = f.doBatchDeleteFolderMetaAndData(ctx, sub, isRecursive, ignoreRecursiveError, shouldDeleteChunks, false, nil)
				chunks = append(chunks, dirChunks...)
				hardlinkIds = append(hardlinkIds, dirHardLinkIds...)
			} else {
				f.NotifyUpdateEvent(ctx, sub, nil, shouldDeleteChunks, isFromOtherCluster, nil)
				if len(sub.HardLinkId) != 0 {
					// hard link chunk data are deleted separately
					hardlinkIds = append(hardlinkIds, sub.HardLinkId)
				} else {
					chunks = append(chunks, sub.Chunks...)
				}
			}
			if err != nil && !ignoreRecursiveError {
				return nil, nil, err
			}
		}

		if len(entries) < PaginationSize {
			break
		}
	}

	glog.V(3).Infof("deleting directory %v delete %d chunks: %v", entry.FullPath, len(chunks), shouldDeleteChunks)

	if storeDeletionErr := f.Store.DeleteFolderChildren(ctx, entry.FullPath); storeDeletionErr != nil {
		return nil, nil, fmt.Errorf("filer store delete: %v", storeDeletionErr)
	}

	f.NotifyUpdateEvent(ctx, entry, nil, shouldDeleteChunks, isFromOtherCluster, signatures)

	return chunks, hardlinkIds, nil
}

func (f *Filer) doDeleteEntryMetaAndData(ctx context.Context, entry *Entry, shouldDeleteChunks bool, isFromOtherCluster bool, signatures []int32) (err error) {

	glog.V(3).Infof("deleting entry %v, delete chunks: %v", entry.FullPath, shouldDeleteChunks)

	if storeDeletionErr := f.Store.DeleteOneEntry(ctx, entry); storeDeletionErr != nil {
		return fmt.Errorf("filer store delete: %v", storeDeletionErr)
	}
	if !entry.IsDirectory() {
		f.NotifyUpdateEvent(ctx, entry, nil, shouldDeleteChunks, isFromOtherCluster, signatures)
	}

	return nil
}

func (f *Filer) doDeleteCollection(collectionName string) (err error) {

	return f.MasterClient.WithClient(func(client master_pb.SeaweedClient) error {
		_, err := client.CollectionDelete(context.Background(), &master_pb.CollectionDeleteRequest{
			Name: collectionName,
		})
		if err != nil {
			glog.Infof("delete collection %s: %v", collectionName, err)
		}
		return err
	})

}

func (f *Filer) maybeDeleteHardLinks(hardLinkIds []HardLinkId) {
	for _, hardLinkId := range hardLinkIds {
		if err := f.Store.DeleteHardLink(context.Background(), hardLinkId); err != nil {
			glog.Errorf("delete hard link id %d : %v", hardLinkId, err)
		}
	}
}

func (f *Filer) removeEmptyParentFolder(ctx context.Context, dir util.FullPath) error {
	if !strings.HasPrefix(string(dir), f.DirBucketsPath) {
		return nil
	}
	parent, _ := dir.DirAndName()
	if parent == f.DirBucketsPath {
		// should not delete bucket itself
		return nil
	}
	entries, err := f.ListDirectoryEntries(ctx, dir, "", false, 1, "")
	if err != nil {
		return err
	}
	if len(entries) > 0 {
		return nil
	}
	if err := f.Store.DeleteEntry(ctx, dir); err != nil {
		return err
	}
	return f.removeEmptyParentFolder(ctx, util.FullPath(parent))
}
