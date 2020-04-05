package filer2

import (
	"context"
	"fmt"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func (f *Filer) DeleteEntryMetaAndData(ctx context.Context, p util.FullPath, isRecursive bool, ignoreRecursiveError, shouldDeleteChunks bool) (err error) {
	if p == "/" {
		return nil
	}

	entry, findErr := f.FindEntry(ctx, p)
	if findErr != nil {
		return findErr
	}

	isCollection := f.isBucket(entry)

	var chunks []*filer_pb.FileChunk
	chunks = append(chunks, entry.Chunks...)
	if entry.IsDirectory() {
		// delete the folder children, not including the folder itself
		var dirChunks []*filer_pb.FileChunk
		dirChunks, err = f.doBatchDeleteFolderMetaAndData(ctx, entry, isRecursive, ignoreRecursiveError, shouldDeleteChunks && !isCollection)
		if err != nil {
			glog.V(0).Infof("delete directory %s: %v", p, err)
			return fmt.Errorf("delete directory %s: %v", p, err)
		}
		chunks = append(chunks, dirChunks...)
	}

	// delete the file or folder
	err = f.doDeleteEntryMetaAndData(ctx, entry, shouldDeleteChunks)
	if err != nil {
		return fmt.Errorf("delete file %s: %v", p, err)
	}

	if shouldDeleteChunks && !isCollection {
		go f.DeleteChunks(chunks)
	}
	if isCollection {
		collectionName := entry.Name()
		f.doDeleteCollection(collectionName)
		f.deleteBucket(collectionName)
	}

	return nil
}

func (f *Filer) doBatchDeleteFolderMetaAndData(ctx context.Context, entry *Entry, isRecursive bool, ignoreRecursiveError, shouldDeleteChunks bool) (chunks []*filer_pb.FileChunk, err error) {

	lastFileName := ""
	includeLastFile := false
	for {
		entries, err := f.ListDirectoryEntries(ctx, entry.FullPath, lastFileName, includeLastFile, PaginationSize)
		if err != nil {
			glog.Errorf("list folder %s: %v", entry.FullPath, err)
			return nil, fmt.Errorf("list folder %s: %v", entry.FullPath, err)
		}
		if lastFileName == "" && !isRecursive && len(entries) > 0 {
			// only for first iteration in the loop
			return nil, fmt.Errorf("fail to delete non-empty folder: %s", entry.FullPath)
		}

		for _, sub := range entries {
			lastFileName = sub.Name()
			var dirChunks []*filer_pb.FileChunk
			if sub.IsDirectory() {
				dirChunks, err = f.doBatchDeleteFolderMetaAndData(ctx, sub, isRecursive, ignoreRecursiveError, shouldDeleteChunks)
				f.cacheDelDirectory(string(sub.FullPath))
				f.NotifyUpdateEvent(sub, nil, shouldDeleteChunks)
				chunks = append(chunks, dirChunks...)
			} else {
				chunks = append(chunks, sub.Chunks...)
			}
			if err != nil && !ignoreRecursiveError {
				return nil, err
			}
		}

		if len(entries) < PaginationSize {
			break
		}
	}

	glog.V(3).Infof("deleting directory %v delete %d chunks: %v", entry.FullPath, len(chunks), shouldDeleteChunks)

	if storeDeletionErr := f.store.DeleteFolderChildren(ctx, entry.FullPath); storeDeletionErr != nil {
		return nil, fmt.Errorf("filer store delete: %v", storeDeletionErr)
	}

	return chunks, nil
}

func (f *Filer) doDeleteEntryMetaAndData(ctx context.Context, entry *Entry, shouldDeleteChunks bool) (err error) {

	glog.V(3).Infof("deleting entry %v, delete chunks: %v", entry.FullPath, shouldDeleteChunks)

	if storeDeletionErr := f.store.DeleteEntry(ctx, entry.FullPath); storeDeletionErr != nil {
		return fmt.Errorf("filer store delete: %v", storeDeletionErr)
	}
	if entry.IsDirectory() {
		f.cacheDelDirectory(string(entry.FullPath))
	}
	f.NotifyUpdateEvent(entry, nil, shouldDeleteChunks)

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
