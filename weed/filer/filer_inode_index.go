package filer

import (
	"context"
	"encoding/binary"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

const inodeIndexKeyPrefix = "filer.inode.path."

type inodeIndexEntry struct {
	path  util.FullPath
	inode uint64
}

func inodeIndexKey(inode uint64) []byte {
	key := make([]byte, len(inodeIndexKeyPrefix)+8)
	copy(key, inodeIndexKeyPrefix)
	binary.BigEndian.PutUint64(key[len(inodeIndexKeyPrefix):], inode)
	return key
}

func (fsw *FilerStoreWrapper) storeInodeIndex(ctx context.Context, path util.FullPath, inode uint64) error {
	if inode == 0 {
		return nil
	}
	return fsw.KvPut(ctx, inodeIndexKey(inode), []byte(path))
}

func (fsw *FilerStoreWrapper) lookupInodePath(ctx context.Context, inode uint64) (util.FullPath, error) {
	if inode == 0 {
		return "", ErrKvNotFound
	}
	value, err := fsw.KvGet(ctx, inodeIndexKey(inode))
	if err != nil {
		return "", err
	}
	return util.FullPath(value), nil
}

func (fsw *FilerStoreWrapper) deleteInodeIndexIfCurrentPath(ctx context.Context, path util.FullPath, inode uint64) error {
	if inode == 0 {
		return nil
	}

	currentPath, err := fsw.lookupInodePath(ctx, inode)
	if err != nil {
		if err == ErrKvNotFound {
			return nil
		}
		return err
	}
	if currentPath != path {
		return nil
	}

	return fsw.KvDelete(ctx, inodeIndexKey(inode))
}

func (fsw *FilerStoreWrapper) collectInodeIndexEntries(ctx context.Context, dirPath util.FullPath) ([]inodeIndexEntry, error) {
	ctx = context.WithoutCancel(ctx)

	var collected []inodeIndexEntry
	if err := fsw.collectInodeIndexEntriesRecursive(ctx, dirPath, &collected); err != nil {
		return nil, err
	}
	return collected, nil
}

func (fsw *FilerStoreWrapper) collectInodeIndexEntriesRecursive(ctx context.Context, dirPath util.FullPath, collected *[]inodeIndexEntry) error {
	actualStore := fsw.getActualStore(dirPath + "/")

	lastFileName := ""
	includeStartFile := false
	for {
		page := make([]*Entry, 0, PaginationSize)
		nextLastFileName, err := actualStore.ListDirectoryEntries(ctx, dirPath, lastFileName, includeStartFile, PaginationSize, func(entry *Entry) (bool, error) {
			page = append(page, entry)
			return true, nil
		})
		if err != nil {
			return err
		}

		for _, entry := range page {
			if entry.Attr.Inode != 0 {
				*collected = append(*collected, inodeIndexEntry{path: entry.FullPath, inode: entry.Attr.Inode})
			}
			if entry.IsDirectory() {
				if err := fsw.collectInodeIndexEntriesRecursive(ctx, entry.FullPath, collected); err != nil {
					return err
				}
			}
		}

		if len(page) < PaginationSize {
			return nil
		}
		lastFileName = nextLastFileName
		includeStartFile = false
	}
}
