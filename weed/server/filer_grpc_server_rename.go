package weed_server

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"path/filepath"
)

func (fs *FilerServer) AtomicRenameEntry(ctx context.Context, req *filer_pb.AtomicRenameEntryRequest) (*filer_pb.AtomicRenameEntryResponse, error) {

	ctx, err := fs.filer.BeginTransaction(ctx)
	if err != nil {
		return nil, err
	}

	oldParent := filer2.FullPath(filepath.ToSlash(req.OldDirectory))

	oldEntry, err := fs.filer.FindEntry(ctx, oldParent.Child(req.OldName))
	if err != nil {
		fs.filer.RollbackTransaction(ctx)
		return nil, fmt.Errorf("%s/%s not found: %v", req.OldDirectory, req.OldName, err)
	}

	moveErr := fs.moveEntry(ctx, oldParent, oldEntry, filer2.FullPath(filepath.ToSlash(req.NewDirectory)), req.NewName)
	if moveErr != nil {
		fs.filer.RollbackTransaction(ctx)
		return nil, fmt.Errorf("%s/%s move error: %v", req.OldDirectory, req.OldName, err)
	} else {
		if commitError := fs.filer.CommitTransaction(ctx); commitError != nil {
			fs.filer.RollbackTransaction(ctx)
			return nil, fmt.Errorf("%s/%s move commit error: %v", req.OldDirectory, req.OldName, err)
		}
	}

	return &filer_pb.AtomicRenameEntryResponse{}, nil
}

func (fs *FilerServer) moveEntry(ctx context.Context, oldParent filer2.FullPath, entry *filer2.Entry, newParent filer2.FullPath, newName string) error {
	if entry.IsDirectory() {
		if err := fs.moveFolderSubEntries(ctx, oldParent, entry, newParent, newName); err != nil {
			return err
		}
	}
	return fs.moveSelfEntry(ctx, oldParent, entry, newParent, newName)
}

func (fs *FilerServer) moveFolderSubEntries(ctx context.Context, oldParent filer2.FullPath, entry *filer2.Entry, newParent filer2.FullPath, newName string) error {

	currentDirPath := oldParent.Child(entry.Name())
	newDirPath := newParent.Child(newName)

	glog.V(1).Infof("moving folder %s => %s", currentDirPath, newDirPath)

	lastFileName := ""
	includeLastFile := false
	for {

		entries, err := fs.filer.ListDirectoryEntries(ctx, currentDirPath, lastFileName, includeLastFile, 1024)
		if err != nil {
			return err
		}

		println("found", len(entries), "entries under", currentDirPath)

		for _, item := range entries {
			lastFileName = item.Name()
			println("processing", lastFileName)
			err := fs.moveEntry(ctx, currentDirPath, item, newDirPath, item.Name())
			if err != nil {
				return err
			}
		}
		if len(entries) < 1024 {
			break
		}
	}
	return nil
}

func (fs *FilerServer) moveSelfEntry(ctx context.Context, oldParent filer2.FullPath, entry *filer2.Entry, newParent filer2.FullPath, newName string) error {

	oldPath, newPath := oldParent.Child(entry.Name()), newParent.Child(newName)

	glog.V(1).Infof("moving entry %s => %s", oldPath, newPath)

	// add to new directory
	createErr := fs.filer.CreateEntry(ctx, &filer2.Entry{
		FullPath: newPath,
		Attr:     entry.Attr,
		Chunks:   entry.Chunks,
	})
	if createErr != nil {
		return createErr
	}

	// delete old entry
	deleteErr := fs.filer.DeleteEntryMetaAndData(ctx, oldPath, false, false)
	if deleteErr != nil {
		return deleteErr
	}
	return nil

}
