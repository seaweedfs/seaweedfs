package weed_server

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func (fs *FilerServer) AtomicRenameEntry(ctx context.Context, req *filer_pb.AtomicRenameEntryRequest) (*filer_pb.AtomicRenameEntryResponse, error) {

	glog.V(1).Infof("AtomicRenameEntry %v", req)

	oldParent := util.FullPath(filepath.ToSlash(req.OldDirectory))
	newParent := util.FullPath(filepath.ToSlash(req.NewDirectory))

	if err := fs.filer.CanRename(oldParent, newParent); err != nil {
		return nil, err
	}

	ctx, err := fs.filer.BeginTransaction(ctx)
	if err != nil {
		return nil, err
	}

	oldEntry, err := fs.filer.FindEntry(ctx, oldParent.Child(req.OldName))
	if err != nil {
		fs.filer.RollbackTransaction(ctx)
		return nil, fmt.Errorf("%s/%s not found: %v", req.OldDirectory, req.OldName, err)
	}

	var events MoveEvents
	moveErr := fs.moveEntry(ctx, oldParent, oldEntry, newParent, req.NewName, &events)
	if moveErr != nil {
		fs.filer.RollbackTransaction(ctx)
		return nil, fmt.Errorf("%s/%s move error: %v", req.OldDirectory, req.OldName, moveErr)
	} else {
		if commitError := fs.filer.CommitTransaction(ctx); commitError != nil {
			fs.filer.RollbackTransaction(ctx)
			return nil, fmt.Errorf("%s/%s move commit error: %v", req.OldDirectory, req.OldName, commitError)
		}
	}

	return &filer_pb.AtomicRenameEntryResponse{}, nil
}

func (fs *FilerServer) moveEntry(ctx context.Context, oldParent util.FullPath, entry *filer.Entry, newParent util.FullPath, newName string, events *MoveEvents) error {

	if err := fs.moveSelfEntry(ctx, oldParent, entry, newParent, newName, events, func() error {
		if entry.IsDirectory() {
			if err := fs.moveFolderSubEntries(ctx, oldParent, entry, newParent, newName, events); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("fail to move %s => %s: %v", oldParent.Child(entry.Name()), newParent.Child(newName), err)
	}

	return nil
}

func (fs *FilerServer) moveFolderSubEntries(ctx context.Context, oldParent util.FullPath, entry *filer.Entry, newParent util.FullPath, newName string, events *MoveEvents) error {

	currentDirPath := oldParent.Child(entry.Name())
	newDirPath := newParent.Child(newName)

	glog.V(1).Infof("moving folder %s => %s", currentDirPath, newDirPath)

	lastFileName := ""
	includeLastFile := false
	for {

		entries, err := fs.filer.ListDirectoryEntries(ctx, currentDirPath, lastFileName, includeLastFile, 1024, "")
		if err != nil {
			return err
		}

		// println("found", len(entries), "entries under", currentDirPath)

		for _, item := range entries {
			lastFileName = item.Name()
			// println("processing", lastFileName)
			err := fs.moveEntry(ctx, currentDirPath, item, newDirPath, item.Name(), events)
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

func (fs *FilerServer) moveSelfEntry(ctx context.Context, oldParent util.FullPath, entry *filer.Entry, newParent util.FullPath, newName string, events *MoveEvents,
	moveFolderSubEntries func() error) error {

	oldPath, newPath := oldParent.Child(entry.Name()), newParent.Child(newName)

	glog.V(1).Infof("moving entry %s => %s", oldPath, newPath)

	if oldPath == newPath {
		glog.V(1).Infof("skip moving entry %s => %s", oldPath, newPath)
		return nil
	}

	// add to new directory
	newEntry := &filer.Entry{
		FullPath: newPath,
		Attr:     entry.Attr,
		Chunks:   entry.Chunks,
		Extended: entry.Extended,
		Content:  entry.Content,
	}
	createErr := fs.filer.CreateEntry(ctx, newEntry, false, false, nil)
	if createErr != nil {
		return createErr
	}

	events.newEntries = append(events.newEntries, newEntry)

	if moveFolderSubEntries != nil {
		if moveChildrenErr := moveFolderSubEntries(); moveChildrenErr != nil {
			return moveChildrenErr
		}
	}

	// delete old entry
	deleteErr := fs.filer.DeleteEntryMetaAndData(ctx, oldPath, false, false, false, false, nil)
	if deleteErr != nil {
		return deleteErr
	}

	events.oldEntries = append(events.oldEntries, entry)

	return nil

}

type MoveEvents struct {
	oldEntries []*filer.Entry
	newEntries []*filer.Entry
}
