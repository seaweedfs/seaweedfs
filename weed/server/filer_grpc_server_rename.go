package weed_server

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func (fs *FilerServer) AtomicRenameEntry(ctx context.Context, req *filer_pb.AtomicRenameEntryRequest) (*filer_pb.AtomicRenameEntryResponse, error) {

	glog.V(1).Infof("AtomicRenameEntry %v", req)

	ctx, err := fs.filer.BeginTransaction(ctx)
	if err != nil {
		return nil, err
	}

	oldParent := util.FullPath(filepath.ToSlash(req.OldDirectory))

	oldEntry, err := fs.filer.FindEntry(ctx, oldParent.Child(req.OldName))
	if err != nil {
		fs.filer.RollbackTransaction(ctx)
		return nil, fmt.Errorf("%s/%s not found: %v", req.OldDirectory, req.OldName, err)
	}

	var events MoveEvents
	moveErr := fs.moveEntry(ctx, oldParent, oldEntry, util.FullPath(filepath.ToSlash(req.NewDirectory)), req.NewName, &events)
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

func (fs *FilerServer) moveEntry(ctx context.Context, oldParent util.FullPath, entry *filer2.Entry, newParent util.FullPath, newName string, events *MoveEvents) error {

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

func (fs *FilerServer) moveFolderSubEntries(ctx context.Context, oldParent util.FullPath, entry *filer2.Entry, newParent util.FullPath, newName string, events *MoveEvents) error {

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

func (fs *FilerServer) moveSelfEntry(ctx context.Context, oldParent util.FullPath, entry *filer2.Entry, newParent util.FullPath, newName string, events *MoveEvents,
	moveFolderSubEntries func() error) error {

	oldPath, newPath := oldParent.Child(entry.Name()), newParent.Child(newName)

	glog.V(1).Infof("moving entry %s => %s", oldPath, newPath)

	if oldPath == newPath {
		glog.V(1).Infof("skip moving entry %s => %s", oldPath, newPath)
		return nil
	}

	// add to new directory
	newEntry := &filer2.Entry{
		FullPath: newPath,
		Attr:     entry.Attr,
		Chunks:   entry.Chunks,
	}
	createErr := fs.filer.CreateEntry(ctx, newEntry, false)
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
	deleteErr := fs.filer.DeleteEntryMetaAndData(ctx, oldPath, false, false, false)
	if deleteErr != nil {
		return deleteErr
	}

	events.oldEntries = append(events.oldEntries, entry)

	return nil

}

type MoveEvents struct {
	oldEntries []*filer2.Entry
	newEntries []*filer2.Entry
}
