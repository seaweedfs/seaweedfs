package weed_server

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func (fs *FilerServer) AtomicRenameEntry(ctx context.Context, req *filer_pb.AtomicRenameEntryRequest) (*filer_pb.AtomicRenameEntryResponse, error) {

	glog.V(1).Infof("AtomicRenameEntry %v", req)

	oldParent := util.FullPath(filepath.ToSlash(req.OldDirectory))
	newParent := util.FullPath(filepath.ToSlash(req.NewDirectory))

	if err := fs.filer.CanRename(ctx, oldParent, newParent, req.OldName); err != nil {
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

	var metadataEvents []metadataEvent
	moveErr := fs.moveEntry(ctx, nil, oldParent, oldEntry, newParent, req.NewName, req.Signatures, false, &metadataEvents)
	if moveErr != nil {
		fs.filer.RollbackTransaction(ctx)
		return nil, fmt.Errorf("%s/%s move error: %v", req.OldDirectory, req.OldName, moveErr)
	} else {
		if commitError := fs.filer.CommitTransaction(ctx); commitError != nil {
			fs.filer.RollbackTransaction(ctx)
			return nil, fmt.Errorf("%s/%s move commit error: %v", req.OldDirectory, req.OldName, commitError)
		}
	}
	for _, event := range metadataEvents {
		event.notify(fs.filer, ctx, req.Signatures)
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

	ctx := context.Background()

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
	moveErr := fs.moveEntry(ctx, stream, oldParent, oldEntry, newParent, req.NewName, req.Signatures, false, &metadataEvents)
	if moveErr != nil {
		fs.filer.RollbackTransaction(ctx)
		return fmt.Errorf("%s/%s move error: %v", req.OldDirectory, req.OldName, moveErr)
	} else {
		if commitError := fs.filer.CommitTransaction(ctx); commitError != nil {
			fs.filer.RollbackTransaction(ctx)
			return fmt.Errorf("%s/%s move commit error: %v", req.OldDirectory, req.OldName, commitError)
		}
	}
	for _, event := range metadataEvents {
		event.notify(fs.filer, ctx, req.Signatures)
	}

	return nil
}

type metadataEvent struct {
	oldEntry     *filer.Entry
	newEntry     *filer.Entry
	deleteChunks bool
}

func (event metadataEvent) notify(f *filer.Filer, ctx context.Context, signatures []int32) {
	f.NotifyUpdateEvent(ctx, event.oldEntry, event.newEntry, event.deleteChunks, false, signatures)
}

func (fs *FilerServer) moveEntry(ctx context.Context, stream filer_pb.SeaweedFiler_StreamRenameEntryServer, oldParent util.FullPath, entry *filer.Entry, newParent util.FullPath, newName string, signatures []int32, skipTargetLookup bool, metadataEvents *[]metadataEvent) error {

	if err := fs.moveSelfEntry(ctx, stream, oldParent, entry, newParent, newName, func() error {
		if entry.IsDirectory() {
			if err := fs.moveFolderSubEntries(ctx, stream, oldParent, entry, newParent, newName, signatures, metadataEvents); err != nil {
				return err
			}
		}
		return nil
	}, signatures, skipTargetLookup, metadataEvents); err != nil {
		return fmt.Errorf("fail to move %s => %s: %v", oldParent.Child(entry.Name()), newParent.Child(newName), err)
	}

	return nil
}

func (fs *FilerServer) moveFolderSubEntries(ctx context.Context, stream filer_pb.SeaweedFiler_StreamRenameEntryServer, oldParent util.FullPath, entry *filer.Entry, newParent util.FullPath, newName string, signatures []int32, metadataEvents *[]metadataEvent) error {

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
			err := fs.moveEntry(ctx, stream, currentDirPath, item, newDirPath, item.Name(), signatures, skipTarget, metadataEvents)
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

func (fs *FilerServer) moveSelfEntry(ctx context.Context, stream filer_pb.SeaweedFiler_StreamRenameEntryServer, oldParent util.FullPath, entry *filer.Entry, newParent util.FullPath, newName string, moveFolderSubEntries func() error, signatures []int32, skipTargetLookup bool, metadataEvents *[]metadataEvent) error {

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
		if deleteErr := fs.filer.DeleteEntryMetaAndData(
			filer.WithSuppressedMetadataEvents(ctx),
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
		Remote:          entry.Remote,
		Quota:           entry.Quota,
	}
	if skipTargetLookup {
		if newEntry.FullPath.IsLongerFileName(fs.filer.MaxFilenameLength) {
			return filer_pb.ErrEntryNameTooLong
		}
		if createErr := fs.filer.Store.InsertEntryKnownAbsent(filer.WithSuppressedMetadataEvents(ctx), newEntry); createErr != nil {
			return fmt.Errorf("insert entry %s: %v", newEntry.FullPath, createErr)
		}
	} else {
		if createErr := fs.filer.CreateEntry(filer.WithSuppressedMetadataEvents(ctx), newEntry, false, false, signatures, false, fs.filer.MaxFilenameLength); createErr != nil {
			return createErr
		}
	}
	if existingTarget != nil {
		toDelete, err := filer.MinusChunks(ctx, fs.filer.MasterClient.GetLookupFileIdFunction(), existingTarget.GetChunks(), newEntry.GetChunks())
		if err != nil {
			glog.ErrorfCtx(ctx, "Failed to resolve overwrite target chunks during rename. new: %v, old: %v", newEntry.GetChunks(), existingTarget.GetChunks())
		} else {
			fs.filer.DeleteChunksNotRecursive(toDelete)
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
		oldEntry: sourceEntry,
		newEntry: newEntry,
	})

	if moveFolderSubEntries != nil {
		if moveChildrenErr := moveFolderSubEntries(); moveChildrenErr != nil {
			return moveChildrenErr
		}
	}

	// delete old entry
	ctx = context.WithValue(ctx, "OP", "MV")
	deleteErr := fs.filer.DeleteEntryMetaAndData(filer.WithSuppressedMetadataEvents(ctx), oldPath, false, false, false, false, signatures, 0)
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
