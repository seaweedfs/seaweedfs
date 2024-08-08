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

	if err := fs.filer.CanRename(oldParent, newParent, req.OldName); err != nil {
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

	moveErr := fs.moveEntry(ctx, nil, oldParent, oldEntry, newParent, req.NewName, req.Signatures)
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

func (fs *FilerServer) StreamRenameEntry(req *filer_pb.StreamRenameEntryRequest, stream filer_pb.SeaweedFiler_StreamRenameEntryServer) (err error) {

	glog.V(1).Infof("StreamRenameEntry %v", req)

	oldParent := util.FullPath(filepath.ToSlash(req.OldDirectory))
	newParent := util.FullPath(filepath.ToSlash(req.NewDirectory))

	if err := fs.filer.CanRename(oldParent, newParent, req.OldName); err != nil {
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

	moveErr := fs.moveEntry(ctx, stream, oldParent, oldEntry, newParent, req.NewName, req.Signatures)
	if moveErr != nil {
		fs.filer.RollbackTransaction(ctx)
		return fmt.Errorf("%s/%s move error: %v", req.OldDirectory, req.OldName, moveErr)
	} else {
		if commitError := fs.filer.CommitTransaction(ctx); commitError != nil {
			fs.filer.RollbackTransaction(ctx)
			return fmt.Errorf("%s/%s move commit error: %v", req.OldDirectory, req.OldName, commitError)
		}
	}

	return nil
}

func (fs *FilerServer) moveEntry(ctx context.Context, stream filer_pb.SeaweedFiler_StreamRenameEntryServer, oldParent util.FullPath, entry *filer.Entry, newParent util.FullPath, newName string, signatures []int32) error {

	if err := fs.moveSelfEntry(ctx, stream, oldParent, entry, newParent, newName, func() error {
		if entry.IsDirectory() {
			if err := fs.moveFolderSubEntries(ctx, stream, oldParent, entry, newParent, newName, signatures); err != nil {
				return err
			}
		}
		return nil
	}, signatures); err != nil {
		return fmt.Errorf("fail to move %s => %s: %v", oldParent.Child(entry.Name()), newParent.Child(newName), err)
	}

	return nil
}

func (fs *FilerServer) moveFolderSubEntries(ctx context.Context, stream filer_pb.SeaweedFiler_StreamRenameEntryServer, oldParent util.FullPath, entry *filer.Entry, newParent util.FullPath, newName string, signatures []int32) error {

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
			err := fs.moveEntry(ctx, stream, currentDirPath, item, newDirPath, item.Name(), signatures)
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

func (fs *FilerServer) moveSelfEntry(ctx context.Context, stream filer_pb.SeaweedFiler_StreamRenameEntryServer, oldParent util.FullPath, entry *filer.Entry, newParent util.FullPath, newName string, moveFolderSubEntries func() error, signatures []int32) error {

	oldPath, newPath := oldParent.Child(entry.Name()), newParent.Child(newName)

	glog.V(1).Infof("moving entry %s => %s", oldPath, newPath)

	if oldPath == newPath {
		glog.V(1).Infof("skip moving entry %s => %s", oldPath, newPath)
		return nil
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
	if createErr := fs.filer.CreateEntry(ctx, newEntry, false, false, signatures, false, fs.filer.MaxFilenameLength); createErr != nil {
		return createErr
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

	if moveFolderSubEntries != nil {
		if moveChildrenErr := moveFolderSubEntries(); moveChildrenErr != nil {
			return moveChildrenErr
		}
	}

	// delete old entry
	ctx = context.WithValue(ctx, "OP", "MV")
	deleteErr := fs.filer.DeleteEntryMetaAndData(ctx, oldPath, false, false, false, false, signatures, 0)
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
