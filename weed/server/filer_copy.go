package weed_server

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"strings"
	"time"

	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func (fs *FilerServer) Copy(ctx context.Context, stream filer_pb.SeaweedFiler_StreamRenameEntryServer, src, dst string, so *operation.StorageOption, deleteSource bool) (err error) {
	glog.V(2).Infof("FilerServer.copy %v to %v", src, dst)

	if src, err = clearName(src); err != nil {
		return
	}
	if dst, err = clearName(dst); err != nil {
		return
	}
	src = strings.TrimRight(src, "/")
	if src == "" {
		err = fmt.Errorf("invalid source '/'")
		return
	}

	srcPath := util.FullPath(src)
	dstPath := util.FullPath(dst)
	srcEntry, err := fs.filer.FindEntry(ctx, srcPath)
	if err != nil {
		err = fmt.Errorf("failed to get src entry '%s', err: %s", src, err)
		return
	}

	oldDir, oldName := srcPath.DirAndName()
	newDir, newName := dstPath.DirAndName()
	newName = util.Nvl(newName, oldName)

	dstEntry, err := fs.filer.FindEntry(ctx, util.FullPath(strings.TrimRight(dst, "/")))
	if err != nil && err != filer_pb.ErrNotFound {
		err = fmt.Errorf("failed to get dst entry '%s', err: %s", dst, err)
		return
	}
	if err == nil && !dstEntry.IsDirectory() && srcEntry.IsDirectory() {
		err = fmt.Errorf("copy: cannot overwrite non-directory '%s' with directory '%s'", dst, src)
		return
	}

	err = fs.atomicCopyEntry(ctx, stream, oldDir, oldName, newDir, newName, nil, deleteSource)
	if err != nil {
		err = fmt.Errorf("failed to copy entry from '%s' to '%s', err: %s", src, dst, err)
		return
	}

	return
}

func (fs *FilerServer) atomicCopyEntry(ctx context.Context, stream filer_pb.SeaweedFiler_StreamRenameEntryServer, srcDir, srcName, dstDir, dstName string, signatures []int32, deleteSource bool) error {
	oldParent := util.FullPath(srcDir)
	newParent := util.FullPath(dstDir)

	if err := fs.filer.CanRename(oldParent, newParent); err != nil {
		return err
	}

	ctx, err := fs.filer.BeginTransaction(ctx)
	if err != nil {
		return err
	}

	oldEntry, err := fs.filer.FindEntry(ctx, oldParent.Child(srcName))
	if err != nil {
		fs.filer.RollbackTransaction(ctx)
		return fmt.Errorf("%s/%s not found: %v", srcDir, srcName, err)
	}

	moveErr := fs.copyEntry(ctx, stream, oldParent, oldEntry, newParent, dstName, signatures, deleteSource)
	if moveErr != nil {
		fs.filer.RollbackTransaction(ctx)
		return fmt.Errorf("%s/%s move error: %v", srcDir, srcName, moveErr)
	} else {
		if commitError := fs.filer.CommitTransaction(ctx); commitError != nil {
			fs.filer.RollbackTransaction(ctx)
			return fmt.Errorf("%s/%s move commit error: %v", srcDir, srcName, commitError)
		}
	}

	return nil
}

func (fs *FilerServer) copyEntry(ctx context.Context, stream filer_pb.SeaweedFiler_StreamRenameEntryServer, oldParent util.FullPath, entry *filer.Entry, newParent util.FullPath, newName string, signatures []int32, deleteSource bool) error {

	if err := fs.copySelfEntry(ctx, stream, oldParent, entry, newParent, newName, deleteSource, func() error {
		if entry.IsDirectory() {
			if err := fs.copyFolderSubEntries(ctx, stream, oldParent, entry, newParent, newName, signatures, deleteSource); err != nil {
				return err
			}
		}
		return nil
	}, signatures); err != nil {
		return fmt.Errorf("fail to copy %s => %s: %v", oldParent.Child(entry.Name()), newParent.Child(newName), err)
	}

	return nil
}

func (fs *FilerServer) copyFolderSubEntries(ctx context.Context, stream filer_pb.SeaweedFiler_StreamRenameEntryServer, oldParent util.FullPath, entry *filer.Entry, newParent util.FullPath, newName string, signatures []int32, deleteSource bool) error {

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
			err := fs.copyEntry(ctx, stream, currentDirPath, item, newDirPath, item.Name(), signatures, deleteSource)
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

func (fs *FilerServer) copySelfEntry(ctx context.Context, stream filer_pb.SeaweedFiler_StreamRenameEntryServer, oldParent util.FullPath, entry *filer.Entry, newParent util.FullPath, newName string, deleteSource bool, copyFolderSubEntries func() error, signatures []int32) error {

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
	if createErr := fs.filer.CreateEntry(ctx, newEntry, false, false, signatures); createErr != nil {
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

	if copyFolderSubEntries != nil {
		if copyChildrenErr := copyFolderSubEntries(); copyChildrenErr != nil {
			return copyChildrenErr
		}
	}

	// delete old entry
	if deleteSource {
		deleteErr := fs.filer.DeleteEntryMetaAndData(ctx, oldPath, false, false, false, false, signatures)
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
	}

	return nil

}
