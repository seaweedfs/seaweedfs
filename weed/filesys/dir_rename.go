package filesys

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/filer"
	"math"

	"github.com/seaweedfs/fuse"
	"github.com/seaweedfs/fuse/fs"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func (dir *Dir) Rename(ctx context.Context, req *fuse.RenameRequest, newDirectory fs.Node) error {

	newDir := newDirectory.(*Dir)

	newPath := util.NewFullPath(newDir.FullPath(), req.NewName)
	oldPath := util.NewFullPath(dir.FullPath(), req.OldName)

	glog.V(4).Infof("dir Rename %s => %s", oldPath, newPath)

	// find local old entry
	oldEntry, err := dir.wfs.metaCache.FindEntry(context.Background(), oldPath)
	if err != nil {
		glog.Errorf("dir Rename can not find source %s : %v", oldPath, err)
		return fuse.ENOENT
	}

	// update remote filer
	err = dir.wfs.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		request := &filer_pb.AtomicRenameEntryRequest{
			OldDirectory: dir.FullPath(),
			OldName:      req.OldName,
			NewDirectory: newDir.FullPath(),
			NewName:      req.NewName,
			Signatures:   []int32{dir.wfs.signature},
		}

		_, err := client.AtomicRenameEntry(ctx, request)
		if err != nil {
			glog.Errorf("dir AtomicRenameEntry %s => %s : %v", oldPath, newPath, err)
			return fuse.EXDEV
		}

		return nil

	})
	if err != nil {
		glog.V(0).Infof("dir Rename %s => %s : %v", oldPath, newPath, err)
		return fuse.EIO
	}

	err = dir.moveEntry(context.Background(), util.FullPath(dir.FullPath()), oldEntry, util.FullPath(newDir.FullPath()), req.NewName)
	if err != nil {
		glog.V(0).Infof("dir local Rename %s => %s : %v", oldPath, newPath, err)
		return fuse.EIO
	}


	return nil
}

func (dir *Dir) moveEntry(ctx context.Context, oldParent util.FullPath, entry *filer.Entry, newParent util.FullPath, newName string) error {

	oldName := entry.Name()

	oldPath := oldParent.Child(oldName)
	newPath := newParent.Child(newName)
	if err := dir.moveSelfEntry(ctx, oldParent, entry, newParent, newName, func() error {

		oldFsNode := NodeWithId(oldPath.AsInode())
		newFsNode := NodeWithId(newPath.AsInode())
		newDirNode, found := dir.wfs.Server.FindInternalNode(NodeWithId(newParent.AsInode()))
		var newDir *Dir
		if found {
			newDir = newDirNode.(*Dir)
		}
		dir.wfs.Server.InvalidateInternalNode(oldFsNode, newFsNode, func(internalNode fs.Node) {
			if file, ok := internalNode.(*File); ok {
				glog.V(4).Infof("internal file node %s", oldParent.Child(oldName))
				file.Name = newName
				file.id = uint64(newFsNode)
				if found {
					file.dir = newDir
				}
			}
			if dir, ok := internalNode.(*Dir); ok {
				glog.V(4).Infof("internal dir node %s", oldParent.Child(oldName))
				dir.name = newName
				dir.id = uint64(newFsNode)
				if found {
					dir.parent = newDir
				}
			}
		})

		// change file handle
		inodeId := oldPath.AsInode()
		dir.wfs.handlesLock.Lock()
		if existingHandle, found := dir.wfs.handles[inodeId]; found && existingHandle == nil {
			glog.V(4).Infof("opened file handle %s => %s", oldPath, newPath)
			delete(dir.wfs.handles, inodeId)
			dir.wfs.handles[newPath.AsInode()] = existingHandle
		}
		dir.wfs.handlesLock.Unlock()

		if entry.IsDirectory() {
			if err := dir.moveFolderSubEntries(ctx, oldParent, oldName, newParent, newName); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("fail to move %s => %s: %v", oldPath, newPath, err)
	}

	return nil
}

func (dir *Dir) moveFolderSubEntries(ctx context.Context, oldParent util.FullPath, oldName string, newParent util.FullPath, newName string) error {

	currentDirPath := oldParent.Child(oldName)
	newDirPath := newParent.Child(newName)

	glog.V(1).Infof("moving folder %s => %s", currentDirPath, newDirPath)

	var moveErr error
	listErr := dir.wfs.metaCache.ListDirectoryEntries(ctx, currentDirPath, "", false, int64(math.MaxInt32), func(item *filer.Entry) bool {
		moveErr = dir.moveEntry(ctx, currentDirPath, item, newDirPath, item.Name())
		if moveErr != nil {
			return false
		}
		return true
	})
	if listErr != nil {
		return listErr
	}
	if moveErr != nil {
		return moveErr
	}

	return nil
}

func (dir *Dir) moveSelfEntry(ctx context.Context, oldParent util.FullPath, entry *filer.Entry, newParent util.FullPath, newName string, moveFolderSubEntries func() error) error {

	newPath := newParent.Child(newName)
	oldPath := oldParent.Child(entry.Name())

	entry.FullPath = newPath
	if err := dir.wfs.metaCache.InsertEntry(ctx, entry); err != nil {
		glog.V(0).Infof("dir Rename insert local %s => %s : %v", oldPath, newPath, err)
		return fuse.EIO
	}

	if moveFolderSubEntries != nil {
		if moveChildrenErr := moveFolderSubEntries(); moveChildrenErr != nil {
			return moveChildrenErr
		}
	}

	if err := dir.wfs.metaCache.DeleteEntry(ctx, oldPath); err != nil {
		glog.V(0).Infof("dir Rename delete local %s => %s : %v", oldPath, newPath, err)
		return fuse.EIO
	}

	return nil
}
