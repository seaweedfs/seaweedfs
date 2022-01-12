package filesys

import (
	"context"
	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/seaweedfs/fuse"
	"github.com/seaweedfs/fuse/fs"
	"io"
	"os"
	"strings"
)

func (dir *Dir) Rename(ctx context.Context, req *fuse.RenameRequest, newDirectory fs.Node) error {

	if err := checkName(req.NewName); err != nil {
		return err
	}
	if err := checkName(req.OldName); err != nil {
		return err
	}

	newDir := newDirectory.(*Dir)

	newPath := util.NewFullPath(newDir.FullPath(), req.NewName)
	oldPath := util.NewFullPath(dir.FullPath(), req.OldName)

	glog.V(4).Infof("dir Rename %s => %s", oldPath, newPath)

	// update remote filer
	err := dir.wfs.WithFilerClient(true, func(client filer_pb.SeaweedFilerClient) error {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		request := &filer_pb.StreamRenameEntryRequest{
			OldDirectory: dir.FullPath(),
			OldName:      req.OldName,
			NewDirectory: newDir.FullPath(),
			NewName:      req.NewName,
			Signatures:   []int32{dir.wfs.signature},
		}

		stream, err := client.StreamRenameEntry(ctx, request)
		if err != nil {
			glog.Errorf("dir AtomicRenameEntry %s => %s : %v", oldPath, newPath, err)
			return fuse.EIO
		}

		for {
			resp, recvErr := stream.Recv()
			if recvErr != nil {
				if recvErr == io.EOF {
					break
				} else {
					glog.V(0).Infof("dir Rename %s => %s receive: %v", oldPath, newPath, recvErr)
					if strings.Contains(recvErr.Error(), "not empty") {
						return fuse.EEXIST
					}
					if strings.Contains(recvErr.Error(), "not directory") {
						return fuse.ENOTDIR
					}
					return recvErr
				}
			}

			if err = dir.handleRenameResponse(ctx, resp); err != nil {
				glog.V(0).Infof("dir Rename %s => %s : %v", oldPath, newPath, err)
				return err
			}

		}

		return nil

	})

	return err
}

func (dir *Dir) handleRenameResponse(ctx context.Context, resp *filer_pb.StreamRenameEntryResponse) error {
	// comes from filer StreamRenameEntry, can only be create or delete entry

	if resp.EventNotification.NewEntry != nil {
		// with new entry, the old entry name also exists. This is the first step to create new entry
		newEntry := filer.FromPbEntry(resp.EventNotification.NewParentPath, resp.EventNotification.NewEntry)
		if err := dir.wfs.metaCache.AtomicUpdateEntryFromFiler(ctx, "", newEntry); err != nil {
			return err
		}

		oldParent, newParent := util.FullPath(resp.Directory), util.FullPath(resp.EventNotification.NewParentPath)
		oldName, newName := resp.EventNotification.OldEntry.Name, resp.EventNotification.NewEntry.Name

		entryFileMode := newEntry.Attr.Mode

		oldPath := oldParent.Child(oldName)
		newPath := newParent.Child(newName)
		oldFsNode := NodeWithId(oldPath.AsInode(entryFileMode))
		newFsNode := NodeWithId(newPath.AsInode(entryFileMode))
		newDirNode, found := dir.wfs.Server.FindInternalNode(NodeWithId(newParent.AsInode(os.ModeDir)))
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
		if !newEntry.IsDirectory() {
			inodeId := oldPath.AsInode(entryFileMode)
			dir.wfs.handlesLock.Lock()
			if existingHandle, found := dir.wfs.handles[inodeId]; found && existingHandle != nil {
				glog.V(4).Infof("opened file handle %s => %s", oldPath, newPath)
				delete(dir.wfs.handles, inodeId)
				existingHandle.handle = newPath.AsInode(entryFileMode)
				existingHandle.f.entry.Name = newName
				existingHandle.f.id = newPath.AsInode(entryFileMode)
				dir.wfs.handles[newPath.AsInode(entryFileMode)] = existingHandle
			}
			dir.wfs.handlesLock.Unlock()
		}

	} else if resp.EventNotification.OldEntry != nil {
		// without new entry, only old entry name exists. This is the second step to delete old entry
		if err := dir.wfs.metaCache.AtomicUpdateEntryFromFiler(ctx, util.NewFullPath(resp.Directory, resp.EventNotification.OldEntry.Name), nil); err != nil {
			return err
		}
	}

	return nil

}
