package filesys

import (
	"context"

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

	// TODO: replicate renaming logic on filer
	if err := dir.wfs.metaCache.DeleteEntry(context.Background(), oldPath); err != nil {
		glog.V(0).Infof("dir Rename delete local %s => %s : %v", oldPath, newPath, err)
		return fuse.EIO
	}
	oldEntry.FullPath = newPath
	if err := dir.wfs.metaCache.InsertEntry(context.Background(), oldEntry); err != nil {
		glog.V(0).Infof("dir Rename insert local %s => %s : %v", oldPath, newPath, err)
		return fuse.EIO
	}

	// fmt.Printf("rename path: %v => %v\n", oldPath, newPath)
	dir.wfs.fsNodeCache.Move(oldPath, newPath)

	// change file handle
	dir.wfs.handlesLock.Lock()
	defer dir.wfs.handlesLock.Unlock()
	inodeId := oldPath.AsInode()
	existingHandle, found := dir.wfs.handles[inodeId]
	if !found || existingHandle == nil {
		return err
	}
	delete(dir.wfs.handles, inodeId)
	dir.wfs.handles[newPath.AsInode()] = existingHandle

	return err
}
