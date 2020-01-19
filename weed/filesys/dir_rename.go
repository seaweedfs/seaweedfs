package filesys

import (
	"context"
	"fmt"

	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/fuse"
	"github.com/seaweedfs/fuse/fs"
)

func (dir *Dir) Rename(ctx context.Context, req *fuse.RenameRequest, newDirectory fs.Node) error {

	newDir := newDirectory.(*Dir)
	glog.V(4).Infof("dir Rename %s/%s => %s/%s", dir.Path, req.OldName, newDir.Path, req.NewName)

	err := dir.wfs.WithFilerClient(ctx, func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.AtomicRenameEntryRequest{
			OldDirectory: dir.Path,
			OldName:      req.OldName,
			NewDirectory: newDir.Path,
			NewName:      req.NewName,
		}

		_, err := client.AtomicRenameEntry(ctx, request)
		if err != nil {
			return fmt.Errorf("renaming %s/%s => %s/%s: %v", dir.Path, req.OldName, newDir.Path, req.NewName, err)
		}

		return nil

	})

	if err == nil {
		oldpath := string(filer2.NewFullPath(dir.Path, req.OldName))
		newpath := string(filer2.NewFullPath(newDir.Path, req.NewName))
		dir.wfs.listDirectoryEntriesCache.Delete(oldpath)
		dir.wfs.listDirectoryEntriesCache.Delete(newpath)
	}

 	return err
}
