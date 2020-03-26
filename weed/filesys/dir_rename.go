package filesys

import (
	"context"
	"fmt"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/seaweedfs/fuse"
	"github.com/seaweedfs/fuse/fs"
)

func (dir *Dir) Rename(ctx context.Context, req *fuse.RenameRequest, newDirectory fs.Node) error {

	newPath := util.NewFullPath(newDir.Path, req.NewName)
	oldPath := util.NewFullPath(dir.Path, req.OldName)

	newDir := newDirectory.(*Dir)
	glog.V(4).Infof("dir Rename %s => %s", oldPath, newPath)

	err := dir.wfs.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.AtomicRenameEntryRequest{
			OldDirectory: dir.Path,
			OldName:      req.OldName,
			NewDirectory: newDir.Path,
			NewName:      req.NewName,
		}

		_, err := client.AtomicRenameEntry(context.Background(), request)
		if err != nil {
			glog.V(0).Infof("dir Rename %s => %s : %v", oldPath, newPath, err)
			return fuse.EIO
		}

		return nil

	})

	if err == nil {
		dir.wfs.cacheDelete(newPath)
		dir.wfs.cacheDelete(oldPath)

		fmt.Printf("rename path: %v => %v\n", oldPath, newPath)
		dir.wfs.fsNodeCache.Move(oldPath, newPath)

	}

	return err
}
