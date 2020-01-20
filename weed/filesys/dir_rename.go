package filesys

import (
	"context"

	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/fuse"
	"github.com/seaweedfs/fuse/fs"
)

func (dir *Dir) Rename(ctx context.Context, req *fuse.RenameRequest, newDirectory fs.Node) error {

	newDir := newDirectory.(*Dir)
	glog.V(4).Infof("dir Rename %s/%s => %s/%s", dir.Path, req.OldName, newDir.Path, req.NewName)

	dir.wfs.cacheDelete(filer2.NewFullPath(newDir.Path, req.NewName))
	dir.wfs.cacheDelete(filer2.NewFullPath(dir.Path, req.OldName))

	return dir.wfs.WithFilerClient(ctx, func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.AtomicRenameEntryRequest{
			OldDirectory: dir.Path,
			OldName:      req.OldName,
			NewDirectory: newDir.Path,
			NewName:      req.NewName,
		}

		_, err := client.AtomicRenameEntry(ctx, request)
		if err != nil {
			glog.V(0).Infof("dir Rename %s/%s => %s/%s : %v", dir.Path, req.OldName, newDir.Path, req.NewName, err)
			return fuse.EIO
		}

		return nil

	})
}
