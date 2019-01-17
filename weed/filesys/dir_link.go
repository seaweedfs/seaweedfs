package filesys

import (
	"context"
	"os"
	"syscall"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/fuse"
	"github.com/seaweedfs/fuse/fs"
)

var _ = fs.NodeSymlinker(&Dir{})
var _ = fs.NodeReadlinker(&File{})

func (dir *Dir) Symlink(ctx context.Context, req *fuse.SymlinkRequest) (fs.Node, error) {

	glog.V(3).Infof("Symlink: %v/%v to %v", dir.Path, req.NewName, req.Target)

	request := &filer_pb.CreateEntryRequest{
		Directory: dir.Path,
		Entry: &filer_pb.Entry{
			Name:        req.NewName,
			IsDirectory: false,
			Attributes: &filer_pb.FuseAttributes{
				Mtime:         time.Now().Unix(),
				Crtime:        time.Now().Unix(),
				FileMode:      uint32(os.FileMode(0755) | os.ModeSymlink),
				Uid:           req.Uid,
				Gid:           req.Gid,
				SymlinkTarget: req.Target,
			},
		},
	}

	err := dir.wfs.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		if _, err := client.CreateEntry(ctx, request); err != nil {
			glog.V(0).Infof("symlink %s/%s: %v", dir.Path, req.NewName, err)
			return fuse.EIO
		}
		return nil
	})

	symlink := dir.newFile(req.NewName, request.Entry)

	return symlink, err

}

func (file *File) Readlink(ctx context.Context, req *fuse.ReadlinkRequest) (string, error) {

	if err := file.maybeLoadAttributes(ctx); err != nil {
		return "", err
	}

	if os.FileMode(file.entry.Attributes.FileMode)&os.ModeSymlink == 0 {
		return "", fuse.Errno(syscall.EINVAL)
	}

	glog.V(3).Infof("Readlink: %v/%v => %v", file.dir.Path, file.Name, file.entry.Attributes.SymlinkTarget)

	return file.entry.Attributes.SymlinkTarget, nil

}
