package filesys

import (
	"context"

	"github.com/seaweedfs/fuse"
	"github.com/seaweedfs/fuse/fs"
)

// https://github.com/bazil/fuse/issues/130

var _ = fs.NodeAccesser(&Dir{})

func (dir *Dir) Access(ctx context.Context, req *fuse.AccessRequest) error {
	return fuse.ENOSYS
}

var _ = fs.NodeAccesser(&File{})

func (file *File) Access(ctx context.Context, req *fuse.AccessRequest) error {
	return fuse.ENOSYS
}
