package filesys

import (
	"context"
	"fmt"

	"bazil.org/fuse"
	"github.com/chrislusf/seaweedfs/weed/filer"
	"bazil.org/fuse/fs"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"path/filepath"
	"os"
	"time"
)

var _ = fs.Node(&File{})
// var _ = fs.NodeOpener(&File{})
// var _ = fs.NodeFsyncer(&File{})
var _ = fs.Handle(&File{})
var _ = fs.HandleReadAller(&File{})
// var _ = fs.HandleReader(&File{})
var _ = fs.HandleWriter(&File{})

type File struct {
	FileId filer.FileId
	Name   string
	dir    *Dir
	wfs    *WFS
}

func (file *File) Attr(context context.Context, attr *fuse.Attr) error {
	fullPath := filepath.Join(file.dir.Path, file.Name)
	item := file.wfs.listDirectoryEntriesCache.Get(fullPath)
	var attributes *filer_pb.FuseAttributes
	if item != nil {
		attributes = item.Value().(*filer_pb.FuseAttributes)
		glog.V(1).Infof("read cached file %v attributes", file.Name)
	} else {
		err := file.wfs.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {

			request := &filer_pb.GetFileAttributesRequest{
				Name:      file.Name,
				ParentDir: file.dir.Path,
			}

			glog.V(1).Infof("read file size: %v", request)
			resp, err := client.GetFileAttributes(context, request)
			if err != nil {
				return err
			}

			attributes = resp.Attributes

			return nil
		})

		if err != nil {
			return err
		}
	}

	attr.Mode = os.FileMode(attributes.FileMode)
	attr.Size = attributes.FileSize
	attr.Mtime = time.Unix(attributes.Mtime, 0)
	attr.Gid = attributes.Gid
	attr.Uid = attributes.Uid
	return nil

}

func (file *File) ReadAll(ctx context.Context) (content []byte, err error) {

	err = file.wfs.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.GetFileContentRequest{
			FileId: string(file.FileId),
		}

		glog.V(1).Infof("read file content: %v", request)
		resp, err := client.GetFileContent(ctx, request)
		if err != nil {
			return err
		}

		content = resp.Content

		return nil
	})

	return content, err
}

func (file *File) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	fmt.Printf("write file %+v\n", req)
	return nil
}
