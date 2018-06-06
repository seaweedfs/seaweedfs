package filesys

import (
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"context"
	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"os"
	"path/filepath"
	"time"
)

var _ = fs.Node(&File{})
var _ = fs.NodeOpener(&File{})
var _ = fs.NodeFsyncer(&File{})
var _ = fs.NodeSetattrer(&File{})

type File struct {
	Chunks     []*filer_pb.FileChunk
	Name       string
	dir        *Dir
	wfs        *WFS
	attributes *filer_pb.FuseAttributes
	isOpen     bool
}

func (file *File) fullpath() string {
	return filepath.Join(file.dir.Path, file.Name)
}

func (file *File) Attr(ctx context.Context, attr *fuse.Attr) error {

	if file.attributes == nil || !file.isOpen {
		item := file.wfs.listDirectoryEntriesCache.Get(file.fullpath())
		if item != nil {
			entry := item.Value().(*filer_pb.Entry)
			file.Chunks = entry.Chunks
			file.attributes = entry.Attributes
			glog.V(1).Infof("file attr read cached %v attributes", file.Name)
		} else {
			err := file.wfs.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {

				request := &filer_pb.GetEntryAttributesRequest{
					Name:      file.Name,
					ParentDir: file.dir.Path,
				}

				resp, err := client.GetEntryAttributes(ctx, request)
				if err != nil {
					glog.V(0).Infof("file attr read file %v: %v", request, err)
					return err
				}

				file.attributes = resp.Attributes
				file.Chunks = resp.Chunks

				glog.V(1).Infof("file attr %v %+v: %d", file.fullpath(), file.attributes, filer2.TotalSize(file.Chunks))

				return nil
			})

			if err != nil {
				return err
			}
		}
	}

	attr.Mode = os.FileMode(file.attributes.FileMode)
	attr.Size = filer2.TotalSize(file.Chunks)
	attr.Mtime = time.Unix(file.attributes.Mtime, 0)
	attr.Gid = file.attributes.Gid
	attr.Uid = file.attributes.Uid

	return nil

}

func (file *File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {

	glog.V(3).Infof("%v file open %+v", file.fullpath(), req)

	file.isOpen = true

	handle := file.wfs.AcquireHandle(file, req.Uid, req.Gid)

	resp.Handle = fuse.HandleID(handle.handle)

	glog.V(3).Infof("%v file open handle id = %d", file.fullpath(), handle.handle)

	return handle, nil

}

func (file *File) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {

	glog.V(3).Infof("%v file setattr %+v, fh=%d", file.fullpath(), req, req.Handle)
	if req.Valid.Size() {

		glog.V(3).Infof("%v file setattr set size=%v", file.fullpath(), req.Size)
		if req.Size == 0 {
			// fmt.Printf("truncate %v \n", fullPath)
			file.Chunks = nil
		}
		file.attributes.FileSize = req.Size
	}
	if req.Valid.Mode() {
		file.attributes.FileMode = uint32(req.Mode)
	}

	if req.Valid.Uid() {
		file.attributes.Uid = req.Uid
	}

	if req.Valid.Gid() {
		file.attributes.Gid = req.Gid
	}

	if req.Valid.Mtime() {
		file.attributes.Mtime = req.Mtime.Unix()
	}

	return nil

}

func (file *File) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	// fsync works at OS level
	// write the file chunks to the filerGrpcAddress
	glog.V(3).Infof("%s/%s fsync file %+v", file.dir.Path, file.Name, req)

	return nil
}
