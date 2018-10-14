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

const blockSize = 512

var _ = fs.Node(&File{})
var _ = fs.NodeOpener(&File{})
var _ = fs.NodeFsyncer(&File{})
var _ = fs.NodeSetattrer(&File{})

type File struct {
	Name   string
	dir    *Dir
	wfs    *WFS
	entry  *filer_pb.Entry
	isOpen bool
}

func (file *File) fullpath() string {
	return filepath.Join(file.dir.Path, file.Name)
}

func (file *File) Attr(ctx context.Context, attr *fuse.Attr) error {

	if err := file.maybeLoadAttributes(ctx); err != nil {
		return err
	}

	attr.Mode = os.FileMode(file.entry.Attributes.FileMode)
	attr.Size = filer2.TotalSize(file.entry.Chunks)
	attr.Mtime = time.Unix(file.entry.Attributes.Mtime, 0)
	attr.Gid = file.entry.Attributes.Gid
	attr.Uid = file.entry.Attributes.Uid
	attr.Blocks = attr.Size/blockSize + 1
	attr.BlockSize = uint32(file.wfs.option.ChunkSizeLimit)

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

	if err := file.maybeLoadAttributes(ctx); err != nil {
		return err
	}

	if file.isOpen {
		return nil
	}

	glog.V(3).Infof("%v file setattr %+v, old:%+v", file.fullpath(), req, file.entry.Attributes)
	if req.Valid.Size() {

		glog.V(3).Infof("%v file setattr set size=%v", file.fullpath(), req.Size)
		if req.Size == 0 {
			// fmt.Printf("truncate %v \n", fullPath)
			file.entry.Chunks = nil
		}
		file.entry.Attributes.FileSize = req.Size
	}
	if req.Valid.Mode() {
		file.entry.Attributes.FileMode = uint32(req.Mode)
	}

	if req.Valid.Uid() {
		file.entry.Attributes.Uid = req.Uid
	}

	if req.Valid.Gid() {
		file.entry.Attributes.Gid = req.Gid
	}

	if req.Valid.Mtime() {
		file.entry.Attributes.Mtime = req.Mtime.Unix()
	}

	return file.wfs.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.UpdateEntryRequest{
			Directory: file.dir.Path,
			Entry:     file.entry,
		}

		glog.V(1).Infof("set attr file entry: %v", request)
		_, err := client.UpdateEntry(ctx, request)
		if err != nil {
			glog.V(0).Infof("UpdateEntry file %s/%s: %v", file.dir.Path, file.Name, err)
			return fuse.EIO
		}

		return nil
	})

}

func (file *File) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	// fsync works at OS level
	// write the file chunks to the filerGrpcAddress
	glog.V(3).Infof("%s/%s fsync file %+v", file.dir.Path, file.Name, req)

	return nil
}

func (file *File) maybeLoadAttributes(ctx context.Context) error {
	if file.entry == nil || !file.isOpen {
		item := file.wfs.listDirectoryEntriesCache.Get(file.fullpath())
		if item != nil && !item.Expired() {
			entry := item.Value().(*filer_pb.Entry)
			file.entry = entry
			// glog.V(1).Infof("file attr read cached %v attributes", file.Name)
		} else {
			err := file.wfs.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {

				request := &filer_pb.LookupDirectoryEntryRequest{
					Name:      file.Name,
					Directory: file.dir.Path,
				}

				resp, err := client.LookupDirectoryEntry(ctx, request)
				if err != nil {
					glog.V(0).Infof("file attr read file %v: %v", request, err)
					return err
				}

				file.entry = resp.Entry

				glog.V(1).Infof("file attr %v %+v: %d", file.fullpath(), file.entry.Attributes, filer2.TotalSize(file.entry.Chunks))

				return nil
			})

			if err != nil {
				return err
			}
		}
	}
	return nil
}
