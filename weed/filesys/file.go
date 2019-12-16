package filesys

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/fuse"
	"github.com/seaweedfs/fuse/fs"
)

const blockSize = 512

var _ = fs.Node(&File{})
var _ = fs.NodeOpener(&File{})
var _ = fs.NodeFsyncer(&File{})
var _ = fs.NodeSetattrer(&File{})
var _ = fs.NodeGetxattrer(&File{})
var _ = fs.NodeSetxattrer(&File{})
var _ = fs.NodeRemovexattrer(&File{})
var _ = fs.NodeListxattrer(&File{})

type File struct {
	Name           string
	dir            *Dir
	wfs            *WFS
	entry          *filer_pb.Entry
	entryViewCache []filer2.VisibleInterval
	isOpen         bool
}

func (file *File) fullpath() string {
	return filepath.Join(file.dir.Path, file.Name)
}

func (file *File) Attr(ctx context.Context, attr *fuse.Attr) error {

	glog.V(4).Infof("file Attr %s", file.fullpath())

	if err := file.maybeLoadEntry(ctx); err != nil {
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

func (file *File) Getxattr(ctx context.Context, req *fuse.GetxattrRequest, resp *fuse.GetxattrResponse) error {

	glog.V(4).Infof("file Getxattr %s", file.fullpath())

	if err := file.maybeLoadEntry(ctx); err != nil {
		return err
	}

	return getxattr(file.entry, req, resp)
}

func (file *File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {

	glog.V(4).Infof("file %v open %+v", file.fullpath(), req)

	file.isOpen = true

	handle := file.wfs.AcquireHandle(file, req.Uid, req.Gid)

	resp.Handle = fuse.HandleID(handle.handle)

	glog.V(3).Infof("%v file open handle id = %d", file.fullpath(), handle.handle)

	return handle, nil

}

func (file *File) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {

	if err := file.maybeLoadEntry(ctx); err != nil {
		return err
	}

	glog.V(3).Infof("%v file setattr %+v, old:%+v", file.fullpath(), req, file.entry.Attributes)
	if req.Valid.Size() {

		glog.V(3).Infof("%v file setattr set size=%v", file.fullpath(), req.Size)
		if req.Size == 0 {
			// fmt.Printf("truncate %v \n", fullPath)
			file.entry.Chunks = nil
			file.entryViewCache = nil
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

	if req.Valid.Crtime() {
		file.entry.Attributes.Crtime = req.Crtime.Unix()
	}

	if req.Valid.Mtime() {
		file.entry.Attributes.Mtime = req.Mtime.Unix()
	}

	if file.isOpen {
		return nil
	}

	file.wfs.listDirectoryEntriesCache.Delete(file.fullpath())

	return file.saveEntry(ctx)

}

func (file *File) Setxattr(ctx context.Context, req *fuse.SetxattrRequest) error {

	glog.V(4).Infof("file Setxattr %s: %s", file.fullpath(), req.Name)

	if err := file.maybeLoadEntry(ctx); err != nil {
		return err
	}

	if err := setxattr(file.entry, req); err != nil {
		return err
	}

	return file.saveEntry(ctx)

}

func (file *File) Removexattr(ctx context.Context, req *fuse.RemovexattrRequest) error {

	glog.V(4).Infof("file Removexattr %s: %s", file.fullpath(), req.Name)

	if err := file.maybeLoadEntry(ctx); err != nil {
		return err
	}

	if err := removexattr(file.entry, req); err != nil {
		return err
	}

	return file.saveEntry(ctx)

}

func (file *File) Listxattr(ctx context.Context, req *fuse.ListxattrRequest, resp *fuse.ListxattrResponse) error {

	glog.V(4).Infof("file Listxattr %s", file.fullpath())

	if err := file.maybeLoadEntry(ctx); err != nil {
		return err
	}

	if err := listxattr(file.entry, req, resp); err != nil {
		return err
	}

	return file.saveEntry(ctx)

}

func (file *File) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	// fsync works at OS level
	// write the file chunks to the filerGrpcAddress
	glog.V(3).Infof("%s/%s fsync file %+v", file.dir.Path, file.Name, req)

	return nil
}

func (file *File) maybeLoadEntry(ctx context.Context) error {
	if file.entry == nil || !file.isOpen {
		entry, err := file.wfs.maybeLoadEntry(ctx, file.dir.Path, file.Name)
		if err != nil {
			return err
		}
		if entry != nil {
			file.setEntry(entry)
		}
	}
	return nil
}

func (file *File) addChunk(chunk *filer_pb.FileChunk) {
	if chunk != nil {
		file.addChunks([]*filer_pb.FileChunk{chunk})
	}
}

func (file *File) addChunks(chunks []*filer_pb.FileChunk) {

	sort.Slice(chunks, func(i, j int) bool {
		return chunks[i].Mtime < chunks[j].Mtime
	})

	var newVisibles []filer2.VisibleInterval
	for _, chunk := range chunks {
		newVisibles = filer2.MergeIntoVisibles(file.entryViewCache, newVisibles, chunk)
		t := file.entryViewCache[:0]
		file.entryViewCache = newVisibles
		newVisibles = t
	}

	glog.V(3).Infof("%s existing %d chunks adds %d more", file.fullpath(), len(file.entry.Chunks), len(chunks))

	file.entry.Chunks = append(file.entry.Chunks, chunks...)
}

func (file *File) setEntry(entry *filer_pb.Entry) {
	file.entry = entry
	file.entryViewCache = filer2.NonOverlappingVisibleIntervals(file.entry.Chunks)
}

func (file *File) saveEntry(ctx context.Context) error {
	return file.wfs.WithFilerClient(ctx, func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.UpdateEntryRequest{
			Directory: file.dir.Path,
			Entry:     file.entry,
		}

		glog.V(1).Infof("save file entry: %v", request)
		_, err := client.UpdateEntry(ctx, request)
		if err != nil {
			glog.V(0).Infof("UpdateEntry file %s/%s: %v", file.dir.Path, file.Name, err)
			return fuse.EIO
		}

		return nil
	})
}
