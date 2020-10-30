package filesys

import (
	"context"
	"io"
	"os"
	"sort"
	"time"

	"github.com/seaweedfs/fuse"
	"github.com/seaweedfs/fuse/fs"

	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
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
var _ = fs.NodeForgetter(&File{})

type File struct {
	Name           string
	dir            *Dir
	wfs            *WFS
	entry          *filer_pb.Entry
	entryViewCache []filer.VisibleInterval
	isOpen         int
	reader         io.ReaderAt
	dirtyMetadata  bool
}

func (file *File) fullpath() util.FullPath {
	return util.NewFullPath(file.dir.FullPath(), file.Name)
}

func (file *File) Attr(ctx context.Context, attr *fuse.Attr) error {

	glog.V(4).Infof("file Attr %s, open:%v, existing attr: %+v", file.fullpath(), file.isOpen, attr)

	if file.isOpen <= 0 {
		if err := file.maybeLoadEntry(ctx); err != nil {
			return err
		}
	}

	attr.Inode = file.fullpath().AsInode()
	attr.Valid = time.Second
	attr.Mode = os.FileMode(file.entry.Attributes.FileMode)
	attr.Size = filer.FileSize(file.entry)
	if file.isOpen > 0 {
		attr.Size = file.entry.Attributes.FileSize
		glog.V(4).Infof("file Attr %s, open:%v, size: %d", file.fullpath(), file.isOpen, attr.Size)
	}
	attr.Crtime = time.Unix(file.entry.Attributes.Crtime, 0)
	attr.Mtime = time.Unix(file.entry.Attributes.Mtime, 0)
	attr.Gid = file.entry.Attributes.Gid
	attr.Uid = file.entry.Attributes.Uid
	attr.Blocks = attr.Size/blockSize + 1
	attr.BlockSize = uint32(file.wfs.option.ChunkSizeLimit)
	if file.entry.HardLinkCounter > 0 {
		attr.Nlink = uint32(file.entry.HardLinkCounter)
	}

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

	handle := file.wfs.AcquireHandle(file, req.Uid, req.Gid)

	resp.Handle = fuse.HandleID(handle.handle)

	glog.V(4).Infof("%v file open handle id = %d", file.fullpath(), handle.handle)

	return handle, nil

}

func (file *File) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {

	glog.V(4).Infof("%v file setattr %+v", file.fullpath(), req)

	if err := file.maybeLoadEntry(ctx); err != nil {
		return err
	}
	if file.isOpen > 0 {
		file.wfs.handlesLock.Lock()
		fileHandle := file.wfs.handles[file.fullpath().AsInode()]
		file.wfs.handlesLock.Unlock()

		if fileHandle != nil {
			fileHandle.Lock()
			defer fileHandle.Unlock()
		}
	}

	if req.Valid.Size() {

		glog.V(4).Infof("%v file setattr set size=%v chunks=%d", file.fullpath(), req.Size, len(file.entry.Chunks))
		if req.Size < filer.FileSize(file.entry) {
			// fmt.Printf("truncate %v \n", fullPath)
			var chunks []*filer_pb.FileChunk
			var truncatedChunks []*filer_pb.FileChunk
			for _, chunk := range file.entry.Chunks {
				int64Size := int64(chunk.Size)
				if chunk.Offset+int64Size > int64(req.Size) {
					// this chunk is truncated
					int64Size = int64(req.Size) - chunk.Offset
					if int64Size > 0 {
						chunks = append(chunks, chunk)
						glog.V(4).Infof("truncated chunk %+v from %d to %d\n", chunk.GetFileIdString(), chunk.Size, int64Size)
						chunk.Size = uint64(int64Size)
					} else {
						glog.V(4).Infof("truncated whole chunk %+v\n", chunk.GetFileIdString())
						truncatedChunks = append(truncatedChunks, chunk)
					}
				}
			}
			file.entry.Chunks = chunks
			file.entryViewCache = nil
			file.reader = nil
			file.wfs.deleteFileChunks(truncatedChunks)
		}
		file.entry.Attributes.FileSize = req.Size
		file.dirtyMetadata = true
	}

	if req.Valid.Mode() {
		file.entry.Attributes.FileMode = uint32(req.Mode)
		file.dirtyMetadata = true
	}

	if req.Valid.Uid() {
		file.entry.Attributes.Uid = req.Uid
		file.dirtyMetadata = true
	}

	if req.Valid.Gid() {
		file.entry.Attributes.Gid = req.Gid
		file.dirtyMetadata = true
	}

	if req.Valid.Crtime() {
		file.entry.Attributes.Crtime = req.Crtime.Unix()
		file.dirtyMetadata = true
	}

	if req.Valid.Mtime() {
		file.entry.Attributes.Mtime = req.Mtime.Unix()
		file.dirtyMetadata = true
	}

	if req.Valid.Handle() {
		// fmt.Printf("file handle => %d\n", req.Handle)
	}

	if file.isOpen > 0 {
		return nil
	}

	if !file.dirtyMetadata {
		return nil
	}

	return file.saveEntry()

}

func (file *File) Setxattr(ctx context.Context, req *fuse.SetxattrRequest) error {

	glog.V(4).Infof("file Setxattr %s: %s", file.fullpath(), req.Name)

	if err := file.maybeLoadEntry(ctx); err != nil {
		return err
	}

	if err := setxattr(file.entry, req); err != nil {
		return err
	}

	return file.saveEntry()

}

func (file *File) Removexattr(ctx context.Context, req *fuse.RemovexattrRequest) error {

	glog.V(4).Infof("file Removexattr %s: %s", file.fullpath(), req.Name)

	if err := file.maybeLoadEntry(ctx); err != nil {
		return err
	}

	if err := removexattr(file.entry, req); err != nil {
		return err
	}

	return file.saveEntry()

}

func (file *File) Listxattr(ctx context.Context, req *fuse.ListxattrRequest, resp *fuse.ListxattrResponse) error {

	glog.V(4).Infof("file Listxattr %s", file.fullpath())

	if err := file.maybeLoadEntry(ctx); err != nil {
		return err
	}

	if err := listxattr(file.entry, req, resp); err != nil {
		return err
	}

	return nil

}

func (file *File) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	// fsync works at OS level
	// write the file chunks to the filerGrpcAddress
	glog.V(4).Infof("%s/%s fsync file %+v", file.dir.FullPath(), file.Name, req)

	return nil
}

func (file *File) Forget() {
	t := util.NewFullPath(file.dir.FullPath(), file.Name)
	glog.V(4).Infof("Forget file %s", t)
	file.wfs.fsNodeCache.DeleteFsNode(t)
}

func (file *File) maybeLoadEntry(ctx context.Context) error {
	if file.isOpen > 0 {
		return nil
	}
	if file.entry != nil {
		if len(file.entry.HardLinkId) == 0 {
			// only always reload hard link
			return nil
		}
	}
	entry, err := file.wfs.maybeLoadEntry(file.dir.FullPath(), file.Name)
	if err != nil {
		glog.V(3).Infof("maybeLoadEntry file %s/%s: %v", file.dir.FullPath(), file.Name, err)
		return err
	}
	if entry != nil {
		file.setEntry(entry)
	} else {
		glog.Warningf("maybeLoadEntry not found entry %s/%s: %v", file.dir.FullPath(), file.Name, err)
	}
	return nil
}

func (file *File) addChunks(chunks []*filer_pb.FileChunk) {

	sort.Slice(chunks, func(i, j int) bool {
		if chunks[i].Mtime == chunks[j].Mtime {
			return chunks[i].Fid.FileKey < chunks[j].Fid.FileKey
		}
		return chunks[i].Mtime < chunks[j].Mtime
	})

	for _, chunk := range chunks {
		file.entryViewCache = filer.MergeIntoVisibles(file.entryViewCache, chunk)
	}

	file.reader = nil

	glog.V(4).Infof("%s existing %d chunks adds %d more", file.fullpath(), len(file.entry.Chunks), len(chunks))

	file.entry.Chunks = append(file.entry.Chunks, chunks...)
}

func (file *File) setEntry(entry *filer_pb.Entry) {
	file.entry = entry
	file.entryViewCache, _ = filer.NonOverlappingVisibleIntervals(filer.LookupFn(file.wfs), file.entry.Chunks)
	file.reader = nil
}

func (file *File) clearEntry() {
	file.entry = nil
	file.entryViewCache = nil
	file.reader = nil
}

func (file *File) saveEntry() error {
	return file.wfs.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		file.wfs.mapPbIdFromLocalToFiler(file.entry)
		defer file.wfs.mapPbIdFromFilerToLocal(file.entry)

		request := &filer_pb.UpdateEntryRequest{
			Directory:  file.dir.FullPath(),
			Entry:      file.entry,
			Signatures: []int32{file.wfs.signature},
		}

		glog.V(4).Infof("save file entry: %v", request)
		_, err := client.UpdateEntry(context.Background(), request)
		if err != nil {
			glog.Errorf("UpdateEntry file %s/%s: %v", file.dir.FullPath(), file.Name, err)
			return fuse.EIO
		}

		file.wfs.metaCache.UpdateEntry(context.Background(), filer.FromPbEntry(request.Directory, request.Entry))

		return nil
	})
}
