package filesys

import (
	"context"
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
var _ = fs.NodeIdentifier(&File{})
var _ = fs.NodeOpener(&File{})
var _ = fs.NodeFsyncer(&File{})
var _ = fs.NodeSetattrer(&File{})
var _ = fs.NodeGetxattrer(&File{})
var _ = fs.NodeSetxattrer(&File{})
var _ = fs.NodeRemovexattrer(&File{})
var _ = fs.NodeListxattrer(&File{})
var _ = fs.NodeForgetter(&File{})

type File struct {
	Name          string
	dir           *Dir
	wfs           *WFS
	entry         *filer_pb.Entry
	isOpen        int
	dirtyMetadata bool
	id            uint64
}

func (file *File) fullpath() util.FullPath {
	return util.NewFullPath(file.dir.FullPath(), file.Name)
}

func (file *File) Id() uint64 {
	return file.id
}

func (file *File) Attr(ctx context.Context, attr *fuse.Attr) (err error) {

	glog.V(4).Infof("file Attr %s, open:%v existing:%v", file.fullpath(), file.isOpen, attr)

	entry, err := file.maybeLoadEntry(ctx)
	if err != nil {
		return err
	}

	if entry == nil {
		return fuse.ENOENT
	}

	attr.Inode = file.Id()
	attr.Valid = time.Second
	attr.Mode = os.FileMode(entry.Attributes.FileMode)
	attr.Size = filer.FileSize(entry)
	if file.isOpen > 0 {
		attr.Size = entry.Attributes.FileSize
		glog.V(4).Infof("file Attr %s, open:%v, size: %d", file.fullpath(), file.isOpen, attr.Size)
	}
	attr.Crtime = time.Unix(entry.Attributes.Crtime, 0)
	attr.Ctime = time.Unix(entry.Attributes.Mtime, 0)
	attr.Mtime = time.Unix(entry.Attributes.Mtime, 0)
	attr.Gid = entry.Attributes.Gid
	attr.Uid = entry.Attributes.Uid
	attr.Blocks = attr.Size/blockSize + 1
	attr.BlockSize = uint32(file.wfs.option.ChunkSizeLimit)
	if entry.HardLinkCounter > 0 {
		attr.Nlink = uint32(entry.HardLinkCounter)
	}

	return nil

}

func (file *File) Getxattr(ctx context.Context, req *fuse.GetxattrRequest, resp *fuse.GetxattrResponse) error {

	// glog.V(4).Infof("file Getxattr %s", file.fullpath())

	entry, err := file.maybeLoadEntry(ctx)
	if err != nil {
		return err
	}

	return getxattr(entry, req, resp)
}

func (file *File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {

	glog.V(4).Infof("file %v open %+v", file.fullpath(), req)
	// resp.Flags |= fuse.OpenDirectIO

	handle := file.wfs.AcquireHandle(file, req.Uid, req.Gid)

	resp.Handle = fuse.HandleID(handle.handle)

	glog.V(4).Infof("%v file open handle id = %d", file.fullpath(), handle.handle)

	return handle, nil

}

func (file *File) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {

	glog.V(4).Infof("%v file setattr %+v mode=%d", file.fullpath(), req, req.Mode)

	entry, err := file.maybeLoadEntry(ctx)
	if err != nil {
		return err
	}

	if req.Valid.Size() {

		glog.V(4).Infof("%v file setattr set size=%v chunks=%d", file.fullpath(), req.Size, len(entry.Chunks))
		if req.Size < filer.FileSize(entry) {
			// fmt.Printf("truncate %v \n", fullPath)
			var chunks []*filer_pb.FileChunk
			var truncatedChunks []*filer_pb.FileChunk
			for _, chunk := range entry.Chunks {
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
			// set the new chunks and reset entry cache
			entry.Chunks = chunks
			file.wfs.handlesLock.Lock()
			existingHandle, found := file.wfs.handles[file.Id()]
			file.wfs.handlesLock.Unlock()
			if found {
				existingHandle.entryViewCache = nil
			}

		}
		entry.Attributes.Mtime = time.Now().Unix()
		entry.Attributes.FileSize = req.Size
		file.dirtyMetadata = true
	}

	if req.Valid.Mode() && entry.Attributes.FileMode != uint32(req.Mode) {
		entry.Attributes.FileMode = uint32(req.Mode)
		entry.Attributes.Mtime = time.Now().Unix()
		file.dirtyMetadata = true
	}

	if req.Valid.Uid() && entry.Attributes.Uid != req.Uid {
		entry.Attributes.Uid = req.Uid
		entry.Attributes.Mtime = time.Now().Unix()
		file.dirtyMetadata = true
	}

	if req.Valid.Gid() && entry.Attributes.Gid != req.Gid {
		entry.Attributes.Gid = req.Gid
		entry.Attributes.Mtime = time.Now().Unix()
		file.dirtyMetadata = true
	}

	if req.Valid.Crtime() {
		entry.Attributes.Crtime = req.Crtime.Unix()
		entry.Attributes.Mtime = time.Now().Unix()
		file.dirtyMetadata = true
	}

	if req.Valid.Mtime() && entry.Attributes.Mtime != req.Mtime.Unix() {
		entry.Attributes.Mtime = req.Mtime.Unix()
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

	return file.saveEntry(entry)

}

func (file *File) Setxattr(ctx context.Context, req *fuse.SetxattrRequest) error {

	glog.V(4).Infof("file Setxattr %s: %s", file.fullpath(), req.Name)

	entry, err := file.maybeLoadEntry(ctx)
	if err != nil {
		return err
	}

	if err := setxattr(entry, req); err != nil {
		return err
	}
	file.dirtyMetadata = true

	if file.isOpen > 0 {
		return nil
	}

	return file.saveEntry(entry)

}

func (file *File) Removexattr(ctx context.Context, req *fuse.RemovexattrRequest) error {

	glog.V(4).Infof("file Removexattr %s: %s", file.fullpath(), req.Name)

	entry, err := file.maybeLoadEntry(ctx)
	if err != nil {
		return err
	}

	if err := removexattr(entry, req); err != nil {
		return err
	}
	file.dirtyMetadata = true

	if file.isOpen > 0 {
		return nil
	}

	return file.saveEntry(entry)

}

func (file *File) Listxattr(ctx context.Context, req *fuse.ListxattrRequest, resp *fuse.ListxattrResponse) error {

	glog.V(4).Infof("file Listxattr %s", file.fullpath())

	entry, err := file.maybeLoadEntry(ctx)
	if err != nil {
		return err
	}

	if err := listxattr(entry, req, resp); err != nil {
		return err
	}

	return nil

}

func (file *File) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {

	// write the file chunks to the filerGrpcAddress
	glog.V(4).Infof("%s/%s fsync file %+v", file.dir.FullPath(), file.Name, req)

	return file.wfs.Fsync(file, req.Header)

}

func (file *File) Forget() {
	t := util.NewFullPath(file.dir.FullPath(), file.Name)
	glog.V(4).Infof("Forget file %s", t)
	file.wfs.ReleaseHandle(t, fuse.HandleID(t.AsInode(file.entry.FileMode())))

}

func (file *File) maybeLoadEntry(ctx context.Context) (entry *filer_pb.Entry, err error) {

	file.wfs.handlesLock.Lock()
	handle, found := file.wfs.handles[file.Id()]
	file.wfs.handlesLock.Unlock()
	entry = file.entry
	if found {
		// glog.V(4).Infof("maybeLoadEntry found opened file %s/%s", file.dir.FullPath(), file.Name)
		entry = handle.f.entry
	}

	if entry != nil {
		if len(entry.HardLinkId) == 0 {
			// only always reload hard link
			return entry, nil
		}
	}
	entry, err = file.wfs.maybeLoadEntry(file.dir.FullPath(), file.Name)
	if err != nil {
		glog.V(3).Infof("maybeLoadEntry file %s/%s: %v", file.dir.FullPath(), file.Name, err)
		return entry, err
	}
	if entry != nil {
		// file.entry = entry
	} else {
		glog.Warningf("maybeLoadEntry not found entry %s/%s: %v", file.dir.FullPath(), file.Name, err)
	}
	return entry, nil
}

func lessThan(a, b *filer_pb.FileChunk) bool {
	if a.Mtime == b.Mtime {
		return a.Fid.FileKey < b.Fid.FileKey
	}
	return a.Mtime < b.Mtime
}

func (file *File) addChunks(chunks []*filer_pb.FileChunk) {

	// find the earliest incoming chunk
	newChunks := chunks
	earliestChunk := newChunks[0]
	for i := 1; i < len(newChunks); i++ {
		if lessThan(earliestChunk, newChunks[i]) {
			earliestChunk = newChunks[i]
		}
	}

	entry := file.getEntry()
	if entry == nil {
		return
	}

	// pick out-of-order chunks from existing chunks
	for _, chunk := range entry.Chunks {
		if lessThan(earliestChunk, chunk) {
			chunks = append(chunks, chunk)
		}
	}

	// sort incoming chunks
	sort.Slice(chunks, func(i, j int) bool {
		return lessThan(chunks[i], chunks[j])
	})

	glog.V(4).Infof("%s existing %d chunks adds %d more", file.fullpath(), len(entry.Chunks), len(chunks))

	entry.Chunks = append(entry.Chunks, newChunks...)
}

func (file *File) saveEntry(entry *filer_pb.Entry) error {
	return file.wfs.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {

		file.wfs.mapPbIdFromLocalToFiler(entry)
		defer file.wfs.mapPbIdFromFilerToLocal(entry)

		request := &filer_pb.CreateEntryRequest{
			Directory:  file.dir.FullPath(),
			Entry:      entry,
			Signatures: []int32{file.wfs.signature},
		}

		glog.V(4).Infof("save file entry: %v", request)
		_, err := client.CreateEntry(context.Background(), request)
		if err != nil {
			glog.Errorf("UpdateEntry file %s/%s: %v", file.dir.FullPath(), file.Name, err)
			return fuse.EIO
		}

		file.wfs.metaCache.InsertEntry(context.Background(), filer.FromPbEntry(request.Directory, request.Entry))

		file.dirtyMetadata = false

		return nil
	})
}

func (file *File) getEntry() *filer_pb.Entry {
	return file.entry
}

func (file *File) downloadRemoteEntry(entry *filer_pb.Entry) (*filer_pb.Entry, error) {
	err := file.wfs.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.CacheRemoteObjectToLocalClusterRequest{
			Directory: file.dir.FullPath(),
			Name:      entry.Name,
		}

		glog.V(4).Infof("download entry: %v", request)
		resp, err := client.CacheRemoteObjectToLocalCluster(context.Background(), request)
		if err != nil {
			glog.Errorf("CacheRemoteObjectToLocalCluster file %s/%s: %v", file.dir.FullPath(), file.Name, err)
			return fuse.EIO
		}

		entry = resp.Entry

		file.wfs.metaCache.InsertEntry(context.Background(), filer.FromPbEntry(request.Directory, resp.Entry))

		file.dirtyMetadata = false

		return nil
	})

	return entry, err
}
