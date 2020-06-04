package filesys

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"time"

	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/fuse"
	"github.com/seaweedfs/fuse/fs"
)

type FileHandle struct {
	// cache file has been written to
	dirtyPages    *ContinuousDirtyPages
	contentType   string
	dirtyMetadata bool
	handle        uint64

	f         *File
	RequestId fuse.RequestID // unique ID for request
	NodeId    fuse.NodeID    // file or directory the request is about
	Uid       uint32         // user ID of process making request
	Gid       uint32         // group ID of process making request

}

func newFileHandle(file *File, uid, gid uint32) *FileHandle {
	fh := &FileHandle{
		f:          file,
		dirtyPages: newDirtyPages(file),
		Uid:        uid,
		Gid:        gid,
	}
	if fh.f.entry != nil {
		fh.f.entry.Attributes.FileSize = filer2.TotalSize(fh.f.entry.Chunks)
	}
	return fh
}

var _ = fs.Handle(&FileHandle{})

// var _ = fs.HandleReadAller(&FileHandle{})
var _ = fs.HandleReader(&FileHandle{})
var _ = fs.HandleFlusher(&FileHandle{})
var _ = fs.HandleWriter(&FileHandle{})
var _ = fs.HandleReleaser(&FileHandle{})

func (fh *FileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {

	glog.V(4).Infof("%s read fh %d: [%d,%d)", fh.f.fullpath(), fh.handle, req.Offset, req.Offset+int64(req.Size))

	buff := make([]byte, req.Size)

	totalRead, err := fh.readFromChunks(buff, req.Offset)
	if err == nil {
		dirtyOffset, dirtySize := fh.readFromDirtyPages(buff, req.Offset)
		if totalRead+req.Offset < dirtyOffset+int64(dirtySize) {
			totalRead = dirtyOffset + int64(dirtySize) - req.Offset
		}
	}

	resp.Data = buff[:totalRead]

	if err != nil {
		glog.Errorf("file handle read %s: %v", fh.f.fullpath(), err)
		return fuse.EIO
	}

	return err
}

func (fh *FileHandle) readFromDirtyPages(buff []byte, startOffset int64) (offset int64, size int) {
	return fh.dirtyPages.ReadDirtyData(buff, startOffset)
}

func (fh *FileHandle) readFromChunks(buff []byte, offset int64) (int64, error) {

	// this value should come from the filer instead of the old f
	if len(fh.f.entry.Chunks) == 0 {
		glog.V(1).Infof("empty fh %v", fh.f.fullpath())
		return 0, nil
	}

	if fh.f.entryViewCache == nil {
		fh.f.entryViewCache = filer2.NonOverlappingVisibleIntervals(fh.f.entry.Chunks)
		fh.f.reader = nil
	}

	if fh.f.reader == nil {
		chunkViews := filer2.ViewFromVisibleIntervals(fh.f.entryViewCache, 0, math.MaxInt32)
		fh.f.reader = filer2.NewChunkReaderAtFromClient(fh.f.wfs, chunkViews, fh.f.wfs.chunkCache)
	}

	totalRead, err := fh.f.reader.ReadAt(buff, offset)

	if err != nil {
		glog.Errorf("file handle read %s: %v", fh.f.fullpath(), err)
	}

	// glog.V(0).Infof("file handle read %s [%d,%d] %d : %v", fh.f.fullpath(), offset, offset+int64(totalRead), totalRead, err)

	return int64(totalRead), err
}

// Write to the file handle
func (fh *FileHandle) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {

	// write the request to volume servers
	data := make([]byte, len(req.Data))
	copy(data, req.Data)

	fh.f.entry.Attributes.FileSize = uint64(max(req.Offset+int64(len(data)), int64(fh.f.entry.Attributes.FileSize)))
	// glog.V(0).Infof("%v write [%d,%d)", fh.f.fullpath(), req.Offset, req.Offset+int64(len(req.Data)))

	chunks, err := fh.dirtyPages.AddPage(req.Offset, data)
	if err != nil {
		glog.Errorf("%v write fh %d: [%d,%d): %v", fh.f.fullpath(), fh.handle, req.Offset, req.Offset+int64(len(data)), err)
		return fuse.EIO
	}

	resp.Size = len(data)

	if req.Offset == 0 {
		// detect mime type
		fh.contentType = http.DetectContentType(data)
		fh.dirtyMetadata = true
	}

	if len(chunks) > 0 {

		fh.f.addChunks(chunks)

		fh.dirtyMetadata = true
	}

	return nil
}

func (fh *FileHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {

	glog.V(4).Infof("%v release fh %d", fh.f.fullpath(), fh.handle)

	fh.f.isOpen--

	if fh.f.isOpen <= 0 {
		fh.dirtyPages.releaseResource()
		fh.f.wfs.ReleaseHandle(fh.f.fullpath(), fuse.HandleID(fh.handle))
	}
	fh.f.entryViewCache = nil
	fh.f.reader = nil

	return nil
}

func (fh *FileHandle) Flush(ctx context.Context, req *fuse.FlushRequest) error {
	// fflush works at fh level
	// send the data to the OS
	glog.V(4).Infof("%s fh %d flush %v", fh.f.fullpath(), fh.handle, req)

	chunks, err := fh.dirtyPages.FlushToStorage()
	if err != nil {
		glog.Errorf("flush %s: %v", fh.f.fullpath(), err)
		return fuse.EIO
	}

	if len(chunks) > 0 {
		fh.f.addChunks(chunks)
		fh.dirtyMetadata = true
	}

	if !fh.dirtyMetadata {
		return nil
	}

	err = fh.f.wfs.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		if fh.f.entry.Attributes != nil {
			fh.f.entry.Attributes.Mime = fh.contentType
			fh.f.entry.Attributes.Uid = req.Uid
			fh.f.entry.Attributes.Gid = req.Gid
			fh.f.entry.Attributes.Mtime = time.Now().Unix()
			fh.f.entry.Attributes.Crtime = time.Now().Unix()
			fh.f.entry.Attributes.FileMode = uint32(0666 &^ fh.f.wfs.option.Umask)
			fh.f.entry.Attributes.Collection = fh.dirtyPages.collection
			fh.f.entry.Attributes.Replication = fh.dirtyPages.replication
		}

		request := &filer_pb.CreateEntryRequest{
			Directory: fh.f.dir.FullPath(),
			Entry:     fh.f.entry,
		}

		glog.V(3).Infof("%s set chunks: %v", fh.f.fullpath(), len(fh.f.entry.Chunks))
		for i, chunk := range fh.f.entry.Chunks {
			glog.V(3).Infof("%s chunks %d: %v [%d,%d)", fh.f.fullpath(), i, chunk.FileId, chunk.Offset, chunk.Offset+int64(chunk.Size))
		}

		chunks, garbages := filer2.CompactFileChunks(fh.f.entry.Chunks)
		fh.f.entry.Chunks = chunks
		// fh.f.entryViewCache = nil

		if err := filer_pb.CreateEntry(client, request); err != nil {
			glog.Errorf("fh flush create %s: %v", fh.f.fullpath(), err)
			return fmt.Errorf("fh flush create %s: %v", fh.f.fullpath(), err)
		}

		if fh.f.wfs.option.AsyncMetaDataCaching {
			fh.f.wfs.metaCache.InsertEntry(context.Background(), filer2.FromPbEntry(request.Directory, request.Entry))
		}

		fh.f.wfs.deleteFileChunks(garbages)
		for i, chunk := range garbages {
			glog.V(3).Infof("garbage %s chunks %d: %v [%d,%d)", fh.f.fullpath(), i, chunk.FileId, chunk.Offset, chunk.Offset+int64(chunk.Size))
		}

		return nil
	})

	if err == nil {
		fh.dirtyMetadata = false
	}

	if err != nil {
		glog.Errorf("%v fh %d flush: %v", fh.f.fullpath(), fh.handle, err)
		return fuse.EIO
	}

	return nil
}
