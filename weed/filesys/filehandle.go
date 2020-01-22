package filesys

import (
	"context"
	"fmt"
	"mime"
	"path"
	"time"

	"github.com/gabriel-vasile/mimetype"

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
	return &FileHandle{
		f:          file,
		dirtyPages: newDirtyPages(file),
		Uid:        uid,
		Gid:        gid,
	}
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

	totalRead, err := fh.readFromChunks(ctx, buff, req.Offset)
	if err == nil {
		dirtyOffset, dirtySize, dirtyReadErr := fh.readFromDirtyPages(ctx, buff, req.Offset)
		if dirtyReadErr == nil && totalRead+req.Offset < dirtyOffset+int64(dirtySize) {
			totalRead = dirtyOffset + int64(dirtySize) - req.Offset
		}
	}

	resp.Data = buff[:totalRead]

	if err != nil {
		glog.Errorf("file handle read %s: %v", fh.f.fullpath(), err)
	}

	return err
}

func (fh *FileHandle) readFromDirtyPages(ctx context.Context, buff []byte, startOffset int64) (offset int64, size int, err error) {
	return fh.dirtyPages.ReadDirtyData(ctx, buff, startOffset)
}

func (fh *FileHandle) readFromChunks(ctx context.Context, buff []byte, offset int64) (int64, error) {

	// this value should come from the filer instead of the old f
	if len(fh.f.entry.Chunks) == 0 {
		glog.V(1).Infof("empty fh %v/%v", fh.f.dir.Path, fh.f.Name)
		return 0, nil
	}

	if fh.f.entryViewCache == nil {
		fh.f.entryViewCache = filer2.NonOverlappingVisibleIntervals(fh.f.entry.Chunks)
	}

	chunkViews := filer2.ViewFromVisibleIntervals(fh.f.entryViewCache, offset, len(buff))

	totalRead, err := filer2.ReadIntoBuffer(ctx, fh.f.wfs, fh.f.fullpath(), buff, chunkViews, offset)

	if err != nil {
		glog.Errorf("file handle read %s: %v", fh.f.fullpath(), err)
	}

	return totalRead, err
}

// Write to the file handle
func (fh *FileHandle) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {

	// write the request to volume servers

	glog.V(4).Infof("%+v/%v write fh %d: [%d,%d)", fh.f.dir.Path, fh.f.Name, fh.handle, req.Offset, req.Offset+int64(len(req.Data)))

	chunks, err := fh.dirtyPages.AddPage(ctx, req.Offset, req.Data)
	if err != nil {
		glog.Errorf("%+v/%v write fh %d: [%d,%d): %v", fh.f.dir.Path, fh.f.Name, fh.handle, req.Offset, req.Offset+int64(len(req.Data)), err)
		return fmt.Errorf("write %s/%s at [%d,%d): %v", fh.f.dir.Path, fh.f.Name, req.Offset, req.Offset+int64(len(req.Data)), err)
	}

	resp.Size = len(req.Data)

	if req.Offset == 0 {
		// detect mime type
		detectedMIME := mimetype.Detect(req.Data)
		fh.contentType = detectedMIME.String()
		if ext := path.Ext(fh.f.Name); ext != detectedMIME.Extension() {
			fh.contentType = mime.TypeByExtension(ext)
		}

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

	return nil
}

func (fh *FileHandle) Flush(ctx context.Context, req *fuse.FlushRequest) error {
	// fflush works at fh level
	// send the data to the OS
	glog.V(4).Infof("%s fh %d flush %v", fh.f.fullpath(), fh.handle, req)

	chunk, err := fh.dirtyPages.FlushToStorage(ctx)
	if err != nil {
		glog.Errorf("flush %s/%s: %v", fh.f.dir.Path, fh.f.Name, err)
		return fmt.Errorf("flush %s/%s: %v", fh.f.dir.Path, fh.f.Name, err)
	}

	fh.f.addChunk(chunk)

	if !fh.dirtyMetadata {
		return nil
	}

	err = fh.f.wfs.WithFilerClient(ctx, func(client filer_pb.SeaweedFilerClient) error {

		if fh.f.entry.Attributes != nil {
			fh.f.entry.Attributes.Mime = fh.contentType
			fh.f.entry.Attributes.Uid = req.Uid
			fh.f.entry.Attributes.Gid = req.Gid
			fh.f.entry.Attributes.Mtime = time.Now().Unix()
			fh.f.entry.Attributes.Crtime = time.Now().Unix()
			fh.f.entry.Attributes.FileMode = uint32(0777 &^ fh.f.wfs.option.Umask)
		}

		request := &filer_pb.CreateEntryRequest{
			Directory: fh.f.dir.Path,
			Entry:     fh.f.entry,
		}

		glog.V(3).Infof("%s/%s set chunks: %v", fh.f.dir.Path, fh.f.Name, len(fh.f.entry.Chunks))
		for i, chunk := range fh.f.entry.Chunks {
			glog.V(3).Infof("%s/%s chunks %d: %v [%d,%d)", fh.f.dir.Path, fh.f.Name, i, chunk.FileId, chunk.Offset, chunk.Offset+int64(chunk.Size))
		}

		chunks, garbages := filer2.CompactFileChunks(fh.f.entry.Chunks)
		fh.f.entry.Chunks = chunks
		// fh.f.entryViewCache = nil

		if _, err := client.CreateEntry(ctx, request); err != nil {
			glog.Errorf("update fh: %v", err)
			return fmt.Errorf("update fh: %v", err)
		}

		fh.f.wfs.deleteFileChunks(ctx, garbages)
		for i, chunk := range garbages {
			glog.V(3).Infof("garbage %s/%s chunks %d: %v [%d,%d)", fh.f.dir.Path, fh.f.Name, i, chunk.FileId, chunk.Offset, chunk.Offset+int64(chunk.Size))
		}

		return nil
	})

	if err == nil {
		fh.dirtyMetadata = false
	}

	return err
}
