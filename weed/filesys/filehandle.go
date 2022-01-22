package filesys

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/seaweedfs/fuse"
	"github.com/seaweedfs/fuse/fs"

	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
)

type FileHandle struct {
	// cache file has been written to
	dirtyPages     *PageWriter
	entryViewCache []filer.VisibleInterval
	reader         io.ReaderAt
	contentType    string
	handle         uint64
	sync.Mutex

	f         *File
	NodeId    fuse.NodeID // file or directory the request is about
	Uid       uint32      // user ID of process making request
	Gid       uint32      // group ID of process making request
	isDeleted bool
}

func newFileHandle(file *File, uid, gid uint32) *FileHandle {
	fh := &FileHandle{
		f:   file,
		Uid: uid,
		Gid: gid,
	}
	// dirtyPages: newContinuousDirtyPages(file, writeOnly),
	fh.dirtyPages = newPageWriter(fh, file.wfs.option.ChunkSizeLimit)
	entry := fh.f.getEntry()
	if entry != nil {
		entry.Attributes.FileSize = filer.FileSize(entry)
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

	fh.Lock()
	defer fh.Unlock()

	glog.V(4).Infof("%s read fh %d: [%d,%d) size %d resp.Data cap=%d", fh.f.fullpath(), fh.handle, req.Offset, req.Offset+int64(req.Size), req.Size, cap(resp.Data))

	if req.Size <= 0 {
		return nil
	}

	buff := resp.Data[:cap(resp.Data)]
	if req.Size > cap(resp.Data) {
		// should not happen
		buff = make([]byte, req.Size)
	}

	fh.lockForRead(req.Offset, len(buff))
	defer fh.unlockForRead(req.Offset, len(buff))
	totalRead, err := fh.readFromChunks(buff, req.Offset)
	if err == nil || err == io.EOF {
		maxStop := fh.readFromDirtyPages(buff, req.Offset)
		totalRead = max(maxStop-req.Offset, totalRead)
	}

	if err == io.EOF {
		err = nil
	}

	if err != nil {
		glog.Warningf("file handle read %s %d: %v", fh.f.fullpath(), totalRead, err)
		return fuse.EIO
	}

	if totalRead > int64(len(buff)) {
		glog.Warningf("%s FileHandle Read %d: [%d,%d) size %d totalRead %d", fh.f.fullpath(), fh.handle, req.Offset, req.Offset+int64(req.Size), req.Size, totalRead)
		totalRead = min(int64(len(buff)), totalRead)
	}
	if err == nil {
		resp.Data = buff[:totalRead]
	}

	return err
}

func (fh *FileHandle) lockForRead(startOffset int64, size int) {
	fh.dirtyPages.LockForRead(startOffset, startOffset+int64(size))
}
func (fh *FileHandle) unlockForRead(startOffset int64, size int) {
	fh.dirtyPages.UnlockForRead(startOffset, startOffset+int64(size))
}

func (fh *FileHandle) readFromDirtyPages(buff []byte, startOffset int64) (maxStop int64) {
	maxStop = fh.dirtyPages.ReadDirtyDataAt(buff, startOffset)
	return
}

func (fh *FileHandle) readFromChunks(buff []byte, offset int64) (int64, error) {

	entry := fh.f.getEntry()
	if entry == nil {
		return 0, io.EOF
	}

	if entry.IsInRemoteOnly() {
		glog.V(4).Infof("download remote entry %s", fh.f.fullpath())
		newEntry, err := fh.f.downloadRemoteEntry(entry)
		if err != nil {
			glog.V(1).Infof("download remote entry %s: %v", fh.f.fullpath(), err)
			return 0, err
		}
		entry = newEntry
	}

	fileSize := int64(filer.FileSize(entry))
	fileFullPath := fh.f.fullpath()

	if fileSize == 0 {
		glog.V(1).Infof("empty fh %v", fileFullPath)
		return 0, io.EOF
	}

	if offset+int64(len(buff)) <= int64(len(entry.Content)) {
		totalRead := copy(buff, entry.Content[offset:])
		glog.V(4).Infof("file handle read cached %s [%d,%d] %d", fileFullPath, offset, offset+int64(totalRead), totalRead)
		return int64(totalRead), nil
	}

	var chunkResolveErr error
	if fh.entryViewCache == nil {
		fh.entryViewCache, chunkResolveErr = filer.NonOverlappingVisibleIntervals(fh.f.wfs.LookupFn(), entry.Chunks, 0, math.MaxInt64)
		if chunkResolveErr != nil {
			return 0, fmt.Errorf("fail to resolve chunk manifest: %v", chunkResolveErr)
		}
		fh.reader = nil
	}

	reader := fh.reader
	if reader == nil {
		chunkViews := filer.ViewFromVisibleIntervals(fh.entryViewCache, 0, math.MaxInt64)
		glog.V(4).Infof("file handle read %s [%d,%d) from %d views", fileFullPath, offset, offset+int64(len(buff)), len(chunkViews))
		for _, chunkView := range chunkViews {
			glog.V(4).Infof("  read %s [%d,%d) from chunk %+v", fileFullPath, chunkView.LogicOffset, chunkView.LogicOffset+int64(chunkView.Size), chunkView.FileId)
		}
		reader = filer.NewChunkReaderAtFromClient(fh.f.wfs.LookupFn(), chunkViews, fh.f.wfs.chunkCache, fileSize)
	}
	fh.reader = reader

	totalRead, err := reader.ReadAt(buff, offset)

	if err != nil && err != io.EOF {
		glog.Errorf("file handle read %s: %v", fileFullPath, err)
	}

	glog.V(4).Infof("file handle read %s [%d,%d] %d : %v", fileFullPath, offset, offset+int64(totalRead), totalRead, err)

	return int64(totalRead), err
}

// Write to the file handle
func (fh *FileHandle) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {

	fh.dirtyPages.writerPattern.MonitorWriteAt(req.Offset, len(req.Data))

	fh.Lock()
	defer fh.Unlock()

	// write the request to volume servers
	data := req.Data
	if len(data) <= 512 && req.Offset == 0 {
		// fuse message cacheable size
		data = make([]byte, len(req.Data))
		copy(data, req.Data)
	}

	entry := fh.f.getEntry()
	if entry == nil {
		return fuse.EIO
	}

	entry.Content = nil
	entry.Attributes.FileSize = uint64(max(req.Offset+int64(len(data)), int64(entry.Attributes.FileSize)))
	// glog.V(4).Infof("%v write [%d,%d) %d", fh.f.fullpath(), req.Offset, req.Offset+int64(len(req.Data)), len(req.Data))

	fh.dirtyPages.AddPage(req.Offset, data)

	resp.Size = len(data)

	if req.Offset == 0 {
		// detect mime type
		fh.contentType = http.DetectContentType(data)
		fh.f.dirtyMetadata = true
	}

	fh.f.dirtyMetadata = true

	return nil
}

func (fh *FileHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {

	glog.V(4).Infof("Release %v fh %d open=%d", fh.f.fullpath(), fh.handle, fh.f.isOpen)

	fh.f.wfs.handlesLock.Lock()
	fh.f.isOpen--
	fh.f.wfs.handlesLock.Unlock()

	if fh.f.isOpen <= 0 {
		fh.f.entry = nil
		fh.entryViewCache = nil
		fh.reader = nil

		fh.f.wfs.ReleaseHandle(fh.f.fullpath(), fuse.HandleID(fh.handle))
		fh.dirtyPages.Destroy()
	}

	if fh.f.isOpen < 0 {
		glog.V(0).Infof("Release reset %s open count %d => %d", fh.f.Name, fh.f.isOpen, 0)
		fh.f.isOpen = 0
		return nil
	}

	return nil
}

func (fh *FileHandle) Flush(ctx context.Context, req *fuse.FlushRequest) error {

	glog.V(4).Infof("Flush %v fh %d", fh.f.fullpath(), fh.handle)

	if fh.isDeleted {
		glog.V(4).Infof("Flush %v fh %d skip deleted", fh.f.fullpath(), fh.handle)
		return nil
	}

	fh.Lock()
	defer fh.Unlock()

	if err := fh.doFlush(ctx, req.Header); err != nil {
		glog.Errorf("Flush doFlush %s: %v", fh.f.Name, err)
		return err
	}

	return nil
}

func (fh *FileHandle) doFlush(ctx context.Context, header fuse.Header) error {
	// flush works at fh level
	// send the data to the OS
	glog.V(4).Infof("doFlush %s fh %d", fh.f.fullpath(), fh.handle)

	if err := fh.dirtyPages.FlushData(); err != nil {
		glog.Errorf("%v doFlush: %v", fh.f.fullpath(), err)
		return fuse.EIO
	}

	if !fh.f.dirtyMetadata {
		return nil
	}

	err := fh.f.wfs.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {

		entry := fh.f.getEntry()
		if entry == nil {
			return nil
		}

		if entry.Attributes != nil {
			entry.Attributes.Mime = fh.contentType
			if entry.Attributes.Uid == 0 {
				entry.Attributes.Uid = header.Uid
			}
			if entry.Attributes.Gid == 0 {
				entry.Attributes.Gid = header.Gid
			}
			if entry.Attributes.Crtime == 0 {
				entry.Attributes.Crtime = time.Now().Unix()
			}
			entry.Attributes.Mtime = time.Now().Unix()
			entry.Attributes.FileMode = uint32(os.FileMode(entry.Attributes.FileMode) &^ fh.f.wfs.option.Umask)
			entry.Attributes.Collection, entry.Attributes.Replication = fh.dirtyPages.GetStorageOptions()
		}

		request := &filer_pb.CreateEntryRequest{
			Directory:  fh.f.dir.FullPath(),
			Entry:      entry,
			Signatures: []int32{fh.f.wfs.signature},
		}

		glog.V(4).Infof("%s set chunks: %v", fh.f.fullpath(), len(entry.Chunks))
		for i, chunk := range entry.Chunks {
			glog.V(4).Infof("%s chunks %d: %v [%d,%d)", fh.f.fullpath(), i, chunk.GetFileIdString(), chunk.Offset, chunk.Offset+int64(chunk.Size))
		}

		manifestChunks, nonManifestChunks := filer.SeparateManifestChunks(entry.Chunks)

		chunks, _ := filer.CompactFileChunks(fh.f.wfs.LookupFn(), nonManifestChunks)
		chunks, manifestErr := filer.MaybeManifestize(fh.f.wfs.saveDataAsChunk(fh.f.fullpath()), chunks)
		if manifestErr != nil {
			// not good, but should be ok
			glog.V(0).Infof("MaybeManifestize: %v", manifestErr)
		}
		entry.Chunks = append(chunks, manifestChunks...)

		fh.f.wfs.mapPbIdFromLocalToFiler(request.Entry)
		defer fh.f.wfs.mapPbIdFromFilerToLocal(request.Entry)

		if err := filer_pb.CreateEntry(client, request); err != nil {
			glog.Errorf("fh flush create %s: %v", fh.f.fullpath(), err)
			return fmt.Errorf("fh flush create %s: %v", fh.f.fullpath(), err)
		}

		fh.f.wfs.metaCache.InsertEntry(context.Background(), filer.FromPbEntry(request.Directory, request.Entry))

		return nil
	})

	if err == nil {
		fh.f.dirtyMetadata = false
	}

	if err != nil {
		glog.Errorf("%v fh %d flush: %v", fh.f.fullpath(), fh.handle, err)
		return fuse.EIO
	}

	return nil
}
