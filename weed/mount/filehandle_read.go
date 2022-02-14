package mount

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"io"
	"math"
)

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

	fileFullPath := fh.FullPath()

	entry := fh.entry
	if entry == nil {
		return 0, io.EOF
	}

	if entry.IsInRemoteOnly() {
		glog.V(4).Infof("download remote entry %s", fileFullPath)
		newEntry, err := fh.downloadRemoteEntry(entry)
		if err != nil {
			glog.V(1).Infof("download remote entry %s: %v", fileFullPath, err)
			return 0, err
		}
		entry = newEntry
	}

	fileSize := int64(filer.FileSize(entry))

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
		fh.entryViewCache, chunkResolveErr = filer.NonOverlappingVisibleIntervals(fh.wfs.LookupFn(), entry.Chunks, 0, math.MaxInt64)
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
		reader = filer.NewChunkReaderAtFromClient(fh.wfs.LookupFn(), chunkViews, fh.wfs.chunkCache, fileSize)
	}
	fh.reader = reader

	totalRead, err := reader.ReadAt(buff, offset)

	if err != nil && err != io.EOF {
		glog.Errorf("file handle read %s: %v", fileFullPath, err)
	}

	glog.V(4).Infof("file handle read %s [%d,%d] %d : %v", fileFullPath, offset, offset+int64(totalRead), totalRead, err)

	return int64(totalRead), err
}

func (fh *FileHandle) downloadRemoteEntry(entry *filer_pb.Entry) (*filer_pb.Entry, error) {

	fileFullPath := fh.FullPath()
	dir, _ := fileFullPath.DirAndName()

	err := fh.wfs.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.CacheRemoteObjectToLocalClusterRequest{
			Directory: string(dir),
			Name:      entry.Name,
		}

		glog.V(4).Infof("download entry: %v", request)
		resp, err := client.CacheRemoteObjectToLocalCluster(context.Background(), request)
		if err != nil {
			return fmt.Errorf("CacheRemoteObjectToLocalCluster file %s: %v", fileFullPath, err)
		}

		entry = resp.Entry

		fh.wfs.metaCache.InsertEntry(context.Background(), filer.FromPbEntry(request.Directory, resp.Entry))

		return nil
	})

	return entry, err
}
