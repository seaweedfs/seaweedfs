package mount

import (
	"context"
	"fmt"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

func (fh *FileHandle) lockForRead(startOffset int64, size int) {
	fh.dirtyPages.LockForRead(startOffset, startOffset+int64(size))
}
func (fh *FileHandle) unlockForRead(startOffset int64, size int) {
	fh.dirtyPages.UnlockForRead(startOffset, startOffset+int64(size))
}

func (fh *FileHandle) readFromDirtyPages(buff []byte, startOffset int64, tsNs int64) (maxStop int64) {
	maxStop = fh.dirtyPages.ReadDirtyDataAt(buff, startOffset, tsNs)
	return
}

func (fh *FileHandle) readFromChunks(buff []byte, offset int64) (int64, int64, error) {
	return fh.readFromChunksWithContext(context.Background(), buff, offset)
}

func (fh *FileHandle) readFromChunksWithContext(ctx context.Context, buff []byte, offset int64) (int64, int64, error) {
	fh.entryLock.RLock()
	defer fh.entryLock.RUnlock()

	fileFullPath := fh.FullPath()

	entry := fh.GetEntry()

	if entry.IsInRemoteOnly() {
		glog.V(4).Infof("download remote entry %s", fileFullPath)
		err := fh.downloadRemoteEntry(entry)
		if err != nil {
			glog.V(1).Infof("download remote entry %s: %v", fileFullPath, err)
			return 0, 0, err
		}
	}

	fileSize := int64(entry.Attributes.FileSize)
	if fileSize == 0 {
		fileSize = int64(filer.FileSize(entry.GetEntry()))
	}

	if fileSize == 0 {
		glog.V(1).Infof("empty fh %v", fileFullPath)
		return 0, 0, io.EOF
	} else if offset == fileSize {
		return 0, 0, io.EOF
	} else if offset >= fileSize {
		glog.V(1).Infof("invalid read, fileSize %d, offset %d for %s", fileSize, offset, fileFullPath)
		return 0, 0, io.EOF
	}

	if offset < int64(len(entry.Content)) {
		totalRead := copy(buff, entry.Content[offset:])
		glog.V(4).Infof("file handle read cached %s [%d,%d] %d", fileFullPath, offset, offset+int64(totalRead), totalRead)
		return int64(totalRead), 0, nil
	}

	// Try RDMA acceleration first if available
	if fh.wfs.rdmaClient != nil && fh.wfs.option.RdmaEnabled {
		totalRead, ts, err := fh.tryRDMARead(ctx, fileSize, buff, offset, entry)
		if err == nil {
			glog.V(4).Infof("RDMA read successful for %s [%d,%d] %d", fileFullPath, offset, offset+int64(totalRead), totalRead)
			return int64(totalRead), ts, nil
		}
		glog.V(4).Infof("RDMA read failed for %s, falling back to HTTP: %v", fileFullPath, err)
	}

	// Fall back to normal chunk reading
	totalRead, ts, err := fh.entryChunkGroup.ReadDataAt(ctx, fileSize, buff, offset)

	if err != nil && err != io.EOF {
		glog.Errorf("file handle read %s: %v", fileFullPath, err)
	}

	// glog.V(4).Infof("file handle read %s [%d,%d] %d : %v", fileFullPath, offset, offset+int64(totalRead), totalRead, err)

	return int64(totalRead), ts, err
}

// tryRDMARead attempts to read file data using RDMA acceleration
func (fh *FileHandle) tryRDMARead(ctx context.Context, fileSize int64, buff []byte, offset int64, entry *LockedEntry) (int64, int64, error) {
	// For now, we'll try to read the chunks directly using RDMA
	// This is a simplified approach - in a full implementation, we'd need to
	// handle chunk boundaries, multiple chunks, etc.

	chunks := entry.GetEntry().Chunks
	if len(chunks) == 0 {
		return 0, 0, fmt.Errorf("no chunks available for RDMA read")
	}

	// Find the chunk that contains our offset
	var targetChunk *filer_pb.FileChunk
	var chunkOffset int64
	currentOffset := int64(0)

	for _, chunk := range chunks {
		chunkEnd := currentOffset + int64(chunk.Size)
		if offset >= currentOffset && offset < chunkEnd {
			targetChunk = chunk
			chunkOffset = offset - currentOffset
			break
		}
		currentOffset = chunkEnd
	}

	if targetChunk == nil {
		return 0, 0, fmt.Errorf("no chunk found for offset %d", offset)
	}

	// Parse the chunk's file ID
	volumeID, needleID, cookie, err := fh.parseFileId(targetChunk.FileId)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to parse chunk fileId %s: %w", targetChunk.FileId, err)
	}

	// Calculate how much to read from this chunk
	remainingInChunk := int64(targetChunk.Size) - chunkOffset
	readSize := min(int64(len(buff)), remainingInChunk)

	glog.V(4).Infof("RDMA read attempt: chunk=%s, volume=%d, needle=%x, cookie=%d, chunkOffset=%d, readSize=%d",
		targetChunk.FileId, volumeID, needleID, cookie, chunkOffset, readSize)

	// Try RDMA read
	data, isRDMA, err := fh.wfs.rdmaClient.ReadNeedle(ctx, volumeID, needleID, cookie, uint64(chunkOffset), uint64(readSize))
	if err != nil {
		return 0, 0, fmt.Errorf("RDMA read failed: %w", err)
	}

	if !isRDMA {
		return 0, 0, fmt.Errorf("RDMA not available for chunk")
	}

	// Copy data to buffer
	copied := copy(buff, data)
	return int64(copied), targetChunk.ModifiedTsNs, nil
}

// parseFileId parses a SeaweedFS fileId into volume, needle, and cookie
func (fh *FileHandle) parseFileId(fileId string) (volumeID uint32, needleID uint64, cookie uint32, err error) {
	// Use existing SeaweedFS file ID parsing
	fid, err := needle.ParseFileIdFromString(fileId)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("failed to parse file ID %s: %w", fileId, err)
	}

	volumeID = uint32(fid.VolumeId)
	needleID = uint64(fid.Key)
	cookie = uint32(fid.Cookie)

	return volumeID, needleID, cookie, nil
}



func (fh *FileHandle) downloadRemoteEntry(entry *LockedEntry) error {

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

		fh.SetEntry(resp.Entry)

		fh.wfs.metaCache.InsertEntry(context.Background(), filer.FromPbEntry(request.Directory, resp.Entry))

		return nil
	})

	return err
}
