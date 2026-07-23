package mount

import (
	"context"
	"fmt"
	"io"
	"sort"

	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
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

	// IsInRemoteOnly inspects entry.Chunks, so take the LockedEntry lock the
	// async uploader appends under.
	entry.RLock()
	remoteOnly := entry.Entry.IsInRemoteOnly()
	entry.RUnlock()
	if remoteOnly {
		glog.V(4).Infof("download remote entry %s", fileFullPath)
		err := fh.downloadRemoteEntry(entry)
		if err != nil {
			glog.V(1).Infof("download remote entry %s: %v", fileFullPath, err)
			return 0, 0, err
		}
	}

	// Snapshot size, inline content, and the chunk list under the LockedEntry
	// lock. Async upload workers append chunks under this lock (AddChunks), so
	// reading entry.Chunks / FileSize without it races with the slice
	// reallocation and can crash in filer.TotalSize. The captured slice headers
	// stay valid afterwards: append never mutates the old backing array, and
	// truncate is excluded by the fh.entryLock held for this whole read.
	entry.RLock()
	pbEntry := entry.Entry
	fileSize := int64(pbEntry.Attributes.FileSize)
	if fileSize == 0 {
		fileSize = int64(filer.FileSize(pbEntry))
	}
	content := pbEntry.Content
	chunks := pbEntry.Chunks
	entry.RUnlock()

	if fileSize == 0 {
		glog.V(1).Infof("empty fh %v", fileFullPath)
		return 0, 0, io.EOF
	} else if offset == fileSize {
		return 0, 0, io.EOF
	} else if offset >= fileSize {
		glog.V(1).Infof("invalid read, fileSize %d, offset %d for %s", fileSize, offset, fileFullPath)
		return 0, 0, io.EOF
	}

	if offset < int64(len(content)) {
		totalRead := copy(buff, content[offset:])
		glog.V(4).Infof("file handle read cached %s [%d,%d] %d", fileFullPath, offset, offset+int64(totalRead), totalRead)
		return int64(totalRead), 0, nil
	}

	// Try RDMA acceleration first if available
	if fh.wfs.rdmaClient != nil && fh.wfs.option.RdmaEnabled {
		totalRead, ts, err := fh.tryRDMARead(ctx, fileSize, buff, offset, chunks)
		if err == nil {
			glog.V(4).Infof("RDMA read successful for %s [%d,%d] %d", fileFullPath, offset, offset+int64(totalRead), totalRead)
			return int64(totalRead), ts, nil
		}
		glog.V(4).Infof("RDMA read failed for %s, falling back to HTTP: %v", fileFullPath, err)
	}

	// Peer chunk sharing: try a peer mount's cache before the volume tier.
	// Any failure falls through transparently. See design-weed-mount-
	// peer-chunk-sharing.md §4.3.
	if fh.wfs.option.PeerEnabled && fh.wfs.peerGrpcServer != nil {
		totalRead, ts, err := fh.tryPeerRead(ctx, fileSize, buff, offset, chunks)
		if err == nil {
			glog.V(4).Infof("peer read successful for %s [%d,%d] %d", fileFullPath, offset, offset+int64(totalRead), totalRead)
			return int64(totalRead), ts, nil
		}
		// Skip the "failed" log for benign skip reasons (local cache
		// hit, no peer owner yet, etc.) — the cache/volume fallback is
		// the expected outcome, not a failure.
		if err != errPeerReadSkipped {
			glog.V(4).Infof("peer read failed for %s, falling back to volume: %v", fileFullPath, err)
		}
	}

	// Fall back to normal chunk reading
	totalRead, ts, err := fh.entryChunkGroup.ReadDataAt(ctx, fileSize, buff, offset)

	if err != nil && err != io.EOF {
		glog.Errorf("file handle read %s: %v", fileFullPath, err)
	}

	// glog.V(4).Infof("file handle read %s [%d,%d] %d : %v", fileFullPath, offset, offset+int64(totalRead), totalRead, err)

	return int64(totalRead), ts, err
}

// tryRDMARead attempts to read file data using RDMA acceleration. chunks is a
// snapshot captured under the LockedEntry lock by the caller.
func (fh *FileHandle) tryRDMARead(ctx context.Context, fileSize int64, buff []byte, offset int64, chunks []*filer_pb.FileChunk) (int64, int64, error) {
	// For now, we'll try to read the chunks directly using RDMA
	// This is a simplified approach - in a full implementation, we'd need to
	// handle chunk boundaries, multiple chunks, etc.

	if len(chunks) == 0 {
		return 0, 0, fmt.Errorf("no chunks available for RDMA read")
	}

	// Find the chunk that contains our offset using binary search
	var targetChunk *filer_pb.FileChunk
	var chunkOffset int64

	// Get cached cumulative offsets for efficient binary search
	cumulativeOffsets := fh.getCumulativeOffsets(chunks)

	// Use binary search to find the chunk containing the offset
	chunkIndex := sort.Search(len(chunks), func(i int) bool {
		return offset < cumulativeOffsets[i+1]
	})

	// Verify the chunk actually contains our offset
	if chunkIndex < len(chunks) && offset >= cumulativeOffsets[chunkIndex] {
		targetChunk = chunks[chunkIndex]
		chunkOffset = offset - cumulativeOffsets[chunkIndex]
	}

	if targetChunk == nil {
		return 0, 0, fmt.Errorf("no chunk found for offset %d", offset)
	}

	// Calculate how much to read from this chunk
	remainingInChunk := int64(targetChunk.Size) - chunkOffset
	readSize := min(int64(len(buff)), remainingInChunk)

	glog.V(4).Infof("RDMA read attempt: chunk=%s (fileId=%s), chunkOffset=%d, readSize=%d",
		targetChunk.FileId, targetChunk.FileId, chunkOffset, readSize)

	// Try RDMA read using file ID directly (more efficient)
	data, isRDMA, err := fh.wfs.rdmaClient.ReadNeedle(ctx, targetChunk.FileId, uint64(chunkOffset), uint64(readSize))
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

		// The handle's live entry and base are kept in local uid/gid form, like
		// every other install path; the caching response carries filer-side
		// ids. Mapping the base too keeps proto.Equal(candidate, base) meaningful
		// under a non-identity UidGidMapper — otherwise an unchanged re-delivery
		// would look foreign and force-destroy dirty pages.
		localEntry := proto.Clone(resp.Entry).(*filer_pb.Entry)
		if localEntry.Attributes != nil && fh.wfs.option.UidGidMapper != nil {
			localEntry.Attributes.Uid, localEntry.Attributes.Gid = fh.wfs.option.UidGidMapper.FilerToLocal(localEntry.Attributes.Uid, localEntry.Attributes.Gid)
		}

		// The response state is versioned by the caching event when the filer
		// performed the download, or by the response's log position when the
		// object was already cached.
		versionTsNs := ackVersionTsNs(resp)

		// Serialize the entry/base/version install: this runs under the file
		// handle's shared lock, so a second concurrent read can be here too;
		// invalidateOpenFileHandle is excluded by the exclusive lock, but two
		// downloads are not excluded from each other.
		fh.remoteInstallMu.Lock()
		fh.SetEntry(localEntry)
		fh.setAuthoritativeBase(proto.Clone(localEntry).(*filer_pb.Entry))
		fh.advanceEntryVersionTsNs(versionTsNs)
		fh.remoteInstallMu.Unlock()

		// Async: a sync apply deadlocks against the apply loop's invalidate, which needs this read's file-handle lock.
		event := resp.GetMetadataEvent()
		if event == nil {
			event = metadataUpdateEvent(request.Directory, resp.Entry)
		}
		fh.wfs.applyLocalMetadataEventAsync(event)

		return nil
	})

	return err
}
