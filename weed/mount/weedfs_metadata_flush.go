package mount

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// loopFlushDirtyMetadata periodically flushes dirty file metadata to the filer.
// This protects newly uploaded chunks from being purged by volume.fsck orphan cleanup
// for files that remain open for extended periods without being closed.
//
// The problem: When a file is opened and written to continuously, chunks are uploaded
// to volume servers but the file metadata (containing chunk references) is only saved
// to the filer on file close or fsync. If volume.fsck runs during this window, it may
// identify these chunks as orphans (since they're not referenced in filer metadata)
// and purge them.
//
// This background task periodically flushes metadata for open files, ensuring chunk
// references are visible to volume.fsck even before files are closed.
func (wfs *WFS) loopFlushDirtyMetadata() {
	if wfs.option.MetadataFlushSeconds <= 0 {
		glog.V(0).Infof("periodic metadata flush disabled")
		return
	}

	flushInterval := time.Duration(wfs.option.MetadataFlushSeconds) * time.Second
	glog.V(0).Infof("periodic metadata flush enabled, interval: %v", flushInterval)

	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	for range ticker.C {
		wfs.flushAllDirtyMetadata()
	}
}

// flushAllDirtyMetadata iterates through all open file handles and flushes
// metadata for files that have dirty metadata (chunks uploaded but not yet persisted).
func (wfs *WFS) flushAllDirtyMetadata() {
	// Collect file handles with dirty metadata under a read lock
	var dirtyHandles []*FileHandle
	wfs.fhMap.RLock()
	for _, fh := range wfs.fhMap.inode2fh {
		if fh.dirtyMetadata {
			dirtyHandles = append(dirtyHandles, fh)
		}
	}
	wfs.fhMap.RUnlock()

	if len(dirtyHandles) == 0 {
		return
	}

	glog.V(3).Infof("flushing metadata for %d open files", len(dirtyHandles))

	// Process dirty handles in parallel with limited concurrency
	var wg sync.WaitGroup
	concurrency := wfs.option.ConcurrentWriters
	if concurrency <= 0 {
		concurrency = 16
	}
	sem := make(chan struct{}, concurrency)

	for _, fh := range dirtyHandles {
		wg.Add(1)
		sem <- struct{}{}
		go func(handle *FileHandle) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := wfs.flushFileMetadata(handle); err != nil {
				glog.Warningf("failed to flush metadata for %s: %v", handle.FullPath(), err)
			}
		}(fh)
	}
	wg.Wait()
}

// flushFileMetadata flushes the current file metadata to the filer without
// flushing dirty pages from memory. This updates chunk references in the filer
// so volume.fsck can see them, while keeping data in the write buffer.
func (wfs *WFS) flushFileMetadata(fh *FileHandle) error {
	// Acquire exclusive lock on the file handle
	fhActiveLock := fh.wfs.fhLockTable.AcquireLock("flushMetadata", fh.fh, util.ExclusiveLock)
	defer fh.wfs.fhLockTable.ReleaseLock(fh.fh, fhActiveLock)

	// Double-check dirty flag under lock
	if !fh.dirtyMetadata {
		return nil
	}

	fileFullPath := fh.FullPath()
	dir, name := fileFullPath.DirAndName()

	glog.V(4).Infof("flushFileMetadata %s fh %d", fileFullPath, fh.fh)

	err := wfs.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		entry := fh.GetEntry()
		if entry == nil {
			return nil
		}
		entry.Name = name

		if entry.Attributes != nil {
			entry.Attributes.Mtime = time.Now().Unix()
		}

		// Get current chunks - these include chunks that have been uploaded
		// but not yet persisted to filer metadata
		chunks := entry.GetChunks()
		if len(chunks) == 0 {
			return nil
		}

		// Separate manifest and non-manifest chunks
		manifestChunks, nonManifestChunks := filer.SeparateManifestChunks(chunks)

		// Compact chunks to remove fully overlapped ones
		compactedChunks, _ := filer.CompactFileChunks(context.Background(), wfs.LookupFn(), nonManifestChunks)

		// Try to create manifest chunks for large files
		compactedChunks, manifestErr := filer.MaybeManifestize(wfs.saveDataAsChunk(fileFullPath), compactedChunks)
		if manifestErr != nil {
			glog.V(0).Infof("flushFileMetadata MaybeManifestize: %v", manifestErr)
		}

		entry.Chunks = append(compactedChunks, manifestChunks...)

		request := &filer_pb.CreateEntryRequest{
			Directory:                string(dir),
			Entry:                    entry.GetEntry(),
			Signatures:               []int32{wfs.signature},
			SkipCheckParentDirectory: true,
		}

		wfs.mapPbIdFromLocalToFiler(request.Entry)
		defer wfs.mapPbIdFromFilerToLocal(request.Entry)

		if err := filer_pb.CreateEntry(context.Background(), client, request); err != nil {
			return err
		}

		// Update meta cache
		if err := wfs.metaCache.InsertEntry(context.Background(), filer.FromPbEntry(request.Directory, request.Entry)); err != nil {
			return fmt.Errorf("update meta cache for %s: %w", fileFullPath, err)
		}

		glog.V(3).Infof("flushed metadata for %s with %d chunks", fileFullPath, len(entry.GetChunks()))
		return nil
	})

	if err != nil {
		return err
	}

	// Note: We do NOT clear dirtyMetadata here because:
	// 1. There may still be dirty pages in the write buffer
	// 2. The file may receive more writes before close
	// 3. dirtyMetadata will be cleared on the final flush when the file is closed

	return nil
}
