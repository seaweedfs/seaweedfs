package mount

import (
	"context"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/protobuf/proto"
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
//
// When -dlm is enabled, the distributed lock is already held by the FileHandle
// from open-for-write through close, so no additional distributed lock is
// needed here. The local fhLockTable lock below serializes within this mount.
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

	if fh.GetEntry() == nil {
		return nil
	}

	// Snapshot the current chunk list. Async uploader goroutines call
	// entry.AppendChunks while we run CompactFileChunks / MaybeManifestize
	// below — those steps can take seconds (manifest upload is a round trip).
	// We must remember the snapshot length so we can splice any chunks that
	// land after the snapshot back in once we reassign entry.Chunks; without
	// that, the naked overwrite below clobbers them and the file ends up
	// missing chunk references for data the volumes already store.
	var snapshotChunks []*filer_pb.FileChunk
	var snapshotLen int
	fh.UpdateEntry(func(e *filer_pb.Entry) {
		// Do not stamp mtime/ctime here. Write/SetAttr already maintain
		// them on the entry; overwriting at periodic-flush time clobbered
		// user-set mtime (utimes/touch -m -d) once the timer fired.
		e.Name = name
		snapshotLen = len(e.Chunks)
		if snapshotLen > 0 {
			snapshotChunks = append([]*filer_pb.FileChunk(nil), e.Chunks...)
		}
	})

	if snapshotLen == 0 {
		return nil
	}

	// Separate manifest and non-manifest chunks
	manifestChunks, nonManifestChunks := filer.SeparateManifestChunks(snapshotChunks)

	// Compact chunks to remove fully overlapped ones
	compactedChunks, _ := filer.CompactFileChunks(context.Background(), wfs.LookupFn(), nonManifestChunks)

	// Try to create manifest chunks for large files
	compactedChunks, manifestErr := filer.MaybeManifestize(wfs.saveDataAsChunk(fileFullPath), compactedChunks)
	if manifestErr != nil {
		glog.V(0).Infof("flushFileMetadata MaybeManifestize: %v", manifestErr)
	}

	processedPrefix := append(compactedChunks, manifestChunks...)

	// Splice the processed snapshot back in, preserving any chunks that
	// async uploaders appended after our snapshot, and clone the resulting
	// entry for the filer request while still holding the lock so the
	// request can't observe a half-merged state.
	var requestEntry *filer_pb.Entry
	fh.UpdateEntry(func(e *filer_pb.Entry) {
		// e.Chunks[snapshotLen:] is whatever async uploaders appended while
		// we processed the snapshot. processedPrefix is freshly built and not
		// referenced elsewhere, so we can append straight onto it.
		var tail []*filer_pb.FileChunk
		if len(e.Chunks) > snapshotLen {
			tail = e.Chunks[snapshotLen:]
		}
		e.Chunks = append(processedPrefix, tail...)
		requestEntry = proto.Clone(e).(*filer_pb.Entry)
	})
	request := &filer_pb.CreateEntryRequest{
		Directory:                string(dir),
		Entry:                    requestEntry,
		Signatures:               []int32{wfs.signature},
		SkipCheckParentDirectory: true,
	}

	// Snapshot with local ids before the request mapping mutates the clone:
	// on ack this becomes the handle's base, judged against future events.
	baseSnapshot := proto.Clone(requestEntry).(*filer_pb.Entry)
	wfs.mapPbIdFromLocalToFiler(request.Entry)

	resp, err := wfs.streamCreateEntry(context.Background(), request)
	if err != nil {
		return err
	}

	event := resp.GetMetadataEvent()
	if event == nil {
		event = metadataUpdateEvent(string(dir), request.Entry)
		if event != nil {
			event.TsNs = ackVersionTsNs(resp)
		}
	}
	fh.setAuthoritativeBase(baseSnapshot)
	fh.advanceEntryVersionTsNs(ackVersionTsNs(resp))
	if applyErr := wfs.applyLocalMetadataEvent(context.Background(), event); applyErr != nil {
		glog.Warningf("flushFileMetadata %s: best-effort metadata apply failed: %v", fileFullPath, applyErr)
		wfs.inodeToPath.InvalidateChildrenCache(util.FullPath(dir))
	}

	glog.V(3).Infof("flushed metadata for %s with %d chunks", fileFullPath, len(requestEntry.GetChunks()))

	// Note: We do NOT clear dirtyMetadata here because:
	// 1. There may still be dirty pages in the write buffer
	// 2. The file may receive more writes before close
	// 3. dirtyMetadata will be cleared on the final flush when the file is closed

	return nil
}
