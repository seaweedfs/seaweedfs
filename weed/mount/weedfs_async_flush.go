package mount

import (
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const asyncFlushMetadataRetries = 3

// completeAsyncFlush is called in a background goroutine when a file handle
// with pending async flush work is released. It performs the deferred data
// upload and metadata flush that was skipped in doFlush() for writebackCache mode.
//
// This enables close() to return immediately for small file workloads (e.g., rsync),
// while the actual I/O happens concurrently in the background.
func (wfs *WFS) completeAsyncFlush(fh *FileHandle) {
	defer wfs.asyncFlushWg.Done()

	fileFullPath := fh.FullPath()
	dir, name := fileFullPath.DirAndName()

	glog.V(3).Infof("completeAsyncFlush start %s fh %d", fileFullPath, fh.fh)

	// Phase 1: Flush dirty pages — seals writable chunks, uploads to volume servers, and waits.
	// The underlying UploadWithRetry already retries transient HTTP/gRPC errors internally,
	// so a failure here indicates a persistent issue; the chunk data has been freed.
	if err := fh.dirtyPages.FlushData(); err != nil {
		glog.Errorf("completeAsyncFlush %s: data flush failed: %v", fileFullPath, err)
		// Data is lost at this point (chunks freed after internal retry exhaustion).
		// Proceed to cleanup to avoid resource leaks and unmount hangs.
	} else if fh.dirtyMetadata {
		// Phase 2: Flush metadata to filer with the uploaded chunk references.
		// This is retried because the chunks are safely on volume servers — only the
		// filer metadata reference needs to be persisted. A transient gRPC error here
		// would otherwise cause uploaded chunks to become orphans.
		wfs.flushMetadataWithRetry(fh, dir, name, fileFullPath)
	}

	glog.V(3).Infof("completeAsyncFlush done %s fh %d", fileFullPath, fh.fh)

	// Phase 3: Destroy the upload pipeline and free resources.
	fh.ReleaseHandle()
}

// flushMetadataWithRetry attempts to flush file metadata to the filer, retrying
// with exponential backoff on transient errors. The chunk data is already on the
// volume servers at this point; only the filer metadata reference needs persisting.
func (wfs *WFS) flushMetadataWithRetry(fh *FileHandle, dir, name string, fileFullPath util.FullPath) {
	for attempt := 0; attempt <= asyncFlushMetadataRetries; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(1<<uint(attempt-1)) * time.Second
			glog.Warningf("completeAsyncFlush %s: retrying metadata flush (attempt %d/%d) after %v",
				fileFullPath, attempt+1, asyncFlushMetadataRetries+1, backoff)
			time.Sleep(backoff)
		}

		if err := wfs.flushMetadataToFiler(fh, dir, name, fh.asyncFlushUid, fh.asyncFlushGid); err != nil {
			if attempt == asyncFlushMetadataRetries {
				glog.Errorf("completeAsyncFlush %s: metadata flush failed after %d attempts: %v — "+
					"chunks are uploaded but NOT referenced in filer metadata; "+
					"they will appear as orphans in volume.fsck",
					fileFullPath, asyncFlushMetadataRetries+1, err)
			}
			continue
		}
		return // success
	}
}

// WaitForAsyncFlush waits for all pending background flush goroutines to complete.
// Called before unmount cleanup to ensure no data is lost.
func (wfs *WFS) WaitForAsyncFlush() {
	wfs.asyncFlushWg.Wait()
}
