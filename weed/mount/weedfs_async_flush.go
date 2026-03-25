package mount

import (
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// asyncFlushItem holds the data needed for a background flush work item.
type asyncFlushItem struct {
	fh   *FileHandle
	done chan struct{}
}

// startAsyncFlushWorkers launches a fixed pool of goroutines that process
// background flush work items from asyncFlushCh. This bounds the number of
// concurrent flush operations to prevent resource exhaustion (connections,
// goroutines) when many files are closed rapidly (e.g., cp -r with writebackCache).
func (wfs *WFS) startAsyncFlushWorkers(numWorkers int) {
	wfs.asyncFlushCh = make(chan *asyncFlushItem, numWorkers*4)
	for i := 0; i < numWorkers; i++ {
		go wfs.asyncFlushWorker()
	}
}

func (wfs *WFS) asyncFlushWorker() {
	for item := range wfs.asyncFlushCh {
		wfs.processAsyncFlushItem(item)
	}
}

func (wfs *WFS) processAsyncFlushItem(item *asyncFlushItem) {
	defer wfs.asyncFlushWg.Done()
	defer func() {
		// Remove from fhMap first (so AcquireFileHandle creates a fresh handle).
		wfs.fhMap.RemoveFileHandle(item.fh.fh, item.fh.inode)
		// Then signal completion (unblocks waitForPendingAsyncFlush).
		close(item.done)
		wfs.pendingAsyncFlushMu.Lock()
		delete(wfs.pendingAsyncFlush, item.fh.inode)
		wfs.pendingAsyncFlushMu.Unlock()
	}()
	wfs.completeAsyncFlush(item.fh)
}

// completeAsyncFlush performs the deferred data upload and metadata flush
// that was skipped in doFlush() for writebackCache mode.
//
// This enables close() to return immediately for small file workloads (e.g., rsync),
// while the actual I/O happens concurrently in the background.
func (wfs *WFS) completeAsyncFlush(fh *FileHandle) {
	// Phase 1: Flush dirty pages — seals writable chunks, uploads to volume servers, and waits.
	// The underlying UploadWithRetry already retries transient HTTP/gRPC errors internally,
	// so a failure here indicates a persistent issue; the chunk data has been freed.
	if err := fh.dirtyPages.FlushData(); err != nil {
		glog.Errorf("completeAsyncFlush inode %d: data flush failed: %v", fh.inode, err)
		// Data is lost at this point (chunks freed after internal retry exhaustion).
		// Proceed to cleanup to avoid resource leaks and unmount hangs.
	} else if fh.dirtyMetadata {
		// Phase 2: Flush metadata unless the file was explicitly unlinked.
		//
		// isDeleted is set by the Unlink handler when it finds a draining
		// handle.  In that case the filer entry is already gone and
		// flushing would recreate it.  The uploaded chunks become orphans
		// and are cleaned up by volume.fsck.
		if fh.isDeleted {
			glog.V(3).Infof("completeAsyncFlush inode %d: file was unlinked, skipping metadata flush", fh.inode)
		} else {
			// Resolve the current path for metadata flush.
			//
			// Try GetPath first — it reflects any rename that happened
			// after close().  If the inode mapping is gone (Forget
			// dropped it after the kernel's lookup count hit zero), fall
			// back to the last path saved on the handle. Rename keeps
			// that fallback current, so it is always the newest known path.
			//
			// Forget does NOT mean the file was deleted — it only means
			// the kernel evicted its cache entry.
			dir, name := fh.savedDir, fh.savedName
			fileFullPath := util.FullPath(dir).Child(name)

			if resolvedPath, status := wfs.inodeToPath.GetPath(fh.inode); status == fuse.OK {
				dir, name = resolvedPath.DirAndName()
				fileFullPath = resolvedPath
			}

			wfs.flushMetadataWithRetry(fh, dir, name, fileFullPath)
		}
	}

	glog.V(3).Infof("completeAsyncFlush done inode %d fh %d", fh.inode, fh.fh)

	// Phase 3: Destroy the upload pipeline and free resources.
	fh.ReleaseHandle()
}

// flushMetadataWithRetry attempts to flush file metadata to the filer, retrying
// with exponential backoff on transient errors. The chunk data is already on the
// volume servers at this point; only the filer metadata reference needs persisting.
func (wfs *WFS) flushMetadataWithRetry(fh *FileHandle, dir, name string, fileFullPath util.FullPath) {
	err := retryMetadataFlush(func() error {
		return wfs.flushMetadataToFiler(fh, dir, name, fh.asyncFlushUid, fh.asyncFlushGid)
	}, func(nextAttempt, totalAttempts int, backoff time.Duration, err error) {
		glog.Warningf("completeAsyncFlush %s: retrying metadata flush (attempt %d/%d) after %v: %v",
			fileFullPath, nextAttempt, totalAttempts, backoff, err)
	})
	if err != nil {
		glog.Errorf("completeAsyncFlush %s: metadata flush failed after %d attempts: %v - "+
			"chunks are uploaded but NOT referenced in filer metadata; "+
			"they will appear as orphans in volume.fsck",
			fileFullPath, metadataFlushRetries+1, err)
	}
}

// WaitForAsyncFlush waits for all pending background flush work items to complete.
// Called before unmount cleanup to ensure no data is lost.
func (wfs *WFS) WaitForAsyncFlush() {
	wfs.asyncFlushWg.Wait()
	if wfs.asyncFlushCh != nil {
		close(wfs.asyncFlushCh)
	}
}
