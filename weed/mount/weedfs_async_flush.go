package mount

import (
	"context"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc/codes"
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
	glog.V(4).Infof("completeAsyncFlush inode %d fh %d saved=%s/%s dirtyMetadata=%v isDeleted=%v isRenamed=%v",
		fh.inode, fh.fh, fh.savedDir, fh.savedName, fh.dirtyMetadata, fh.isDeleted, fh.isRenamed)

	// Phase 1: Flush dirty pages — seals writable chunks, uploads to volume servers, and waits.
	// Writeback uploads retry transient errors until they succeed, so a failure here
	// is a permanent one and the chunk data has been freed.
	if err := fh.dirtyPages.FlushData(); err != nil {
		glog.Errorf("completeAsyncFlush inode %d: data flush failed: %v", fh.inode, err)
		(&contentPublication{failure: fh.writebackContentError}).abandon()
		// Data is lost at this point. Proceed to cleanup to avoid resource leaks and unmount hangs.
	} else if fh.dirtyMetadata {
		// Phase 2: Flush metadata unless the file was explicitly unlinked.
		//
		// isDeleted is set by the Unlink handler when it finds a draining
		// handle.  In that case the filer entry is already gone and
		// flushing would recreate it.  The uploaded chunks become orphans
		// and are cleaned up by volume.fsck.
		if fh.isDeleted || fh.isRenamed {
			if fh.isDeleted {
				glog.V(3).Infof("completeAsyncFlush inode %d: file was unlinked, skipping metadata flush", fh.inode)
			} else {
				glog.V(3).Infof("completeAsyncFlush inode %d: file was renamed, skipping old-path metadata flush (Rename handles it)", fh.inode)
			}
		} else if savedInode, found := wfs.inodeToPath.GetInode(util.FullPath(fh.savedDir).Child(fh.savedName)); !found || savedInode != fh.inode {
			// The saved path no longer maps to this inode — the file was
			// renamed (or deleted and recreated). Flushing metadata under
			// the old path would re-insert a stale entry into the meta
			// cache, breaking git's lock file protocol.
			glog.V(3).Infof("completeAsyncFlush inode %d: saved path %s/%s no longer maps to this inode, skipping metadata flush",
				fh.inode, fh.savedDir, fh.savedName)
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

			if err := wfs.flushMetadataWithRetry(fh, dir, name, fileFullPath); err != nil {
				// close already returned. Do not discard the only pending
				// references until a private recovery record is durable.
				// Disk failures keep this drain pending; retry retention only,
				// never replay the refused publication against a new base.
				for {
					path, saveErr := wfs.retainFailedContent(fh, dir, name, err)
					if saveErr == nil {
						glog.Errorf("completeAsyncFlush %s: publication failed: %v; recovery request retained at %s (no automatic replay)", fileFullPath, err, path)
						break
					}
					glog.Errorf("completeAsyncFlush %s: retaining pending references in memory: %v; publication failure: %v", fileFullPath, saveErr, err)
					metadataFlushSleep(context.Background(), util.RetryWaitTime)
				}
			}
		}
	}

	glog.V(3).Infof("completeAsyncFlush done inode %d fh %d", fh.inode, fh.fh)

	// Phase 3: Destroy the upload pipeline and free resources.
	fh.ReleaseHandle()
}

// isFilerUnreachable reports a filer call that failed without the filer
// answering it: the dial, the connection or the call deadline gave out. It
// reads only the gRPC status, because a refusal the filer sends in its
// response has none, and its message carries the file path.
func isFilerUnreachable(err error) bool {
	st, ok := util.ServerStatus(err)
	return ok && st.Code() == codes.Unavailable
}

// flushMetadataWithRetry persists the chunk references of a closed file. The
// chunk data is already on the volume servers and close() has returned, so
// nothing else holds these references: like the writeback chunk uploads, it
// keeps retrying for as long as the filer is unreachable.
func (wfs *WFS) flushMetadataWithRetry(fh *FileHandle, dir, name string, fileFullPath util.FullPath) error {
	publication := &contentPublication{failure: fh.writebackContentError}
	attempts := 0
	err := retryMetadataFlushUntil(context.Background(), func() error {
		attempts++
		return wfs.flushMetadataToFiler(context.Background(), fh, dir, name, fh.asyncFlushUid, fh.asyncFlushGid, nil, publication)
	}, isFilerUnreachable, func(nextAttempt, totalAttempts int, backoff time.Duration, err error) {
		if totalAttempts == 0 {
			glog.Warningf("completeAsyncFlush %s: filer unreachable, retrying metadata flush (attempt %d) after %v: %v",
				fileFullPath, nextAttempt, backoff, err)
			return
		}
		glog.Warningf("completeAsyncFlush %s: retrying metadata flush (attempt %d/%d) after %v: %v",
			fileFullPath, nextAttempt, totalAttempts, backoff, err)
	})
	if err != nil {
		if fh.dirtyContent {
			publication.abandon()
		}
		glog.Errorf("completeAsyncFlush %s: metadata flush failed after %d attempts: %v - "+
			"chunks are uploaded but NOT referenced in filer metadata; "+
			"they will appear as orphans in volume.fsck",
			fileFullPath, attempts, err)
	}
	return err
}

// WaitForAsyncFlush waits for all pending background flush work items to complete.
// Called before unmount cleanup to ensure no data is lost.
func (wfs *WFS) WaitForAsyncFlush() {
	wfs.asyncFlushWg.Wait()
	// Shutdown reaches here from both the interrupt hook and the path that
	// resumes once serving stops, so closing has to survive a second caller.
	wfs.asyncFlushClose.Do(func() {
		if wfs.asyncFlushCh != nil {
			close(wfs.asyncFlushCh)
		}
		if wfs.streamMutate != nil {
			wfs.streamMutate.Close()
		}
	})
}
