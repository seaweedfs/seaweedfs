package mount

import (
	"github.com/seaweedfs/seaweedfs/weed/glog"
)

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
	if err := fh.dirtyPages.FlushData(); err != nil {
		glog.Errorf("completeAsyncFlush %s: data flush failed: %v", fileFullPath, err)
		// Continue to cleanup even on error
	} else if fh.dirtyMetadata {
		// Phase 2: Flush metadata to filer with the uploaded chunk references.
		if err := wfs.flushMetadataToFiler(fh, dir, name, fh.asyncFlushUid, fh.asyncFlushGid); err != nil {
			glog.Errorf("completeAsyncFlush %s: metadata flush failed: %v", fileFullPath, err)
		}
	}

	glog.V(3).Infof("completeAsyncFlush done %s fh %d", fileFullPath, fh.fh)

	// Phase 3: Destroy the upload pipeline and free resources.
	fh.ReleaseHandle()
}

// WaitForAsyncFlush waits for all pending background flush goroutines to complete.
// Called before unmount cleanup to ensure no data is lost.
func (wfs *WFS) WaitForAsyncFlush() {
	wfs.asyncFlushWg.Wait()
}
