package mount

import (
	"net/http"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

/**
 * Write data
 *
 * Write should return exactly the number of bytes requested
 * except on error.  An exception to this is when the file has
 * been opened in 'direct_io' mode, in which case the return value
 * of the write system call will reflect the return value of this
 * operation.
 *
 * Unless FUSE_CAP_HANDLE_KILLPRIV is disabled, this method is
 * expected to reset the setuid and setgid bits.
 *
 * fi->fh will contain the value set by the open method, or will
 * be undefined if the open method didn't set any value.
 *
 * Valid replies:
 *   fuse_reply_write
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param buf data to write
 * @param size number of bytes to write
 * @param off offset to write to
 * @param fi file information
 */
func (wfs *WFS) Write(cancel <-chan struct{}, in *fuse.WriteIn, data []byte) (written uint32, code fuse.Status) {

	// Check quota including uncommitted writes for real-time enforcement
	if wfs.IsOverQuotaWithUncommitted() {
		return 0, fuse.Status(syscall.ENOSPC)
	}

	fh := wfs.GetHandle(FileHandleId(in.Fh))
	if fh == nil {
		return 0, fuse.ENOENT
	}

	fh.dirtyPages.writerPattern.MonitorWriteAt(int64(in.Offset), int(in.Size))

	tsNs := time.Now().UnixNano()

	fhActiveLock := fh.wfs.fhLockTable.AcquireLock("Write", fh.fh, util.ExclusiveLock)
	defer fh.wfs.fhLockTable.ReleaseLock(fh.fh, fhActiveLock)

	entry := fh.GetEntry()
	if entry == nil {
		return 0, fuse.OK
	}

	entry.Content = nil
	offset := int64(in.Offset)
	oldFileSize := int64(entry.Attributes.FileSize)
	newFileSize := max(offset+int64(len(data)), oldFileSize)
	entry.Attributes.FileSize = uint64(newFileSize)

	// Track uncommitted bytes for real-time quota enforcement.
	// Only count the new bytes being added beyond the current file size.
	if newFileSize > oldFileSize {
		wfs.AddUncommittedBytes(newFileSize - oldFileSize)
	}

	// glog.V(4).Infof("%v write [%d,%d) %d", fh.f.fullpath(), req.Offset, req.Offset+int64(len(req.Data)), len(req.Data))

	if err := fh.dirtyPages.AddPage(offset, data, fh.dirtyPages.writerPattern.IsSequentialMode(), tsNs); err != nil {
		glog.Errorf("AddPage error: %v", err)
		return 0, fuse.EIO
	}

	written = uint32(len(data))

	if offset == 0 {
		// detect mime type
		fh.contentType = http.DetectContentType(data)
	}

	fh.dirtyMetadata = true

	if IsDebugFileReadWrite {
		// print("+")
		fh.mirrorFile.WriteAt(data, offset)
	}

	return written, fuse.OK
}
