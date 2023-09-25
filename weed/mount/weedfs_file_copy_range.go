package mount

import (
	"github.com/seaweedfs/seaweedfs/weed/util"
	"net/http"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// CopyFileRange copies data from one file to another from and to specified offsets.
//
// See https://man7.org/linux/man-pages/man2/copy_file_range.2.html
// See https://github.com/libfuse/libfuse/commit/fe4f9428fc403fa8b99051f52d84ea5bd13f3855
/**
 * Copy a range of data from one file to another
 *
 * Niels de Vos: • libfuse: add copy_file_range() support
 *
 * Performs an optimized copy between two file descriptors without the
 * additional cost of transferring data through the FUSE kernel module
 * to user space (glibc) and then back into the FUSE filesystem again.
 *
 * In case this method is not implemented, applications are expected to
 * fall back to a regular file copy.   (Some glibc versions did this
 * emulation automatically, but the emulation has been removed from all
 * glibc release branches.)
 */
func (wfs *WFS) CopyFileRange(cancel <-chan struct{}, in *fuse.CopyFileRangeIn) (written uint32, code fuse.Status) {
	// flags must equal 0 for this syscall as of now
	if in.Flags != 0 {
		return 0, fuse.EINVAL
	}

	// files must exist
	fhOut := wfs.GetHandle(FileHandleId(in.FhOut))
	if fhOut == nil {
		return 0, fuse.EBADF
	}
	fhIn := wfs.GetHandle(FileHandleId(in.FhIn))
	if fhIn == nil {
		return 0, fuse.EBADF
	}

	// lock source and target file handles
	fhOutActiveLock := fhOut.wfs.fhLockTable.AcquireLock("CopyFileRange", fhOut.fh, util.ExclusiveLock)
	defer fhOut.wfs.fhLockTable.ReleaseLock(fhOut.fh, fhOutActiveLock)

	if fhOut.entry == nil {
		return 0, fuse.ENOENT
	}

	if fhIn.fh != fhOut.fh {
		fhInActiveLock := fhIn.wfs.fhLockTable.AcquireLock("CopyFileRange", fhIn.fh, util.SharedLock)
		defer fhIn.wfs.fhLockTable.ReleaseLock(fhIn.fh, fhInActiveLock)
	}

	// directories are not supported
	if fhIn.entry.IsDirectory || fhOut.entry.IsDirectory {
		return 0, fuse.EISDIR
	}

	glog.V(4).Infof(
		"CopyFileRange %s fhIn %d -> %s fhOut %d, [%d,%d) -> [%d,%d)",
		fhIn.FullPath(), fhIn.fh,
		fhOut.FullPath(), fhOut.fh,
		in.OffIn, in.OffIn+in.Len,
		in.OffOut, in.OffOut+in.Len,
	)

	data := make([]byte, in.Len)
	totalRead, err := readDataByFileHandle(data, fhIn, int64(in.OffIn))
	if err != nil {
		glog.Warningf("file handle read %s %d: %v", fhIn.FullPath(), totalRead, err)
		return 0, fuse.EIO
	}
	data = data[:totalRead]

	if totalRead == 0 {
		return 0, fuse.OK
	}

	// put data at the specified offset in target file
	fhOut.dirtyPages.writerPattern.MonitorWriteAt(int64(in.OffOut), int(in.Len))
	fhOut.entry.Content = nil
	fhOut.dirtyPages.AddPage(int64(in.OffOut), data, fhOut.dirtyPages.writerPattern.IsSequentialMode(), time.Now().UnixNano())
	fhOut.entry.Attributes.FileSize = uint64(max(int64(in.OffOut)+totalRead, int64(fhOut.entry.Attributes.FileSize)))
	fhOut.dirtyMetadata = true
	written = uint32(totalRead)

	// detect mime type
	if written > 0 && in.OffOut <= 512 {
		fhOut.contentType = http.DetectContentType(data)
	}

	return written, fuse.OK
}
