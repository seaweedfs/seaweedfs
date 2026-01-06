package mount

import (
	"net/http"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/util"
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

	// Concurrent copy operations could allocate too much memory, so we want to
	// throttle our concurrency, scaling with the number of writers the mount
	// was configured with.
	if wfs.concurrentCopiersSem != nil {
		wfs.concurrentCopiersSem <- struct{}{}
		defer func() { <-wfs.concurrentCopiersSem }()
	}

	// We want to stream the copy operation to avoid allocating massive buffers.
	nowUnixNano := time.Now().UnixNano()
	totalCopied := int64(0)
	buff := wfs.copyBufferPool.Get().([]byte)
	defer wfs.copyBufferPool.Put(buff)
	for {
		// Comply with cancellation as best as we can, given that the underlying
		// IO functions aren't cancellation-aware.
		select {
		case <-cancel:
			glog.Warningf("canceled CopyFileRange for %s (copied %d)",
				fhIn.FullPath(), totalCopied)
			return uint32(totalCopied), fuse.EINTR
		default: // keep going
		}

		// We can save one IO by breaking early if we already know the next read
		// will result in zero bytes.
		remaining := int64(in.Len) - totalCopied
		readLen := min(remaining, int64(len(buff)))
		if readLen == 0 {
			break
		}

		// Perform the read
		offsetIn := totalCopied + int64(in.OffIn)
		numBytesRead, err := readDataByFileHandle(
			buff[:readLen], fhIn, offsetIn)
		if err != nil {
			glog.Warningf("file handle read %s %d (total %d): %v",
				fhIn.FullPath(), numBytesRead, totalCopied, err)
			return 0, fuse.EIO
		}

		// Break if we're done copying (no more bytes to read)
		if numBytesRead == 0 {
			break
		}

		offsetOut := int64(in.OffOut) + totalCopied

		// Detect mime type only during the beginning of our stream, since
		// DetectContentType is expecting some of the first 512 bytes of the
		// file. See [http.DetectContentType] for details.
		if offsetOut <= 512 {
			fhOut.contentType = http.DetectContentType(buff[:numBytesRead])
		}

		// Perform the write
		fhOut.dirtyPages.writerPattern.MonitorWriteAt(offsetOut, int(numBytesRead))
		if err := fhOut.dirtyPages.AddPage(
			offsetOut,
			buff[:numBytesRead],
			fhOut.dirtyPages.writerPattern.IsSequentialMode(),
			nowUnixNano); err != nil {
			glog.Errorf("AddPage error: %v", err)
			return 0, fuse.EIO
		}

		// Accumulate for the next loop iteration
		totalCopied += numBytesRead
	}

	if totalCopied == 0 {
		return 0, fuse.OK
	}

	fhOut.entry.Attributes.FileSize = uint64(max(
		totalCopied+int64(in.OffOut),
		int64(fhOut.entry.Attributes.FileSize),
	))
	fhOut.entry.Content = nil
	fhOut.dirtyMetadata = true

	written = uint32(totalCopied)
	return written, fuse.OK
}
