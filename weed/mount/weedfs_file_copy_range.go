package mount

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

// performServerSideWholeFileCopy is a package-level seam so tests can override
// the filer call without standing up an HTTP endpoint.
var performServerSideWholeFileCopy = func(cancel <-chan struct{}, wfs *WFS, srcPath, dstPath util.FullPath) (*filer_pb.Entry, bool, error) {
	return wfs.copyEntryViaFiler(cancel, srcPath, dstPath)
}

// filerCopyRequestTimeout bounds the mount->filer POST so a stalled copy does
// not block copy_file_range workers indefinitely.
const filerCopyRequestTimeout = 60 * time.Second

// filerCopyReadbackTimeout gives the follow-up metadata reload a fresh deadline
// after the filer already accepted the copy request.
const filerCopyReadbackTimeout = 15 * time.Second

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

	if written, handled, status := wfs.tryServerSideWholeFileCopy(cancel, in, fhIn, fhOut); handled {
		return written, status
	}

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

func (wfs *WFS) tryServerSideWholeFileCopy(cancel <-chan struct{}, in *fuse.CopyFileRangeIn, fhIn, fhOut *FileHandle) (written uint32, handled bool, code fuse.Status) {
	srcPath, dstPath, sourceSize, ok := wholeFileServerCopyCandidate(fhIn, fhOut, in)
	if !ok {
		return 0, false, fuse.OK
	}

	glog.V(1).Infof("CopyFileRange server-side copy %s => %s (%d bytes)", srcPath, dstPath, sourceSize)

	entry, committed, err := performServerSideWholeFileCopy(cancel, wfs, srcPath, dstPath)
	if err != nil {
		if committed {
			glog.Warningf("CopyFileRange server-side copy %s => %s committed but local refresh failed: %v", srcPath, dstPath, err)
			wfs.applyServerSideWholeFileCopyResult(fhIn, fhOut, dstPath, entry, sourceSize)
			return uint32(sourceSize), true, fuse.OK
		}
		glog.V(0).Infof("CopyFileRange server-side copy %s => %s fallback to chunk copy: %v", srcPath, dstPath, err)
		return 0, false, fuse.OK
	}

	glog.V(1).Infof("CopyFileRange server-side copy %s => %s completed (%d bytes)", srcPath, dstPath, sourceSize)

	wfs.applyServerSideWholeFileCopyResult(fhIn, fhOut, dstPath, entry, sourceSize)

	return uint32(sourceSize), true, fuse.OK
}

func (wfs *WFS) applyServerSideWholeFileCopyResult(fhIn, fhOut *FileHandle, dstPath util.FullPath, entry *filer_pb.Entry, sourceSize int64) {
	if entry == nil {
		entry = synthesizeLocalEntryForServerSideWholeFileCopy(fhIn, fhOut, sourceSize)
	}
	if entry == nil {
		glog.Warningf("CopyFileRange server-side copy %s left no local entry to apply", dstPath)
		return
	}

	fhOut.SetEntry(entry)
	fhOut.RememberPath(dstPath)
	if entry.Attributes != nil {
		fhOut.contentType = entry.Attributes.Mime
	}
	fhOut.dirtyMetadata = false
	wfs.invalidateCopyDestinationCache(fhOut.inode, dstPath)
}

func synthesizeLocalEntryForServerSideWholeFileCopy(fhIn, fhOut *FileHandle, sourceSize int64) *filer_pb.Entry {
	dstEntry := fhOut.GetEntry().GetEntry()
	if dstEntry == nil {
		return nil
	}

	localEntry := proto.Clone(dstEntry).(*filer_pb.Entry)
	if localEntry.Attributes == nil {
		localEntry.Attributes = &filer_pb.FuseAttributes{}
	}

	if srcEntry := fhIn.GetEntry().GetEntry(); srcEntry != nil {
		srcEntryCopy := proto.Clone(srcEntry).(*filer_pb.Entry)
		localEntry.Content = srcEntryCopy.Content
		localEntry.Chunks = srcEntryCopy.Chunks
		if srcEntryCopy.Attributes != nil {
			localEntry.Attributes.Mime = srcEntryCopy.Attributes.Mime
			localEntry.Attributes.Md5 = srcEntryCopy.Attributes.Md5
		}
	}

	localEntry.Attributes.FileSize = uint64(sourceSize)
	localEntry.Attributes.Mtime = time.Now().Unix()
	return localEntry
}

func wholeFileServerCopyCandidate(fhIn, fhOut *FileHandle, in *fuse.CopyFileRangeIn) (srcPath, dstPath util.FullPath, sourceSize int64, ok bool) {
	if fhIn == nil || fhOut == nil || in == nil {
		glog.V(4).Infof("server-side copy: skipped (nil handle or input)")
		return "", "", 0, false
	}
	if fhIn.fh == fhOut.fh {
		glog.V(4).Infof("server-side copy: skipped (same file handle)")
		return "", "", 0, false
	}
	if fhIn.dirtyMetadata || fhOut.dirtyMetadata {
		glog.V(4).Infof("server-side copy: skipped (dirty metadata: in=%v out=%v)", fhIn.dirtyMetadata, fhOut.dirtyMetadata)
		return "", "", 0, false
	}
	if in.OffIn != 0 || in.OffOut != 0 {
		glog.V(4).Infof("server-side copy: skipped (non-zero offsets: in=%d out=%d)", in.OffIn, in.OffOut)
		return "", "", 0, false
	}

	srcEntry := fhIn.GetEntry()
	dstEntry := fhOut.GetEntry()
	if srcEntry == nil || dstEntry == nil {
		glog.V(4).Infof("server-side copy: skipped (nil entry: src=%v dst=%v)", srcEntry == nil, dstEntry == nil)
		return "", "", 0, false
	}
	if srcEntry.IsDirectory || dstEntry.IsDirectory {
		glog.V(4).Infof("server-side copy: skipped (directory)")
		return "", "", 0, false
	}

	sourceSize = int64(filer.FileSize(srcEntry.GetEntry()))
	// go-fuse exposes CopyFileRange's return value as uint32, so the fast path
	// should only claim copies that can be reported without truncation.
	if sourceSize <= 0 || sourceSize > math.MaxUint32 || int64(in.Len) < sourceSize {
		glog.V(4).Infof("server-side copy: skipped (size mismatch: sourceSize=%d len=%d)", sourceSize, in.Len)
		return "", "", 0, false
	}

	if filer.FileSize(dstEntry.GetEntry()) != 0 || len(dstEntry.GetChunks()) > 0 || len(dstEntry.Content) > 0 {
		glog.V(4).Infof("server-side copy: skipped (destination not empty)")
		return "", "", 0, false
	}

	srcPath = fhIn.FullPath()
	dstPath = fhOut.FullPath()
	if srcPath == "" || dstPath == "" || srcPath == dstPath {
		glog.V(4).Infof("server-side copy: skipped (invalid paths: src=%q dst=%q)", srcPath, dstPath)
		return "", "", 0, false
	}

	return srcPath, dstPath, sourceSize, true
}

func (wfs *WFS) copyEntryViaFiler(cancel <-chan struct{}, srcPath, dstPath util.FullPath) (*filer_pb.Entry, bool, error) {
	baseCtx, baseCancel := context.WithCancel(context.Background())
	defer baseCancel()

	if cancel != nil {
		go func() {
			select {
			case <-cancel:
				baseCancel()
			case <-baseCtx.Done():
			}
		}()
	}

	postCtx, postCancel := context.WithTimeout(baseCtx, filerCopyRequestTimeout)
	defer postCancel()

	httpClient := util_http.GetGlobalHttpClient()
	if httpClient == nil {
		var err error
		httpClient, err = util_http.NewGlobalHttpClient()
		if err != nil {
			return nil, false, fmt.Errorf("create filer copy http client: %w", err)
		}
	}

	copyURL := &url.URL{
		Scheme: httpClient.GetHttpScheme(),
		Host:   wfs.getCurrentFiler().ToHttpAddress(),
		Path:   string(dstPath),
	}
	query := copyURL.Query()
	query.Set("cp.from", string(srcPath))
	query.Set("overwrite", "true")
	query.Set("dataOnly", "true")
	copyURL.RawQuery = query.Encode()

	req, err := http.NewRequestWithContext(postCtx, http.MethodPost, copyURL.String(), nil)
	if err != nil {
		return nil, false, fmt.Errorf("create filer copy request: %w", err)
	}
	if jwt := wfs.filerCopyJWT(); jwt != "" {
		req.Header.Set("Authorization", "Bearer "+string(jwt))
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, false, fmt.Errorf("execute filer copy request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, false, fmt.Errorf("filer copy %s => %s failed: status %d: %s", srcPath, dstPath, resp.StatusCode, string(body))
	}

	readbackCtx, readbackCancel := context.WithTimeout(baseCtx, filerCopyReadbackTimeout)
	defer readbackCancel()

	entry, err := filer_pb.GetEntry(readbackCtx, wfs, dstPath)
	if err != nil {
		return nil, true, fmt.Errorf("reload copied entry %s: %w", dstPath, err)
	}
	if entry == nil {
		return nil, true, fmt.Errorf("reload copied entry %s: not found", dstPath)
	}
	if entry.Attributes != nil && wfs.option != nil && wfs.option.UidGidMapper != nil {
		entry.Attributes.Uid, entry.Attributes.Gid = wfs.option.UidGidMapper.FilerToLocal(entry.Attributes.Uid, entry.Attributes.Gid)
	}

	if wfs.metaCache != nil {
		dir, _ := dstPath.DirAndName()
		event := metadataUpdateEvent(dir, entry)
		if applyErr := wfs.applyLocalMetadataEvent(context.Background(), event); applyErr != nil {
			glog.Warningf("copyEntryViaFiler %s: best-effort metadata apply failed: %v", dstPath, applyErr)
			wfs.inodeToPath.InvalidateChildrenCache(util.FullPath(dir))
		}
	}

	return entry, true, nil
}

func (wfs *WFS) filerCopyJWT() security.EncodedJwt {
	if wfs.option == nil || len(wfs.option.FilerSigningKey) == 0 {
		return ""
	}
	return security.GenJwtForFilerServer(wfs.option.FilerSigningKey, wfs.option.FilerSigningExpiresAfterSec)
}

func (wfs *WFS) invalidateCopyDestinationCache(inode uint64, fullPath util.FullPath) {
	if wfs.fuseServer != nil {
		if status := wfs.fuseServer.InodeNotify(inode, 0, -1); status != fuse.OK {
			glog.V(4).Infof("CopyFileRange invalidate inode %d: %v", inode, status)
		}
		dir, name := fullPath.DirAndName()
		if parentInode, found := wfs.inodeToPath.GetInode(util.FullPath(dir)); found {
			if status := wfs.fuseServer.EntryNotify(parentInode, name); status != fuse.OK {
				glog.V(4).Infof("CopyFileRange invalidate entry %s: %v", fullPath, status)
			}
		}
	}
}
