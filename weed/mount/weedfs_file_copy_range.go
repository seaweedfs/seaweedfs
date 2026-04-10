package mount

import (
	"bytes"
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
	request_id "github.com/seaweedfs/seaweedfs/weed/util/request_id"
)

type serverSideWholeFileCopyOutcome uint8

const (
	serverSideWholeFileCopyNotCommitted serverSideWholeFileCopyOutcome = iota
	serverSideWholeFileCopyCommitted
	serverSideWholeFileCopyAmbiguous
)

type wholeFileServerCopyRequest struct {
	srcPath       util.FullPath
	dstPath       util.FullPath
	sourceSize    int64
	srcInode      uint64
	srcMtime      int64
	dstInode      uint64
	dstMtime      int64
	dstSize       int64
	sourceMime    string
	sourceMd5     []byte
	copyRequestID string
}

// performServerSideWholeFileCopy is a package-level seam so tests can override
// the filer call without standing up an HTTP endpoint.
var performServerSideWholeFileCopy = func(cancel <-chan struct{}, wfs *WFS, copyRequest wholeFileServerCopyRequest) (*filer_pb.Entry, serverSideWholeFileCopyOutcome, error) {
	return wfs.copyEntryViaFiler(cancel, copyRequest)
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
	copyRequest, ok := wholeFileServerCopyCandidate(fhIn, fhOut, in)
	if !ok {
		return 0, false, fuse.OK
	}

	glog.V(1).Infof("CopyFileRange server-side copy %s => %s (%d bytes)", copyRequest.srcPath, copyRequest.dstPath, copyRequest.sourceSize)

	entry, outcome, err := performServerSideWholeFileCopy(cancel, wfs, copyRequest)
	switch outcome {
	case serverSideWholeFileCopyCommitted:
		if err != nil {
			glog.Warningf("CopyFileRange server-side copy %s => %s committed but local refresh failed: %v", copyRequest.srcPath, copyRequest.dstPath, err)
		} else {
			glog.V(1).Infof("CopyFileRange server-side copy %s => %s completed (%d bytes)", copyRequest.srcPath, copyRequest.dstPath, copyRequest.sourceSize)
		}
		wfs.applyServerSideWholeFileCopyResult(fhIn, fhOut, copyRequest.dstPath, entry, copyRequest.sourceSize)
		return uint32(copyRequest.sourceSize), true, fuse.OK
	case serverSideWholeFileCopyAmbiguous:
		glog.Warningf("CopyFileRange server-side copy %s => %s outcome ambiguous: %v", copyRequest.srcPath, copyRequest.dstPath, err)
		return 0, true, fuse.EIO
	default:
		glog.V(0).Infof("CopyFileRange server-side copy %s => %s fallback to chunk copy: %v", copyRequest.srcPath, copyRequest.dstPath, err)
		return 0, false, fuse.OK
	}
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
	wfs.updateServerSideWholeFileCopyMetaCache(dstPath, entry)
	wfs.invalidateCopyDestinationCache(fhOut.inode, dstPath)
}

func (wfs *WFS) updateServerSideWholeFileCopyMetaCache(dstPath util.FullPath, entry *filer_pb.Entry) {
	if wfs.metaCache == nil || entry == nil {
		return
	}

	dir, _ := dstPath.DirAndName()
	event := metadataUpdateEvent(dir, entry)
	if applyErr := wfs.applyLocalMetadataEvent(context.Background(), event); applyErr != nil {
		glog.Warningf("CopyFileRange metadata update %s: %v", dstPath, applyErr)
		wfs.markDirectoryReadThrough(util.FullPath(dir))
	}
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

	copyNow := time.Now()
	localEntry.Attributes.FileSize = uint64(sourceSize)
	localEntry.Attributes.Mtime = copyNow.Unix()
	localEntry.Attributes.MtimeNs = int32(copyNow.Nanosecond())
	localEntry.Attributes.Ctime = copyNow.Unix()
	localEntry.Attributes.CtimeNs = int32(copyNow.Nanosecond())
	return localEntry
}

func wholeFileServerCopyCandidate(fhIn, fhOut *FileHandle, in *fuse.CopyFileRangeIn) (copyRequest wholeFileServerCopyRequest, ok bool) {
	if fhIn == nil || fhOut == nil || in == nil {
		glog.V(4).Infof("server-side copy: skipped (nil handle or input)")
		return wholeFileServerCopyRequest{}, false
	}
	if fhIn.fh == fhOut.fh {
		glog.V(4).Infof("server-side copy: skipped (same file handle)")
		return wholeFileServerCopyRequest{}, false
	}
	if fhIn.dirtyMetadata || fhOut.dirtyMetadata {
		glog.V(4).Infof("server-side copy: skipped (dirty metadata: in=%v out=%v)", fhIn.dirtyMetadata, fhOut.dirtyMetadata)
		return wholeFileServerCopyRequest{}, false
	}
	if in.OffIn != 0 || in.OffOut != 0 {
		glog.V(4).Infof("server-side copy: skipped (non-zero offsets: in=%d out=%d)", in.OffIn, in.OffOut)
		return wholeFileServerCopyRequest{}, false
	}

	srcEntry := fhIn.GetEntry()
	dstEntry := fhOut.GetEntry()
	if srcEntry == nil || dstEntry == nil {
		glog.V(4).Infof("server-side copy: skipped (nil entry: src=%v dst=%v)", srcEntry == nil, dstEntry == nil)
		return wholeFileServerCopyRequest{}, false
	}
	if srcEntry.IsDirectory || dstEntry.IsDirectory {
		glog.V(4).Infof("server-side copy: skipped (directory)")
		return wholeFileServerCopyRequest{}, false
	}

	srcPbEntry := srcEntry.GetEntry()
	dstPbEntry := dstEntry.GetEntry()
	if srcPbEntry == nil || dstPbEntry == nil || srcPbEntry.Attributes == nil || dstPbEntry.Attributes == nil {
		glog.V(4).Infof("server-side copy: skipped (missing entry attributes)")
		return wholeFileServerCopyRequest{}, false
	}

	sourceSize := int64(filer.FileSize(srcPbEntry))
	// go-fuse exposes CopyFileRange's return value as uint32, so the fast path
	// should only claim copies that can be reported without truncation.
	if sourceSize <= 0 || sourceSize > math.MaxUint32 || int64(in.Len) < sourceSize {
		glog.V(4).Infof("server-side copy: skipped (size mismatch: sourceSize=%d len=%d)", sourceSize, in.Len)
		return wholeFileServerCopyRequest{}, false
	}

	dstSize := int64(filer.FileSize(dstPbEntry))
	if dstSize != 0 || len(dstPbEntry.GetChunks()) > 0 || len(dstPbEntry.Content) > 0 {
		glog.V(4).Infof("server-side copy: skipped (destination not empty)")
		return wholeFileServerCopyRequest{}, false
	}

	srcPath := fhIn.FullPath()
	dstPath := fhOut.FullPath()
	if srcPath == "" || dstPath == "" || srcPath == dstPath {
		glog.V(4).Infof("server-side copy: skipped (invalid paths: src=%q dst=%q)", srcPath, dstPath)
		return wholeFileServerCopyRequest{}, false
	}

	if srcPbEntry.Attributes.Inode == 0 || dstPbEntry.Attributes.Inode == 0 {
		glog.V(4).Infof("server-side copy: skipped (missing inode preconditions: src=%d dst=%d)", srcPbEntry.Attributes.Inode, dstPbEntry.Attributes.Inode)
		return wholeFileServerCopyRequest{}, false
	}

	return wholeFileServerCopyRequest{
		srcPath:       srcPath,
		dstPath:       dstPath,
		sourceSize:    sourceSize,
		srcInode:      srcPbEntry.Attributes.Inode,
		srcMtime:      srcPbEntry.Attributes.Mtime,
		dstInode:      dstPbEntry.Attributes.Inode,
		dstMtime:      dstPbEntry.Attributes.Mtime,
		dstSize:       dstSize,
		sourceMime:    srcPbEntry.Attributes.Mime,
		sourceMd5:     append([]byte(nil), srcPbEntry.Attributes.Md5...),
		copyRequestID: request_id.New(),
	}, true
}

func (wfs *WFS) copyEntryViaFiler(cancel <-chan struct{}, copyRequest wholeFileServerCopyRequest) (*filer_pb.Entry, serverSideWholeFileCopyOutcome, error) {
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
			return nil, serverSideWholeFileCopyNotCommitted, fmt.Errorf("create filer copy http client: %w", err)
		}
	}

	copyURL := &url.URL{
		Scheme: httpClient.GetHttpScheme(),
		Host:   wfs.getCurrentFiler().ToHttpAddress(),
		Path:   string(copyRequest.dstPath),
	}
	query := copyURL.Query()
	query.Set(filer.CopyQueryParamFrom, string(copyRequest.srcPath))
	query.Set(filer.CopyQueryParamOverwrite, "true")
	query.Set(filer.CopyQueryParamDataOnly, "true")
	query.Set(filer.CopyQueryParamRequestID, copyRequest.copyRequestID)
	query.Set(filer.CopyQueryParamSourceInode, fmt.Sprintf("%d", copyRequest.srcInode))
	query.Set(filer.CopyQueryParamSourceMtime, fmt.Sprintf("%d", copyRequest.srcMtime))
	query.Set(filer.CopyQueryParamSourceSize, fmt.Sprintf("%d", copyRequest.sourceSize))
	query.Set(filer.CopyQueryParamDestinationInode, fmt.Sprintf("%d", copyRequest.dstInode))
	query.Set(filer.CopyQueryParamDestinationMtime, fmt.Sprintf("%d", copyRequest.dstMtime))
	query.Set(filer.CopyQueryParamDestinationSize, fmt.Sprintf("%d", copyRequest.dstSize))
	copyURL.RawQuery = query.Encode()

	req, err := http.NewRequestWithContext(postCtx, http.MethodPost, copyURL.String(), nil)
	if err != nil {
		return nil, serverSideWholeFileCopyNotCommitted, fmt.Errorf("create filer copy request: %w", err)
	}
	if jwt := wfs.filerCopyJWT(); jwt != "" {
		req.Header.Set("Authorization", "Bearer "+string(jwt))
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return wfs.confirmServerSideWholeFileCopyAfterAmbiguousRequest(baseCtx, copyRequest, fmt.Errorf("execute filer copy request: %w", err))
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, serverSideWholeFileCopyNotCommitted, fmt.Errorf("filer copy %s => %s failed: status %d: %s", copyRequest.srcPath, copyRequest.dstPath, resp.StatusCode, string(body))
	}

	readbackCtx, readbackCancel := context.WithTimeout(baseCtx, filerCopyReadbackTimeout)
	defer readbackCancel()

	entry, err := filer_pb.GetEntry(readbackCtx, wfs, copyRequest.dstPath)
	if err != nil {
		return nil, serverSideWholeFileCopyCommitted, fmt.Errorf("reload copied entry %s: %w", copyRequest.dstPath, err)
	}
	if entry == nil {
		return nil, serverSideWholeFileCopyCommitted, fmt.Errorf("reload copied entry %s: not found", copyRequest.dstPath)
	}
	if entry.Attributes != nil && wfs.option != nil && wfs.option.UidGidMapper != nil {
		entry.Attributes.Uid, entry.Attributes.Gid = wfs.option.UidGidMapper.FilerToLocal(entry.Attributes.Uid, entry.Attributes.Gid)
	}

	return entry, serverSideWholeFileCopyCommitted, nil
}

func (wfs *WFS) confirmServerSideWholeFileCopyAfterAmbiguousRequest(baseCtx context.Context, copyRequest wholeFileServerCopyRequest, requestErr error) (*filer_pb.Entry, serverSideWholeFileCopyOutcome, error) {
	readbackCtx, readbackCancel := context.WithTimeout(baseCtx, filerCopyReadbackTimeout)
	defer readbackCancel()

	entry, err := filer_pb.GetEntry(readbackCtx, wfs, copyRequest.dstPath)
	if err == nil && entry != nil && entryMatchesServerSideWholeFileCopy(copyRequest, entry) {
		if entry.Attributes != nil && wfs.option != nil && wfs.option.UidGidMapper != nil {
			entry.Attributes.Uid, entry.Attributes.Gid = wfs.option.UidGidMapper.FilerToLocal(entry.Attributes.Uid, entry.Attributes.Gid)
		}
		return entry, serverSideWholeFileCopyCommitted, nil
	}

	if err != nil {
		return nil, serverSideWholeFileCopyAmbiguous, fmt.Errorf("%w; post-copy readback failed: %v", requestErr, err)
	}
	if entry == nil {
		return nil, serverSideWholeFileCopyAmbiguous, fmt.Errorf("%w; destination %s was not readable after the ambiguous request", requestErr, copyRequest.dstPath)
	}
	return nil, serverSideWholeFileCopyAmbiguous, fmt.Errorf("%w; destination %s did not match the requested copy after the ambiguous request", requestErr, copyRequest.dstPath)
}

func entryMatchesServerSideWholeFileCopy(copyRequest wholeFileServerCopyRequest, entry *filer_pb.Entry) bool {
	if entry == nil || entry.Attributes == nil {
		return false
	}
	if copyRequest.dstInode != 0 && entry.Attributes.Inode != copyRequest.dstInode {
		return false
	}
	if entry.Attributes.FileSize != uint64(copyRequest.sourceSize) {
		return false
	}
	if copyRequest.sourceMime != "" && entry.Attributes.Mime != copyRequest.sourceMime {
		return false
	}
	if len(copyRequest.sourceMd5) > 0 && !bytes.Equal(entry.Attributes.Md5, copyRequest.sourceMd5) {
		return false
	}
	return true
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
