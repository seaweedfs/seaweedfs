//go:build linux

package mount

import (
	"fmt"
	"os"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// tryEnablePassthrough materializes a read-only file into a local backing file
// and registers it with the kernel via FUSE_PASSTHROUGH, so reads and mmap on
// this open are served by the kernel directly from the backing file without
// round-tripping through this daemon.
//
// It is auto-enabled — there is no opt-in flag. Kernel support (Linux >= 6.9 and
// a privileged mount) is probed by the first RegisterBackingFd call and latched
// off on failure, so unsupported mounts transparently use the normal read path.
// It is gated to read-only opens of files no larger than FusePassthroughMaxMB
// (set that to 0 to disable entirely).
//
// Tradeoff: the backing file is a point-in-time snapshot taken at open. While an
// inode stays open in passthrough, concurrent writes (this mount or another) are
// not reflected — best for immutable / read-mostly data.
func (wfs *WFS) tryEnablePassthrough(fh *FileHandle, out *fuse.OpenOut) bool {
	capBytes := wfs.option.FusePassthroughMaxMB * 1024 * 1024
	if capBytes <= 0 || wfs.fuseServer == nil {
		return false
	}
	if wfs.passthroughDisabled.Load() {
		return false
	}

	entry := fh.GetEntry()
	if entry == nil || entry.Attributes == nil {
		return false
	}
	fileSize := int64(entry.Attributes.FileSize)
	if fileSize <= 0 || fileSize > capBytes {
		return false
	}

	fh.passthroughMu.Lock()
	defer fh.passthroughMu.Unlock()

	// Establish the backing file once per handle (one handle per inode), then
	// reuse it for every concurrent open of the same inode.
	if !fh.passthroughTried {
		fh.passthroughTried = true
		backingID, file, err := wfs.setupPassthroughBacking(fh, fileSize)
		if err != nil {
			glog.V(1).Infof("passthrough setup %s: %v", fh.FullPath(), err)
			return false
		}
		fh.passthroughBackingID = backingID
		fh.passthroughFile = file
		glog.V(2).Infof("passthrough enabled for %s (%d bytes, backingID %d)", fh.FullPath(), fileSize, backingID)
	}

	if fh.passthroughBackingID == 0 {
		return false
	}

	out.BackingID = fh.passthroughBackingID
	out.OpenFlags |= fuse.FOPEN_PASSTHROUGH
	// Passthrough and the page-cache keep-cache hint are mutually exclusive;
	// the backing file owns caching once passthrough is on.
	out.OpenFlags &^= fuse.FOPEN_KEEP_CACHE
	return true
}

// setupPassthroughBacking downloads the whole file into a temp file and
// registers its fd with the kernel, returning the backing ID.
func (wfs *WFS) setupPassthroughBacking(fh *FileHandle, fileSize int64) (int32, *os.File, error) {
	dir := wfs.option.CacheDirForRead
	if dir == "" {
		dir = os.TempDir()
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return 0, nil, err
	}
	file, err := os.CreateTemp(dir, "swpassthrough-")
	if err != nil {
		return 0, nil, err
	}
	// Unlink immediately: the kernel keeps the inode alive via the registered
	// fd, so no temp file is left behind on close or crash.
	os.Remove(file.Name())

	if err := materializeFile(fh, file, fileSize); err != nil {
		file.Close()
		return 0, nil, err
	}

	backingID, errno := wfs.fuseServer.RegisterBackingFd(&fuse.BackingMap{
		Fd: int32(file.Fd()),
	})
	if errno != 0 {
		// Typically EPERM (unprivileged mount) or ENOTTY (kernel <6.9 or
		// MaxStackDepth unset). Latch off so we stop attempting per open.
		wfs.passthroughDisabled.Store(true)
		file.Close()
		return 0, nil, fmt.Errorf("RegisterBackingFd: %v", errno)
	}
	return backingID, file, nil
}

// materializeFile copies the file's full contents into dst using the same read
// path the kernel Read handler uses, so the backing bytes are identical to what
// a normal read would return.
func materializeFile(fh *FileHandle, dst *os.File, fileSize int64) error {
	// Reuse the shared copy buffer pool (ChunkSizeLimit-sized) rather than
	// allocating a fresh buffer per materialization.
	buf := fh.wfs.copyBufferPool.Get().([]byte)
	defer fh.wfs.copyBufferPool.Put(buf)
	for offset := int64(0); offset < fileSize; {
		// readDataByFileHandle reads chunks + dirty pages and maps EOF to nil.
		n, err := readDataByFileHandle(buf, fh, offset)
		if n > 0 {
			if _, werr := dst.WriteAt(buf[:int(n)], offset); werr != nil {
				return werr
			}
			offset += n
		}
		if err != nil {
			return err
		}
		if n == 0 {
			break
		}
	}
	return dst.Sync()
}

// teardownPassthrough unregisters and closes the backing file. Called from
// ReleaseHandle when the last open of the inode is closed.
func (fh *FileHandle) teardownPassthrough() {
	fh.passthroughMu.Lock()
	defer fh.passthroughMu.Unlock()
	if fh.passthroughBackingID != 0 {
		if errno := fh.wfs.fuseServer.UnregisterBackingFd(fh.passthroughBackingID); errno != 0 {
			glog.Warningf("UnregisterBackingFd inode %d: %v", fh.inode, errno)
		}
		fh.passthroughBackingID = 0
	}
	if fh.passthroughFile != nil {
		fh.passthroughFile.Close()
		fh.passthroughFile = nil
	}
}
