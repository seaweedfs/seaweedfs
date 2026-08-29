package mount

import (
	"syscall"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// See https://man7.org/linux/man-pages/man2/fallocate.2.html
const FALLOC_FL_KEEP_SIZE uint32 = 0x01

// Fallocate allocates space for an open file. Volume space is assigned when a
// write is flushed, so nothing can be reserved up front; only a range past the
// end of the file has an effect, growing it the way a truncate would.
func (wfs *WFS) Fallocate(cancel <-chan struct{}, in *fuse.FallocateIn) (code fuse.Status) {

	// ENOSYS makes the kernel stop sending FUSE_FALLOCATE for every mode, so a
	// mode we cannot honor has to be refused on its own.
	if in.Mode&^FALLOC_FL_KEEP_SIZE != 0 {
		return fuse.ENOTSUP
	}

	fh := wfs.GetHandle(FileHandleId(in.Fh))
	if fh == nil {
		return fuse.EBADF
	}

	fhActiveLock := fh.wfs.fhLockTable.AcquireLock("Fallocate", fh.fh, util.ExclusiveLock)
	defer fh.wfs.fhLockTable.ReleaseLock(fh.fh, fhActiveLock)

	entry := fh.GetEntry().GetEntry()
	if entry == nil {
		return fuse.ENOENT
	}

	oldFileSize := filer.FileSize(entry)
	newFileSize := in.Offset + in.Length
	if in.Mode&FALLOC_FL_KEEP_SIZE != 0 || newFileSize <= oldFileSize {
		return fuse.OK
	}

	if wfs.IsOverQuotaWithUncommitted() {
		return fuse.Status(syscall.ENOSPC)
	}

	if wormEnforced, _ := wfs.wormEnforcedForEntry(fh.FullPath(), entry); wormEnforced {
		return fuse.EPERM
	}

	glog.V(4).Infof("Fallocate %s fh %d grow to %d", fh.FullPath(), fh.fh, newFileSize)

	entry.Attributes.FileSize = newFileSize
	// The writes that fill the range will not grow the file, so they charge
	// nothing; the growth is counted here or the quota never sees it.
	wfs.AddUncommittedBytes(int64(newFileSize - oldFileSize))

	now := time.Now()
	entry.Attributes.Mtime = now.Unix()
	entry.Attributes.MtimeNs = int32(now.Nanosecond())
	entry.Attributes.Ctime = now.Unix()
	entry.Attributes.CtimeNs = int32(now.Nanosecond())

	fh.dirtyMetadata = true
	wfs.invalidateOpenMtimeCache(in.NodeId)

	return fuse.OK
}
