package mount

import (
	"github.com/seaweedfs/seaweedfs/weed/util"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// These are non-POSIX extensions
const (
	SEEK_DATA uint32 = 3 // seek to next data after the offset
	SEEK_HOLE uint32 = 4 // seek to next hole after the offset
	ENXIO            = fuse.Status(syscall.ENXIO)
)

// Lseek finds next data or hole segments after the specified offset
// See https://man7.org/linux/man-pages/man2/lseek.2.html
func (wfs *WFS) Lseek(cancel <-chan struct{}, in *fuse.LseekIn, out *fuse.LseekOut) fuse.Status {
	// not a documented feature
	if in.Padding != 0 {
		return fuse.EINVAL
	}

	if in.Whence != SEEK_DATA && in.Whence != SEEK_HOLE {
		return fuse.EINVAL
	}

	// file must exist
	fh := wfs.GetHandle(FileHandleId(in.Fh))
	if fh == nil {
		return fuse.EBADF
	}

	// lock the file until the proper offset was calculated
	fhActiveLock := fh.wfs.fhLockTable.AcquireLock("Lseek", fh.fh, util.SharedLock)
	defer fh.wfs.fhLockTable.ReleaseLock(fh.fh, fhActiveLock)

	fileSize := int64(filer.FileSize(fh.GetEntry().GetEntry()))
	offset := max(int64(in.Offset), 0)

	glog.V(4).Infof(
		"Lseek %s fh %d in [%d,%d], whence %d",
		fh.FullPath(), fh.fh, offset, fileSize, in.Whence,
	)

	// can neither seek beyond file nor seek to a hole at the end of the file with SEEK_DATA
	if offset > fileSize {
		return ENXIO
	} else if in.Whence == SEEK_DATA && offset == fileSize {
		return ENXIO
	}

	// search chunks for the offset
	found, offset := fh.entryChunkGroup.SearchChunks(offset, fileSize, in.Whence)
	if found {
		out.Offset = uint64(offset)
		return fuse.OK
	}

	// in case we found no exact matches, we return the recommended fallbacks, that is:
	// original offset for SEEK_DATA or end of file for an implicit hole
	if in.Whence == SEEK_DATA {
		out.Offset = in.Offset
	} else {
		out.Offset = uint64(fileSize)
	}

	return fuse.OK
}
