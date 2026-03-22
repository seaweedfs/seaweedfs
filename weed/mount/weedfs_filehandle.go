package mount

import (
	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func (wfs *WFS) AcquireHandle(inode uint64, flags, uid, gid uint32) (fileHandle *FileHandle, status fuse.Status) {
	var entry *filer_pb.Entry
	var path util.FullPath
	path, _, entry, status = wfs.maybeReadEntry(inode)
	if status == fuse.OK {
		if wormEnforced, _ := wfs.wormEnforcedForEntry(path, entry); wormEnforced && flags&fuse.O_ANYWRITE != 0 {
			return nil, fuse.EPERM
		}
		// need to AcquireFileHandle again to ensure correct handle counter
		fileHandle = wfs.fhMap.AcquireFileHandle(wfs, inode, entry)
	}
	return
}

func (wfs *WFS) ReleaseHandle(handleId FileHandleId) {
	fhToRelease := wfs.fhMap.ReleaseByHandle(handleId)
	wfs.finalizeFileHandle(fhToRelease)
}

// finalizeFileHandle either destroys the FileHandle immediately or, if an async
// flush is pending (writebackCache mode), submits a background goroutine to
// complete the data upload + metadata flush first.
func (wfs *WFS) finalizeFileHandle(fh *FileHandle) {
	if fh == nil {
		return
	}
	if fh.asyncFlushPending {
		wfs.asyncFlushWg.Add(1)
		go wfs.completeAsyncFlush(fh)
	} else {
		fh.ReleaseHandle()
	}
}

func (wfs *WFS) GetHandle(handleId FileHandleId) *FileHandle {
	return wfs.fhMap.GetFileHandle(handleId)
}
