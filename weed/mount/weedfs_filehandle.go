package mount

import (
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"time"
)

func (wfs *WFS) AcquireHandle(inode uint64, flags, uid, gid uint32) (fileHandle *FileHandle, status fuse.Status) {
	var entry *filer_pb.Entry
	_, _, entry, status = wfs.maybeReadEntry(inode)
	if status == fuse.OK {
		if entry != nil && wfs.option.WriteOnceReadMany {
			if entry.Attributes.Mtime+10 < time.Now().Unix() {
				if flags&fuse.O_ANYWRITE != 0 {
					return nil, fuse.EPERM
				}
			}
		}
		// need to AcquireFileHandle again to ensure correct handle counter
		fileHandle = wfs.fhmap.AcquireFileHandle(wfs, inode, entry)
	}
	return
}

func (wfs *WFS) ReleaseHandle(handleId FileHandleId) {
	wfs.fhmap.ReleaseByHandle(handleId)
}

func (wfs *WFS) GetHandle(handleId FileHandleId) *FileHandle {
	return wfs.fhmap.GetFileHandle(handleId)
}
