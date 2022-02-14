package mount

import "github.com/hanwen/go-fuse/v2/fuse"

func (wfs *WFS) AcquireHandle(inode uint64, uid, gid uint32) (fileHandle *FileHandle, code fuse.Status) {
	_, entry, status := wfs.maybeReadEntry(inode)
	if status == fuse.OK {
		fileHandle = wfs.fhmap.GetFileHandle(inode)
		fileHandle.entry = entry
	}
	return
}

func (wfs *WFS) ReleaseHandle(handleId FileHandleId) {
	wfs.fhmap.ReleaseByHandle(handleId)
}
