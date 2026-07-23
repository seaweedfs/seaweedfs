package mount

import (
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/util"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

type FileHandleToInode struct {
	sync.RWMutex
	inode2fh map[uint64]*FileHandle
	fh2inode map[FileHandleId]uint64
}

func NewFileHandleToInode() *FileHandleToInode {
	return &FileHandleToInode{
		inode2fh: make(map[uint64]*FileHandle),
		fh2inode: make(map[FileHandleId]uint64),
	}
}

func (i *FileHandleToInode) GetFileHandle(fh FileHandleId) *FileHandle {
	i.RLock()
	defer i.RUnlock()
	inode, found := i.fh2inode[fh]
	if found {
		return i.inode2fh[inode]
	}
	return nil
}

func (i *FileHandleToInode) FindFileHandle(inode uint64) (fh *FileHandle, found bool) {
	i.RLock()
	defer i.RUnlock()
	fh, found = i.inode2fh[inode]
	return
}

// MarkInodeRenamed sets isRenamed on any file handle associated with the
// given inode.  This prevents the async flush from recreating a renamed
// file's metadata under its old path.
func (i *FileHandleToInode) MarkInodeRenamed(inode uint64) {
	i.RLock()
	defer i.RUnlock()
	if fh, ok := i.inode2fh[inode]; ok {
		fh.isRenamed = true
	}
}

func (i *FileHandleToInode) AcquireFileHandle(wfs *WFS, inode uint64, entry *filer_pb.Entry) *FileHandle {
	fh, _ := i.AcquireFileHandleWithVersion(wfs, inode, entry, 0)
	return fh
}

// AcquireFileHandleWithVersion creates a handle fully initialized — entry and
// the filer log position it reflects — before exposing it in the map. For an
// existing handle it only bumps the counter: its entry belongs to whoever
// holds the handle lock, so any install from a fresh lookup must happen there
// (see AcquireHandle), not under the map lock.
func (i *FileHandleToInode) AcquireFileHandleWithVersion(wfs *WFS, inode uint64, entry *filer_pb.Entry, versionTsNs int64) (*FileHandle, bool) {
	i.Lock()
	defer i.Unlock()
	fh, found := i.inode2fh[inode]
	if !found {
		fh = newFileHandle(wfs, FileHandleId(util.RandomUint64()), inode, entry)
		fh.entryVersionTsNs.Store(versionTsNs)
		i.inode2fh[inode] = fh
		i.fh2inode[fh.fh] = inode
		return fh, false
	}
	fh.counter++
	return fh, true
}

func (i *FileHandleToInode) ReleaseByHandle(fh FileHandleId) *FileHandle {
	i.Lock()
	defer i.Unlock()

	inode, found := i.fh2inode[fh]
	if !found {
		return nil
	}

	fhHandle, fhFound := i.inode2fh[inode]
	if !fhFound {
		delete(i.fh2inode, fh)
		return nil
	}

	// If the counter is already <= 0, a prior Release already started the
	// drain.  Return nil to prevent double-processing.
	if fhHandle.counter <= 0 {
		return nil
	}

	fhHandle.counter--
	if fhHandle.counter <= 0 {
		if fhHandle.asyncFlushPending {
			// Handle stays in fhMap so rename/unlink can still find it
			// via FindFileHandle during the background drain.
			return fhHandle
		}
		delete(i.inode2fh, inode)
		delete(i.fh2inode, fhHandle.fh)
		return fhHandle
	}
	return nil
}

// RemoveFileHandle removes a handle from both maps.  Called after an async
// drain completes to clean up the handle that was intentionally kept in the
// maps during the flush.
func (i *FileHandleToInode) RemoveFileHandle(fh FileHandleId, inode uint64) {
	i.Lock()
	defer i.Unlock()
	delete(i.inode2fh, inode)
	delete(i.fh2inode, fh)
}
