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

func (i *FileHandleToInode) AcquireFileHandle(wfs *WFS, inode uint64, entry *filer_pb.Entry) *FileHandle {
	i.Lock()
	defer i.Unlock()
	fh, found := i.inode2fh[inode]
	if !found {
		fh = newFileHandle(wfs, FileHandleId(util.RandomUint64()), inode, entry)
		i.inode2fh[inode] = fh
		i.fh2inode[fh.fh] = inode
	} else {
		fh.counter++
	}
	if fh.GetEntry().GetEntry() != entry {
		fh.SetEntry(entry)
	}
	return fh
}

func (i *FileHandleToInode) ReleaseByInode(inode uint64) *FileHandle {
	i.Lock()
	defer i.Unlock()
	fh, found := i.inode2fh[inode]
	if !found {
		return nil
	}
	// If the counter is already <= 0, a prior Release already started the
	// drain.  Return nil to prevent double-processing (e.g. Forget after Release).
	if fh.counter <= 0 {
		return nil
	}
	fh.counter--
	if fh.counter <= 0 {
		if fh.asyncFlushPending {
			// Handle stays in fhMap so rename/unlink can find it during drain.
			return fh
		}
		delete(i.inode2fh, inode)
		delete(i.fh2inode, fh.fh)
		return fh
	}
	return nil
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
