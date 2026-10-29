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

// AcquireFileHandle fully initializes a handle — entry and the log position it
// reflects — before exposing it in the map, and reports whether one already
// existed. An existing handle only gets its counter bumped: its entry belongs
// to whoever holds the handle lock, so callers install there (AcquireHandle)
// rather than under this one.
// Every open gets its own fh value over the shared FileHandle so that
// RELEASE — which identifies nothing else about the dying description — can
// scope per-open state (e.g. OFD lock owners) to the open that is closing.
//
// A handle whose counter already reached zero is draining its deferred flush:
// it stays in the map only so rename/unlink can still find it, and
// RemoveFileHandle will drop it (and every fh id minted for it) when the drain
// completes. Attaching here would hand the kernel an fh id that vanishes mid-
// open — reads on it return ENOENT, which the kernel reports to read() as EIO.
// Callers must treat a draining report as "wait for the pending flush, then
// retry the whole acquire", not as success.
func (i *FileHandleToInode) AcquireFileHandle(wfs *WFS, inode uint64, entry *filer_pb.Entry, versionTsNs int64, signature int32) (*FileHandle, FileHandleId, bool, bool) {
	i.Lock()
	defer i.Unlock()
	fh, found := i.inode2fh[inode]
	if !found {
		fh = newFileHandle(wfs, FileHandleId(util.RandomUint64()), inode, entry)
		fh.entryVersionTsNs.Store(versionTsNs)
		fh.entryVersionSignature.Store(signature)
		i.inode2fh[inode] = fh
		i.fh2inode[fh.fh] = inode
		return fh, fh.fh, false, false
	}
	if fh.counter <= 0 {
		return nil, 0, true, true
	}
	fh.counter++
	openFh := FileHandleId(util.RandomUint64())
	fh.fhs[openFh] = struct{}{}
	i.fh2inode[openFh] = inode
	return fh, openFh, true, false
}

func (i *FileHandleToInode) ReleaseByHandle(fh FileHandleId) *FileHandle {
	i.Lock()
	defer i.Unlock()

	inode, found := i.fh2inode[fh]
	if !found {
		return nil
	}
	// The released id never resolves again: a duplicate RELEASE on it must
	// not decrement the counter a second time.
	delete(i.fh2inode, fh)

	fhHandle, fhFound := i.inode2fh[inode]
	if !fhFound {
		return nil
	}
	delete(fhHandle.fhs, fh)

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
		for openFh := range fhHandle.fhs {
			delete(i.fh2inode, openFh)
		}
		fhHandle.fhs = nil
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
	if fhHandle, ok := i.inode2fh[inode]; ok {
		for openFh := range fhHandle.fhs {
			delete(i.fh2inode, openFh)
		}
	}
	delete(i.inode2fh, inode)
	delete(i.fh2inode, fh)
}

// openFhIds returns the live fh values minted for an inode's shared handle —
// one per still-open description.
func (i *FileHandleToInode) openFhIds(inode uint64) []FileHandleId {
	i.RLock()
	defer i.RUnlock()
	fh := i.inode2fh[inode]
	if fh == nil {
		return nil
	}
	ids := make([]FileHandleId, 0, len(fh.fhs))
	for id := range fh.fhs {
		ids = append(ids, id)
	}
	return ids
}
