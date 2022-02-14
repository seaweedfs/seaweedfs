package mount

import (
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"sync"
)

type FileHandleToInode struct {
	sync.RWMutex
	nextFh   FileHandleId
	inode2fh map[uint64]*FileHandle
	fh2inode map[FileHandleId]uint64
}

func NewFileHandleToInode() *FileHandleToInode {
	return &FileHandleToInode{
		inode2fh: make(map[uint64]*FileHandle),
		fh2inode: make(map[FileHandleId]uint64),
		nextFh:   0,
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
		fh = newFileHandle(wfs, i.nextFh, inode, entry)
		i.nextFh++
		i.inode2fh[inode] = fh
		i.fh2inode[fh.fh] = inode
	} else {
		fh.counter++
	}
	return fh
}

func (i *FileHandleToInode) ReleaseByInode(inode uint64) {
	i.Lock()
	defer i.Unlock()
	fh, found := i.inode2fh[inode]
	if found {
		fh.counter--
		if fh.counter <= 0 {
			delete(i.inode2fh, inode)
			delete(i.fh2inode, fh.fh)
		}
	}
}
func (i *FileHandleToInode) ReleaseByHandle(fh FileHandleId) {
	i.Lock()
	defer i.Unlock()
	inode, found := i.fh2inode[fh]
	if found {
		fhHandle, fhFound := i.inode2fh[inode]
		if !fhFound {
			delete(i.fh2inode, fh)
		} else {
			fhHandle.counter--
			if fhHandle.counter <= 0 {
				delete(i.inode2fh, inode)
				delete(i.fh2inode, fhHandle.fh)
			}
		}

	}
}
