package mount

import (
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/util"
	"sync"
)

type InodeToPath struct {
	sync.RWMutex
	nextInodeId uint64
	inode2path  map[uint64]*InodeEntry
	path2inode  map[util.FullPath]uint64
}
type InodeEntry struct {
	util.FullPath
	nlookup uint64
}

func NewInodeToPath() *InodeToPath {
	return &InodeToPath{
		inode2path:  make(map[uint64]*InodeEntry),
		path2inode:  make(map[util.FullPath]uint64),
		nextInodeId: 2, // the root inode id is 1
	}
}

func (i *InodeToPath) Lookup(path util.FullPath) uint64 {
	if path == "/" {
		return 1
	}
	i.Lock()
	defer i.Unlock()
	inode, found := i.path2inode[path]
	if !found {
		inode = i.nextInodeId
		i.nextInodeId++
		i.path2inode[path] = inode
		i.inode2path[inode] = &InodeEntry{path, 1}
	} else {
		i.inode2path[inode].nlookup++
	}
	return inode
}

func (i *InodeToPath) GetInode(path util.FullPath) uint64 {
	if path == "/" {
		return 1
	}
	i.Lock()
	defer i.Unlock()
	inode, found := i.path2inode[path]
	if !found {
		glog.Fatalf("GetInode unknown inode %d", inode)
	}
	return inode
}

func (i *InodeToPath) GetPath(inode uint64) util.FullPath {
	if inode == 1 {
		return "/"
	}
	i.RLock()
	defer i.RUnlock()
	path, found := i.inode2path[inode]
	if !found {
		glog.Fatalf("not found inode %d", inode)
	}
	return path.FullPath
}

func (i *InodeToPath) HasPath(path util.FullPath) bool {
	if path == "/" {
		return true
	}
	i.RLock()
	defer i.RUnlock()
	_, found := i.path2inode[path]
	return found
}

func (i *InodeToPath) HasInode(inode uint64) bool {
	if inode == 1 {
		return true
	}
	i.RLock()
	defer i.RUnlock()
	_, found := i.inode2path[inode]
	return found
}

func (i *InodeToPath) RemovePath(path util.FullPath) {
	if path == "/" {
		return
	}
	i.Lock()
	defer i.Unlock()
	inode, found := i.path2inode[path]
	if found {
		delete(i.path2inode, path)
		delete(i.inode2path, inode)
	}
}

func (i *InodeToPath) Forget(inode, nlookup uint64) {
	if inode == 1 {
		return
	}
	i.Lock()
	defer i.Unlock()
	path, found := i.inode2path[inode]
	if found {
		path.nlookup -= nlookup
		if path.nlookup <= 0 {
			delete(i.path2inode, path.FullPath)
			delete(i.inode2path, inode)
		}
	}
}
