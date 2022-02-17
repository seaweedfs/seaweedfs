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
	nlookup          uint64
	isDirectory      bool
	isChildrenCached bool
}

func NewInodeToPath() *InodeToPath {
	t := &InodeToPath{
		inode2path:  make(map[uint64]*InodeEntry),
		path2inode:  make(map[util.FullPath]uint64),
		nextInodeId: 2, // the root inode id is 1
	}
	t.inode2path[1] = &InodeEntry{"/", 1, true, false}
	t.path2inode["/"] = 1
	return t
}

func (i *InodeToPath) Lookup(path util.FullPath, isDirectory bool, isLookup bool) uint64 {
	i.Lock()
	defer i.Unlock()
	inode, found := i.path2inode[path]
	if !found {
		inode = i.nextInodeId
		i.nextInodeId++
		i.path2inode[path] = inode
		if !isLookup {
			i.inode2path[inode] = &InodeEntry{path, 0, isDirectory, false}
		} else {
			i.inode2path[inode] = &InodeEntry{path, 1, isDirectory, false}
		}
	} else {
		if isLookup {
			i.inode2path[inode].nlookup++
		}
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
		// glog.Fatalf("GetInode unknown inode for %s", path)
		// this could be the parent for mount point
	}
	return inode
}

func (i *InodeToPath) GetPath(inode uint64) util.FullPath {
	i.RLock()
	defer i.RUnlock()
	path, found := i.inode2path[inode]
	if !found {
		glog.Fatalf("not found inode %d", inode)
	}
	return path.FullPath
}

func (i *InodeToPath) HasPath(path util.FullPath) bool {
	i.RLock()
	defer i.RUnlock()
	_, found := i.path2inode[path]
	return found
}

func (i *InodeToPath) MarkChildrenCached(fullpath util.FullPath) {
	i.RLock()
	defer i.RUnlock()
	inode, found := i.path2inode[fullpath]
	if !found {
		glog.Fatalf("MarkChildrenCached not found inode %v", fullpath)
	}
	path, found := i.inode2path[inode]
	path.isChildrenCached = true
}

func (i *InodeToPath) IsChildrenCached(fullpath util.FullPath) bool {
	i.RLock()
	defer i.RUnlock()
	inode, found := i.path2inode[fullpath]
	if !found {
		return false
	}
	path, found := i.inode2path[inode]
	if found {
		return path.isChildrenCached
	}
	return false
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
	i.Lock()
	defer i.Unlock()
	inode, found := i.path2inode[path]
	if found {
		delete(i.path2inode, path)
		delete(i.inode2path, inode)
	}
}

func (i *InodeToPath) MovePath(sourcePath, targetPath util.FullPath) {
	i.Lock()
	defer i.Unlock()
	sourceInode, sourceFound := i.path2inode[sourcePath]
	targetInode, targetFound := i.path2inode[targetPath]
	if sourceFound {
		delete(i.path2inode, sourcePath)
		i.path2inode[targetPath] = sourceInode
	} else {
		// it is possible some source folder items has not been visited before
		// so no need to worry about their source inodes
		return
	}
	i.inode2path[sourceInode].FullPath = targetPath
	if targetFound {
		delete(i.inode2path, targetInode)
	} else {
		i.inode2path[sourceInode].nlookup++
	}
}

func (i *InodeToPath) Forget(inode, nlookup uint64, onForgetDir func(dir util.FullPath)) {
	i.Lock()
	path, found := i.inode2path[inode]
	if found {
		path.nlookup -= nlookup
		if path.nlookup <= 0 {
			delete(i.path2inode, path.FullPath)
			delete(i.inode2path, inode)
		}
	}
	i.Unlock()
	if found {
		if path.isDirectory && onForgetDir != nil {
			onForgetDir(path.FullPath)
		}
	}
}
