package mount

import (
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"sync"
	"time"
)

type InodeToPath struct {
	sync.RWMutex
	nextInodeId     uint64
	cacheMetaTtlSec time.Duration
	inode2path      map[uint64]*InodeEntry
	path2inode      map[util.FullPath]uint64
}
type InodeEntry struct {
	paths             []util.FullPath
	nlookup           uint64
	isDirectory       bool
	isChildrenCached  bool
	cachedExpiresTime time.Time
}

func (ie *InodeEntry) removeOnePath(p util.FullPath) bool {
	if len(ie.paths) == 0 {
		return false
	}
	idx := -1
	for i, x := range ie.paths {
		if x == p {
			idx = i
			break
		}
	}
	if idx < 0 {
		return false
	}
	for x := idx; x < len(ie.paths)-1; x++ {
		ie.paths[x] = ie.paths[x+1]
	}
	ie.paths = ie.paths[0 : len(ie.paths)-1]
	ie.nlookup--
	return true
}

func NewInodeToPath(root util.FullPath, ttlSec int) *InodeToPath {
	t := &InodeToPath{
		inode2path:      make(map[uint64]*InodeEntry),
		path2inode:      make(map[util.FullPath]uint64),
		cacheMetaTtlSec: time.Second * time.Duration(ttlSec),
	}
	t.inode2path[1] = &InodeEntry{[]util.FullPath{root}, 1, true, false, time.Time{}}
	t.path2inode[root] = 1

	return t
}

// EnsurePath make sure the full path is tracked, used by symlink.
func (i *InodeToPath) EnsurePath(path util.FullPath, isDirectory bool) bool {
	for {
		dir, _ := path.DirAndName()
		if dir == "/" {
			return true
		}
		if i.EnsurePath(util.FullPath(dir), true) {
			i.Lookup(path, time.Now().Unix(), isDirectory, false, 0, false)
			return true
		}
	}
	return false
}

func (i *InodeToPath) Lookup(path util.FullPath, unixTime int64, isDirectory bool, isHardlink bool, possibleInode uint64, isLookup bool) uint64 {
	i.Lock()
	defer i.Unlock()
	inode, found := i.path2inode[path]
	if !found {
		if possibleInode == 0 {
			inode = path.AsInode(unixTime)
		} else {
			inode = possibleInode
		}
		if !isHardlink {
			for _, found := i.inode2path[inode]; found; inode++ {
				_, found = i.inode2path[inode+1]
			}
		}
	}
	i.path2inode[path] = inode

	if _, found := i.inode2path[inode]; found {
		if isLookup {
			i.inode2path[inode].nlookup++
		}
	} else {
		if !isLookup {
			i.inode2path[inode] = &InodeEntry{[]util.FullPath{path}, 0, isDirectory, false, time.Time{}}
		} else {
			i.inode2path[inode] = &InodeEntry{[]util.FullPath{path}, 1, isDirectory, false, time.Time{}}
		}
	}

	return inode
}

func (i *InodeToPath) AllocateInode(path util.FullPath, unixTime int64) uint64 {
	if path == "/" {
		return 1
	}
	i.Lock()
	defer i.Unlock()
	inode := path.AsInode(unixTime)
	for _, found := i.inode2path[inode]; found; inode++ {
		_, found = i.inode2path[inode]
	}
	return inode
}

func (i *InodeToPath) GetInode(path util.FullPath) (uint64, bool) {
	if path == "/" {
		return 1, true
	}
	i.Lock()
	defer i.Unlock()
	inode, found := i.path2inode[path]
	if !found {
		// glog.Fatalf("GetInode unknown inode for %s", path)
		// this could be the parent for mount point
	}
	return inode, found
}

func (i *InodeToPath) GetPath(inode uint64) (util.FullPath, fuse.Status) {
	i.RLock()
	defer i.RUnlock()
	path, found := i.inode2path[inode]
	if !found || len(path.paths) == 0 {
		return "", fuse.ENOENT
	}
	return path.paths[0], fuse.OK
}

func (i *InodeToPath) HasPath(path util.FullPath) bool {
	i.RLock()
	defer i.RUnlock()
	_, found := i.path2inode[path]
	return found
}

func (i *InodeToPath) MarkChildrenCached(fullpath util.FullPath) {
	i.Lock()
	defer i.Unlock()
	inode, found := i.path2inode[fullpath]
	if !found {
		// https://github.com/seaweedfs/seaweedfs/issues/4968
		// glog.Fatalf("MarkChildrenCached not found inode %v", fullpath)
		glog.Warningf("MarkChildrenCached not found inode %v", fullpath)
		return
	}
	path, found := i.inode2path[inode]
	path.isChildrenCached = true
	if i.cacheMetaTtlSec > 0 {
		path.cachedExpiresTime = time.Now().Add(i.cacheMetaTtlSec)
	}
}

func (i *InodeToPath) IsChildrenCached(fullpath util.FullPath) bool {
	i.RLock()
	defer i.RUnlock()
	inode, found := i.path2inode[fullpath]
	if !found {
		return false
	}
	path, found := i.inode2path[inode]
	if !found {
		return false
	}
	if path.isChildrenCached {
		return path.cachedExpiresTime.IsZero() || time.Now().Before(path.cachedExpiresTime)
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

func (i *InodeToPath) AddPath(inode uint64, path util.FullPath) {
	i.Lock()
	defer i.Unlock()
	i.path2inode[path] = inode

	ie, found := i.inode2path[inode]
	if found {
		ie.paths = append(ie.paths, path)
		ie.nlookup++
	} else {
		i.inode2path[inode] = &InodeEntry{
			paths:            []util.FullPath{path},
			nlookup:          1,
			isDirectory:      false,
			isChildrenCached: false,
		}
	}
}

func (i *InodeToPath) RemovePath(path util.FullPath) {
	i.Lock()
	defer i.Unlock()
	inode, found := i.path2inode[path]
	if found {
		delete(i.path2inode, path)
		i.removePathFromInode2Path(inode, path)
	}
}

func (i *InodeToPath) removePathFromInode2Path(inode uint64, path util.FullPath) {
	ie, found := i.inode2path[inode]
	if !found {
		return
	}
	if !ie.removeOnePath(path) {
		return
	}
	if len(ie.paths) == 0 {
		delete(i.inode2path, inode)
	}
}

func (i *InodeToPath) MovePath(sourcePath, targetPath util.FullPath) (sourceInode, targetInode uint64) {
	i.Lock()
	defer i.Unlock()
	sourceInode, sourceFound := i.path2inode[sourcePath]
	targetInode, targetFound := i.path2inode[targetPath]
	if targetFound {
		i.removePathFromInode2Path(targetInode, targetPath)
		delete(i.path2inode, targetPath)
	}
	if sourceFound {
		delete(i.path2inode, sourcePath)
		i.path2inode[targetPath] = sourceInode
	} else {
		// it is possible some source folder items has not been visited before
		// so no need to worry about their source inodes
		return
	}
	if entry, entryFound := i.inode2path[sourceInode]; entryFound {
		for i, p := range entry.paths {
			if p == sourcePath {
				entry.paths[i] = targetPath
			}
		}
		entry.isChildrenCached = false
		if !targetFound {
			entry.nlookup++
		}
	} else {
		glog.Errorf("MovePath %s to %s: sourceInode %d not found", sourcePath, targetPath, sourceInode)
	}
	return
}

func (i *InodeToPath) Forget(inode, nlookup uint64, onForgetDir func(dir util.FullPath)) {
	i.Lock()
	path, found := i.inode2path[inode]
	if found {
		path.nlookup -= nlookup
		if path.nlookup <= 0 {
			for _, p := range path.paths {
				delete(i.path2inode, p)
			}
			delete(i.inode2path, inode)
		}
	}
	i.Unlock()
	if found {
		if path.isDirectory && path.nlookup <= 0 && onForgetDir != nil {
			path.isChildrenCached = false
			for _, p := range path.paths {
				onForgetDir(p)
			}
		}
	}
}
