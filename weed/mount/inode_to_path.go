package mount

import (
	"sync"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

type InodeToPath struct {
	sync.RWMutex
	nextInodeId     uint64
	cacheMetaTtlSec time.Duration
	inode2path      map[uint64]*InodeEntry
	path2inode      map[util.FullPath]uint64
	// dirStates holds directory-only readdir-cache state, keyed by inode. An
	// inode is a directory iff it has an entry here, registered at creation.
	dirStates map[uint64]*dirState
}

// InodeEntry exists per inode the kernel references. Directory cache state is
// kept out in dirStates so a file entry stays in the 32-byte size class — the
// dominant cost on a mount with millions of files.
type InodeEntry struct {
	paths   []util.FullPath
	nlookup uint64
}

type dirState struct {
	isChildrenCached  bool
	readDirDirect     bool
	cachedExpiresTime time.Time
	lastAccess        time.Time
	lastRefresh       time.Time
	updateWindowStart time.Time
	updateCount       int
	subdirCount       int32 // tracked in-memory for POSIX directory nlink
}

func (d *dirState) resetCacheState() {
	d.isChildrenCached = false
	d.readDirDirect = false
	d.cachedExpiresTime = time.Time{}
	d.updateCount = 0
	d.updateWindowStart = time.Time{}
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
	return true
}

func NewInodeToPath(root util.FullPath, ttlSec int) *InodeToPath {
	t := &InodeToPath{
		inode2path:      make(map[uint64]*InodeEntry),
		path2inode:      make(map[util.FullPath]uint64),
		dirStates:       make(map[uint64]*dirState),
		cacheMetaTtlSec: time.Second * time.Duration(ttlSec),
	}
	t.inode2path[1] = &InodeEntry{
		paths:   []util.FullPath{root},
		nlookup: 1,
	}
	t.dirStates[1] = &dirState{lastAccess: time.Now()}
	t.path2inode[root] = 1

	return t
}

// EnsurePath make sure the full path is tracked, used by symlink.
func (i *InodeToPath) EnsurePath(path util.FullPath, isDirectory bool) bool {
	dir, _ := path.DirAndName()
	if dir == "/" {
		return true
	}
	if i.EnsurePath(util.FullPath(dir), true) {
		i.Lookup(path, time.Now().Unix(), isDirectory, false, 0, false)
		return true
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
		nlookup := uint64(0)
		if isLookup {
			nlookup = 1
		}
		i.inode2path[inode] = &InodeEntry{
			paths:   []util.FullPath{path},
			nlookup: nlookup,
		}
		if isDirectory {
			i.dirStates[inode] = &dirState{}
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

// GetAllPaths returns a copy of all paths associated with an inode. For a
// hard-linked file, this includes every link that the mount currently knows
// about. Returns nil if the inode is unknown.
func (i *InodeToPath) GetAllPaths(inode uint64) []util.FullPath {
	i.RLock()
	defer i.RUnlock()
	ie, found := i.inode2path[inode]
	if !found || len(ie.paths) == 0 {
		return nil
	}
	out := make([]util.FullPath, len(ie.paths))
	copy(out, ie.paths)
	return out
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
	d, found := i.dirStates[inode]
	if !found {
		glog.Warningf("MarkChildrenCached inode %d not a tracked directory for %v", inode, fullpath)
		return
	}
	d.isChildrenCached = true
	d.readDirDirect = false
	now := time.Now()
	d.lastAccess = now
	d.lastRefresh = now
	d.updateCount = 0
	d.updateWindowStart = time.Time{}
	if i.cacheMetaTtlSec > 0 {
		d.cachedExpiresTime = now.Add(i.cacheMetaTtlSec)
	}
}

func (i *InodeToPath) IsChildrenCached(fullpath util.FullPath) bool {
	i.RLock()
	defer i.RUnlock()
	inode, found := i.path2inode[fullpath]
	if !found {
		return false
	}
	d := i.dirStates[inode]
	if d == nil {
		return false
	}
	if d.isChildrenCached {
		return d.cachedExpiresTime.IsZero() || time.Now().Before(d.cachedExpiresTime)
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

func (i *InodeToPath) InvalidateAllChildrenCache() {
	i.Lock()
	defer i.Unlock()
	for _, d := range i.dirStates {
		if d.isChildrenCached {
			d.resetCacheState()
		}
	}
}

func (i *InodeToPath) InvalidateChildrenCache(fullpath util.FullPath) {
	i.Lock()
	defer i.Unlock()
	inode, found := i.path2inode[fullpath]
	if !found {
		return
	}
	if d := i.dirStates[inode]; d != nil {
		d.resetCacheState()
	}
}

// AdjustSubdirCount adjusts the subdirectory count for a directory inode.
// delta is typically +1 (mkdir) or -1 (rmdir).
func (i *InodeToPath) AdjustSubdirCount(dirPath util.FullPath, delta int32) {
	i.Lock()
	defer i.Unlock()
	inode, found := i.path2inode[dirPath]
	if !found {
		return
	}
	d := i.dirStates[inode]
	if d == nil {
		return
	}
	d.subdirCount += delta
	if d.subdirCount < 0 {
		d.subdirCount = 0
	}
}

// GetSubdirCount returns the tracked subdirectory count for a directory.
func (i *InodeToPath) GetSubdirCount(dirPath util.FullPath) int32 {
	i.RLock()
	defer i.RUnlock()
	inode, found := i.path2inode[dirPath]
	if !found {
		return 0
	}
	d := i.dirStates[inode]
	if d == nil {
		return 0
	}
	return d.subdirCount
}

// SetSubdirCount sets the subdirectory count for a directory (used after readdir).
func (i *InodeToPath) SetSubdirCount(dirPath util.FullPath, count int32) {
	i.Lock()
	defer i.Unlock()
	inode, found := i.path2inode[dirPath]
	if !found {
		return
	}
	if d := i.dirStates[inode]; d != nil {
		d.subdirCount = count
	}
}

func (i *InodeToPath) TouchDirectory(fullpath util.FullPath) {
	i.Lock()
	defer i.Unlock()
	inode, found := i.path2inode[fullpath]
	if !found {
		return
	}
	if d := i.dirStates[inode]; d != nil {
		d.lastAccess = time.Now()
	}
}

func (i *InodeToPath) MarkDirectoryReadThrough(fullpath util.FullPath, now time.Time) bool {
	i.Lock()
	defer i.Unlock()
	inode, found := i.path2inode[fullpath]
	if !found {
		return false
	}
	d := i.dirStates[inode]
	if d == nil {
		return false
	}
	d.isChildrenCached = false
	d.readDirDirect = true
	d.cachedExpiresTime = time.Time{}
	d.lastAccess = now
	d.lastRefresh = time.Time{}
	d.updateCount = 0
	d.updateWindowStart = time.Time{}
	return true
}

func (i *InodeToPath) RecordDirectoryUpdate(fullpath util.FullPath, now time.Time, window time.Duration, threshold int) bool {
	if threshold <= 0 || window <= 0 {
		return false
	}
	i.Lock()
	defer i.Unlock()
	inode, found := i.path2inode[fullpath]
	if !found {
		return false
	}
	d := i.dirStates[inode]
	if d == nil || !d.isChildrenCached {
		return false
	}
	if d.updateWindowStart.IsZero() || now.Sub(d.updateWindowStart) > window {
		d.updateWindowStart = now
		d.updateCount = 0
	}
	d.updateCount++
	if d.updateCount >= threshold {
		d.isChildrenCached = false
		d.readDirDirect = true
		d.cachedExpiresTime = time.Time{}
		d.lastAccess = now
		d.lastRefresh = time.Time{}
		d.updateCount = 0
		d.updateWindowStart = time.Time{}
		return true
	}
	return false
}

func (i *InodeToPath) ShouldReadDirectoryDirect(fullpath util.FullPath) bool {
	i.RLock()
	defer i.RUnlock()
	inode, found := i.path2inode[fullpath]
	if !found {
		return false
	}
	d := i.dirStates[inode]
	if d == nil {
		return false
	}
	return d.readDirDirect
}

func (i *InodeToPath) MarkDirectoryRefreshed(fullpath util.FullPath, now time.Time) {
	i.Lock()
	defer i.Unlock()
	inode, found := i.path2inode[fullpath]
	if !found {
		return
	}
	d := i.dirStates[inode]
	if d == nil {
		return
	}
	d.lastRefresh = now
	d.lastAccess = now
	d.readDirDirect = false
	d.updateCount = 0
	d.updateWindowStart = time.Time{}
	if i.cacheMetaTtlSec > 0 {
		d.cachedExpiresTime = now.Add(i.cacheMetaTtlSec)
	}
}

func (i *InodeToPath) CollectEvictableDirs(now time.Time, idle time.Duration) []util.FullPath {
	if idle <= 0 {
		return nil
	}
	i.Lock()
	defer i.Unlock()
	var dirs []util.FullPath
	for inode, d := range i.dirStates {
		if !d.isChildrenCached {
			continue
		}
		if d.lastAccess.IsZero() || now.Sub(d.lastAccess) < idle {
			continue
		}
		d.resetCacheState()
		if entry, ok := i.inode2path[inode]; ok {
			dirs = append(dirs, entry.paths...)
		}
	}
	return dirs
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
			paths:   []util.FullPath{path},
			nlookup: 1,
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
		if d := i.dirStates[sourceInode]; d != nil {
			d.resetCacheState()
		}
	} else {
		glog.Errorf("MovePath %s to %s: sourceInode %d not found", sourcePath, targetPath, sourceInode)
	}
	return
}

func (i *InodeToPath) Forget(inode, nlookup uint64, onForgetDir func(dir util.FullPath)) {
	var dirPaths []util.FullPath
	callOnForgetDir := false

	i.Lock()
	path, found := i.inode2path[inode]
	if found {
		if nlookup > path.nlookup {
			glog.Errorf("kernel forget over-decrement: inode %d paths %v current %d forget %d", inode, path.paths, path.nlookup, nlookup)
			path.nlookup = 0
		} else {
			path.nlookup -= nlookup
		}
		glog.V(4).Infof("kernel forget: inode %d paths %v nlookup %d", inode, path.paths, path.nlookup)
		if path.nlookup == 0 {
			if _, isDir := i.dirStates[inode]; isDir && onForgetDir != nil {
				dirPaths = append([]util.FullPath(nil), path.paths...)
				callOnForgetDir = true
			}
			for _, p := range path.paths {
				delete(i.path2inode, p)
			}
			delete(i.inode2path, inode)
			delete(i.dirStates, inode)
		} else {
			glog.V(4).Infof("kernel forget but nlookup not zero: inode %d paths %v nlookup %d", inode, path.paths, path.nlookup)
		}
	} else {
		glog.Warningf("kernel forget but inode not found: inode %d", inode)
	}
	i.Unlock()

	if callOnForgetDir {
		for _, p := range dirPaths {
			onForgetDir(p)
		}
	}
}
