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
// dominant cost on a mount with millions of files. A hard link's extra paths
// hang off a pointer for the same reason.
type InodeEntry struct {
	path       util.FullPath
	nlookup    uint64
	extraPaths *[]util.FullPath
}

type dirState struct {
	isChildrenCached  bool
	readDirDirect     bool
	cachedExpiresTime time.Time
	lastAccess        time.Time
	lastRefresh       time.Time
	subdirCount       int32 // tracked in-memory for POSIX directory nlink
}

func (d *dirState) resetCacheState() {
	d.isChildrenCached = false
	d.readDirDirect = false
	d.cachedExpiresTime = time.Time{}
}

// appendPaths appends every path the inode is reachable by, primary first.
func (ie *InodeEntry) appendPaths(dst []util.FullPath) []util.FullPath {
	if ie.path == "" {
		return dst
	}
	dst = append(dst, ie.path)
	if ie.extraPaths != nil {
		dst = append(dst, *ie.extraPaths...)
	}
	return dst
}

func (ie *InodeEntry) addPath(p util.FullPath) {
	if ie.path == "" {
		ie.path = p
		return
	}
	if ie.extraPaths == nil {
		ie.extraPaths = &[]util.FullPath{p}
		return
	}
	*ie.extraPaths = append(*ie.extraPaths, p)
}

func (ie *InodeEntry) replacePath(from, to util.FullPath) {
	if ie.path == from {
		ie.path = to
	}
	if ie.extraPaths == nil {
		return
	}
	for i, p := range *ie.extraPaths {
		if p == from {
			(*ie.extraPaths)[i] = to
		}
	}
}

func (ie *InodeEntry) setExtraPaths(extra []util.FullPath) {
	if len(extra) == 0 {
		ie.extraPaths = nil
		return
	}
	ie.extraPaths = &extra
}

// removeOnePath promotes an extra path when the primary is the one going away,
// so an entry that still has a path always has a primary one.
func (ie *InodeEntry) removeOnePath(p util.FullPath) bool {
	if ie.path == "" {
		return false
	}
	if ie.path == p {
		if ie.extraPaths == nil {
			ie.path = ""
			return true
		}
		extra := *ie.extraPaths
		ie.path = extra[0]
		ie.setExtraPaths(extra[1:])
		return true
	}
	if ie.extraPaths == nil {
		return false
	}
	extra := *ie.extraPaths
	for i, x := range extra {
		if x != p {
			continue
		}
		ie.setExtraPaths(append(extra[:i], extra[i+1:]...))
		return true
	}
	return false
}

func NewInodeToPath(root util.FullPath, ttlSec int) *InodeToPath {
	t := &InodeToPath{
		inode2path:      make(map[uint64]*InodeEntry),
		path2inode:      make(map[util.FullPath]uint64),
		dirStates:       make(map[uint64]*dirState),
		cacheMetaTtlSec: time.Second * time.Duration(ttlSec),
	}
	t.inode2path[1] = &InodeEntry{
		path:    root,
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
			path:    path,
			nlookup: nlookup,
		}
		if isDirectory {
			i.dirStates[inode] = &dirState{}
		}
	}

	return inode
}

// IncrementNlookup takes one more reference on an inode already in the table,
// reporting false if it is not there.
func (i *InodeToPath) IncrementNlookup(inode uint64) bool {
	i.Lock()
	defer i.Unlock()
	entry, found := i.inode2path[inode]
	if !found {
		return false
	}
	entry.nlookup++
	return true
}

// InodeForListing returns the inode number a readdir should report for path
// without entering it in the table. Nothing is reserved, so the collision probe
// Lookup does is skipped: the worst case is a repeated st_ino in one listing.
func (i *InodeToPath) InodeForListing(path util.FullPath, unixTime int64, possibleInode uint64) uint64 {
	i.RLock()
	inode, found := i.path2inode[path]
	i.RUnlock()
	if found {
		return inode
	}
	if possibleInode != 0 {
		return possibleInode
	}
	return path.AsInode(unixTime)
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
	if !found || path.path == "" {
		return "", fuse.ENOENT
	}
	return path.path, fuse.OK
}

// GetAllPaths returns a copy of all paths associated with an inode. For a
// hard-linked file, this includes every link that the mount currently knows
// about. Returns nil if the inode is unknown.
func (i *InodeToPath) GetAllPaths(inode uint64) []util.FullPath {
	i.RLock()
	defer i.RUnlock()
	ie, found := i.inode2path[inode]
	if !found {
		return nil
	}
	return ie.appendPaths(nil)
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
	return true
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
			dirs = entry.appendPaths(dirs)
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
		ie.addPath(path)
		ie.nlookup++
	} else {
		i.inode2path[inode] = &InodeEntry{
			path:    path,
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
	if !sourceFound {
		// Nothing of ours to move: the source was never visited here, or a
		// redelivery already moved it. Whatever sits at the target is not ours
		// to take apart on the strength of an absent source, and deciding that
		// outside this lock would race a concurrent move to the same target.
		return 0, 0
	}
	targetInode, targetFound := i.path2inode[targetPath]
	if targetFound {
		i.removePathFromInode2Path(targetInode, targetPath)
		delete(i.path2inode, targetPath)
	}
	delete(i.path2inode, sourcePath)
	i.path2inode[targetPath] = sourceInode
	if entry, entryFound := i.inode2path[sourceInode]; entryFound {
		entry.replacePath(sourcePath, targetPath)
		if d := i.dirStates[sourceInode]; d != nil {
			d.resetCacheState()
		}
	} else {
		glog.Errorf("MovePath %s to %s: sourceInode %d not found", sourcePath, targetPath, sourceInode)
	}
	return
}

// Forget drops nlookup references. onRelease, if given, runs at the moment the
// inode is released and while the table is still locked: state keyed by the
// inode number has to be dropped there, because the number is derived from the
// path and a lookup arriving after the unlock would be handed the same one.
func (i *InodeToPath) Forget(inode, nlookup uint64, onRelease func(inode uint64), onForgetDir func(dir util.FullPath)) {
	var dirPaths []util.FullPath
	callOnForgetDir := false

	i.Lock()
	path, found := i.inode2path[inode]
	if found {
		if nlookup > path.nlookup {
			glog.Errorf("kernel forget over-decrement: inode %d path %v current %d forget %d", inode, path.path, path.nlookup, nlookup)
			path.nlookup = 0
		} else {
			path.nlookup -= nlookup
		}
		glog.V(4).Infof("kernel forget: inode %d path %v nlookup %d", inode, path.path, path.nlookup)
		if path.nlookup == 0 {
			if onRelease != nil {
				onRelease(inode)
			}
			if _, isDir := i.dirStates[inode]; isDir && onForgetDir != nil {
				dirPaths = path.appendPaths(nil)
				callOnForgetDir = true
			}
			delete(i.path2inode, path.path)
			if path.extraPaths != nil {
				for _, p := range *path.extraPaths {
					delete(i.path2inode, p)
				}
			}
			delete(i.inode2path, inode)
			delete(i.dirStates, inode)
		} else {
			glog.V(4).Infof("kernel forget but nlookup not zero: inode %d path %v nlookup %d", inode, path.path, path.nlookup)
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
