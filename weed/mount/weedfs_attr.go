package mount

import (
	"context"
	"os"
	"syscall"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func (wfs *WFS) GetAttr(cancel <-chan struct{}, input *fuse.GetAttrIn, out *fuse.AttrOut) (code fuse.Status) {
	glog.V(4).Infof("GetAttr %v", input.NodeId)
	if input.NodeId == 1 {
		wfs.setRootAttr(out)
		wfs.applyDirNlink(&out.Attr, util.FullPath(wfs.option.FilerMountRootPath))
		return fuse.OK
	}

	inode := input.NodeId
	path, _, entry, status := wfs.maybeReadEntry(inode)
	if status == fuse.OK {
		out.AttrValid = 1
		wfs.setAttrByPbEntry(&out.Attr, inode, entry, true)
		wfs.applyInMemoryAtime(&out.Attr, inode)
		if entry.IsDirectory {
			wfs.applyDirNlink(&out.Attr, path)
		}
		return status
	} else {
		if fh, found := wfs.fhMap.FindFileHandle(inode); found {
			out.AttrValid = 1
			// Use shared lock to prevent race with Write operations
			fhActiveLock := wfs.fhLockTable.AcquireLock("GetAttr", fh.fh, util.SharedLock)
			wfs.setAttrByPbEntry(&out.Attr, inode, fh.entry.GetEntry(), true)
			wfs.fhLockTable.ReleaseLock(fh.fh, fhActiveLock)
			wfs.applyInMemoryAtime(&out.Attr, inode)
			out.Nlink = 0
			return fuse.OK
		}
	}

	return status
}

func (wfs *WFS) SetAttr(cancel <-chan struct{}, input *fuse.SetAttrIn, out *fuse.AttrOut) (code fuse.Status) {

	// Check quota including uncommitted writes for real-time enforcement
	if wfs.IsOverQuotaWithUncommitted() {
		return fuse.Status(syscall.ENOSPC)
	}

	path, fh, entry, status := wfs.maybeReadEntry(input.NodeId)
	if status != fuse.OK || entry == nil {
		return status
	}
	if fh != nil {
		fh.entryLock.Lock()
		defer fh.entryLock.Unlock()
	}

	wormEnforced, wormEnabled := wfs.wormEnforcedForEntry(path, entry)
	if wormEnforced {
		return fuse.EPERM
	}

	if size, ok := input.GetSize(); ok {
		glog.V(4).Infof("%v setattr set size=%v chunks=%d", path, size, len(entry.GetChunks()))
		if size < filer.FileSize(entry) {
			// fmt.Printf("truncate %v \n", fullPath)
			var chunks []*filer_pb.FileChunk
			var truncatedChunks []*filer_pb.FileChunk
			for _, chunk := range entry.GetChunks() {
				int64Size := int64(chunk.Size)
				if chunk.Offset+int64Size > int64(size) {
					// this chunk is truncated
					int64Size = int64(size) - chunk.Offset
					if int64Size > 0 {
						chunks = append(chunks, chunk)
						glog.V(4).Infof("truncated chunk %+v from %d to %d\n", chunk.GetFileIdString(), chunk.Size, int64Size)
						chunk.Size = uint64(int64Size)
					} else {
						glog.V(4).Infof("truncated whole chunk %+v\n", chunk.GetFileIdString())
						truncatedChunks = append(truncatedChunks, chunk)
					}
				} else {
					chunks = append(chunks, chunk)
				}
			}
			// set the new chunks and reset entry cache
			entry.Chunks = chunks
			if fh != nil {
				fh.entryChunkGroup.SetChunks(chunks)
			}
		}
		truncNow := time.Now()
		entry.Attributes.Mtime = truncNow.Unix()
		entry.Attributes.MtimeNs = int32(truncNow.Nanosecond())
		entry.Attributes.FileSize = size

	}

	if mode, ok := input.GetMode(); ok {
		// commit the file to worm when it is set to readonly at the first time
		if entry.WormEnforcedAtTsNs == 0 && wormEnabled && !hasWritePermission(mode) {
			entry.WormEnforcedAtTsNs = time.Now().UnixNano()
		}

		// glog.V(4).Infof("setAttr mode %o", mode)
		entry.Attributes.FileMode = chmod(entry.Attributes.FileMode, mode)
		if input.NodeId == 1 {
			wfs.option.MountMode = os.FileMode(chmod(uint32(wfs.option.MountMode), mode))
		}
	}

	ownerChanged := false
	if uid, ok := input.GetUID(); ok {
		entry.Attributes.Uid = uid
		ownerChanged = true
		if input.NodeId == 1 {
			wfs.option.MountUid = uid
		}
	}

	if gid, ok := input.GetGID(); ok {
		entry.Attributes.Gid = gid
		ownerChanged = true
		if input.NodeId == 1 {
			wfs.option.MountGid = gid
		}
	}

	// POSIX: clear SUID/SGID bits when ownership changes (unless caller is root).
	if ownerChanged && input.Uid != 0 {
		entry.Attributes.FileMode &^= 0o6000
	}

	if atime, ok := input.GetATime(); ok {
		wfs.setAtime(input.NodeId, atime)
	}

	if mtime, ok := input.GetMTime(); ok {
		entry.Attributes.Mtime = mtime.Unix()
		entry.Attributes.MtimeNs = int32(mtime.Nanosecond())
	}

	// POSIX: update ctime on any metadata change.
	now := time.Now()
	entry.Attributes.Ctime = now.Unix()
	entry.Attributes.CtimeNs = int32(now.Nanosecond())

	out.AttrValid = 1
	size, includeSize := input.GetSize()
	if includeSize {
		out.Attr.Size = size
	}
	wfs.setAttrByPbEntry(&out.Attr, input.NodeId, entry, !includeSize)
	wfs.applyInMemoryAtime(&out.Attr, input.NodeId)

	if fh != nil {
		fh.dirtyMetadata = true
		return fuse.OK
	}

	return wfs.saveEntry(path, entry)

}

func (wfs *WFS) setRootAttr(out *fuse.AttrOut) {
	now := uint64(time.Now().Unix())
	out.AttrValid = 119
	out.Ino = 1
	setBlksize(&out.Attr, blockSize)
	out.Uid = wfs.option.MountUid
	out.Gid = wfs.option.MountGid
	out.Mtime = now
	out.Ctime = now
	out.Atime = now
	out.Mode = toSyscallType(os.ModeDir) | uint32(wfs.option.MountMode)
	out.Nlink = 2
}

func (wfs *WFS) setAttrByPbEntry(out *fuse.Attr, inode uint64, entry *filer_pb.Entry, calculateSize bool) {
	out.Ino = inode
	setBlksize(out, blockSize)
	if entry == nil {
		return
	}
	if entry.Attributes != nil && entry.Attributes.Inode != 0 {
		out.Ino = entry.Attributes.Inode
	}
	if calculateSize {
		out.Size = filer.FileSize(entry)
	}
	if entry.FileMode()&os.ModeSymlink != 0 {
		out.Size = uint64(len(entry.Attributes.SymlinkTarget))
	}
	out.Blocks = (out.Size + blockSize - 1) / blockSize
	out.Mtime = uint64(entry.Attributes.Mtime)
	out.Mtimensec = uint32(entry.Attributes.MtimeNs)
	if entry.Attributes.Ctime != 0 {
		out.Ctime = uint64(entry.Attributes.Ctime)
		out.Ctimensec = uint32(entry.Attributes.CtimeNs)
	} else {
		out.Ctime = uint64(entry.Attributes.Mtime)
		out.Ctimensec = uint32(entry.Attributes.MtimeNs)
	}
	out.Atime = uint64(entry.Attributes.Mtime)
	out.Atimensec = uint32(entry.Attributes.MtimeNs)
	// In-memory atime overlay is applied by the caller via applyInMemoryAtime.
	out.Mode = toSyscallMode(os.FileMode(entry.Attributes.FileMode))
	if entry.IsDirectory {
		out.Nlink = 2
	} else if entry.HardLinkCounter > 0 {
		out.Nlink = uint32(entry.HardLinkCounter)
	} else {
		out.Nlink = 1
	}
	out.Uid = entry.Attributes.Uid
	out.Gid = entry.Attributes.Gid
	out.Rdev = entry.Attributes.Rdev
}

func (wfs *WFS) setAttrByFilerEntry(out *fuse.Attr, inode uint64, entry *filer.Entry) {
	out.Ino = inode
	out.Size = entry.FileSize
	if entry.Mode&os.ModeSymlink != 0 {
		out.Size = uint64(len(entry.SymlinkTarget))
	}
	out.Blocks = (out.Size + blockSize - 1) / blockSize
	setBlksize(out, blockSize)
	out.Atime = uint64(entry.Attr.Mtime.Unix())
	out.Atimensec = uint32(entry.Attr.Mtime.Nanosecond())
	out.Mtime = uint64(entry.Attr.Mtime.Unix())
	out.Mtimensec = uint32(entry.Attr.Mtime.Nanosecond())
	if !entry.Attr.Ctime.IsZero() {
		out.Ctime = uint64(entry.Attr.Ctime.Unix())
		out.Ctimensec = uint32(entry.Attr.Ctime.Nanosecond())
	} else {
		out.Ctime = uint64(entry.Attr.Mtime.Unix())
		out.Ctimensec = uint32(entry.Attr.Mtime.Nanosecond())
	}
	out.Mode = toSyscallMode(entry.Attr.Mode)
	if entry.IsDirectory() {
		out.Nlink = 2
	} else if entry.HardLinkCounter > 0 {
		out.Nlink = uint32(entry.HardLinkCounter)
	} else {
		out.Nlink = 1
	}
	out.Uid = entry.Attr.Uid
	out.Gid = entry.Attr.Gid
	out.Rdev = entry.Attr.Rdev
}

func (wfs *WFS) outputPbEntry(out *fuse.EntryOut, inode uint64, entry *filer_pb.Entry) {
	out.NodeId = inode
	out.Generation = 1
	out.EntryValid = 1
	out.AttrValid = 1
	wfs.setAttrByPbEntry(&out.Attr, inode, entry, true)
}

func (wfs *WFS) outputFilerEntry(out *fuse.EntryOut, inode uint64, entry *filer.Entry) {
	out.NodeId = inode
	out.Generation = 1
	out.EntryValid = 1
	out.AttrValid = 1
	wfs.setAttrByFilerEntry(&out.Attr, inode, entry)
}

// touchDirMtimeCtime updates a directory's mtime and ctime on the filer.
// POSIX requires this when entries are created or removed in the directory.
func (wfs *WFS) touchDirMtimeCtime(dirPath util.FullPath) {
	dirEntry, code := wfs.maybeLoadEntry(dirPath)
	if code != fuse.OK || dirEntry == nil || dirEntry.Attributes == nil {
		return
	}
	now := time.Now()
	dirEntry.Attributes.Mtime = now.Unix()
	dirEntry.Attributes.MtimeNs = int32(now.Nanosecond())
	dirEntry.Attributes.Ctime = now.Unix()
	dirEntry.Attributes.CtimeNs = int32(now.Nanosecond())
	wfs.saveEntry(dirPath, dirEntry)
}

// touchDirMtimeCtimeLocal updates a directory's mtime and ctime directly
// in the local metadata cache, without a filer RPC. This is used for
// deferred file creates where a filer round-trip would invalidate the
// just-cached child entry.
func (wfs *WFS) touchDirMtimeCtimeLocal(dirPath util.FullPath) {
	now := time.Now()
	if err := wfs.metaCache.TouchDirMtimeCtime(context.Background(), dirPath, now); err != nil {
		glog.V(3).Infof("touchDirMtimeCtimeLocal %s: %v", dirPath, err)
	}
}

const atimeMapMaxSize = 8192

// setAtime stores an in-memory atime for an inode. The map is bounded;
// when full, a random entry is evicted.
func (wfs *WFS) setAtime(inode uint64, t time.Time) {
	wfs.atimeMu.Lock()
	defer wfs.atimeMu.Unlock()
	if len(wfs.atimeMap) >= atimeMapMaxSize {
		// evict one random entry
		for k := range wfs.atimeMap {
			delete(wfs.atimeMap, k)
			break
		}
	}
	wfs.atimeMap[inode] = t
}

// applyInMemoryAtime overlays the in-memory atime onto a fuse.Attr if present.
func (wfs *WFS) applyInMemoryAtime(out *fuse.Attr, inode uint64) {
	wfs.atimeMu.Lock()
	if t, ok := wfs.atimeMap[inode]; ok {
		out.Atime = uint64(t.Unix())
		out.Atimensec = uint32(t.Nanosecond())
	}
	wfs.atimeMu.Unlock()
}

// applyDirNlink sets nlink = 2 + number_of_subdirectories for a directory.
// Only counts from the local metacache to avoid expensive filer queries.
// When the cache has no entries (e.g. before readdir), keeps nlink=2.
func (wfs *WFS) applyDirNlink(out *fuse.Attr, dirPath util.FullPath) {
	var subdirCount uint32
	wfs.metaCache.ListDirectoryEntries(context.Background(), dirPath, "", false, 100000, func(entry *filer.Entry) (bool, error) {
		if entry.IsDirectory() {
			subdirCount++
		}
		return true, nil
	})
	if subdirCount > 0 {
		out.Nlink = 2 + subdirCount
	}
}

func chmod(existing uint32, mode uint32) uint32 {
	return existing&^07777 | mode&07777
}

const ownerWrite = 0o200
const groupWrite = 0o020
const otherWrite = 0o002

func hasWritePermission(mode uint32) bool {
	return (mode&ownerWrite != 0) || (mode&groupWrite != 0) || (mode&otherWrite != 0)
}

func toSyscallMode(mode os.FileMode) uint32 {
	return toSyscallType(mode) | uint32(mode)
}

func toSyscallType(mode os.FileMode) uint32 {
	switch mode & os.ModeType {
	case os.ModeDir:
		return syscall.S_IFDIR
	case os.ModeSymlink:
		return syscall.S_IFLNK
	case os.ModeNamedPipe:
		return syscall.S_IFIFO
	case os.ModeSocket:
		return syscall.S_IFSOCK
	case os.ModeDevice:
		return syscall.S_IFBLK
	case os.ModeCharDevice:
		return syscall.S_IFCHR
	default:
		return syscall.S_IFREG
	}
}

func toOsFileType(mode uint32) os.FileMode {
	switch mode & (syscall.S_IFMT & 0xffff) {
	case syscall.S_IFDIR:
		return os.ModeDir
	case syscall.S_IFLNK:
		return os.ModeSymlink
	case syscall.S_IFIFO:
		return os.ModeNamedPipe
	case syscall.S_IFSOCK:
		return os.ModeSocket
	case syscall.S_IFBLK:
		return os.ModeDevice
	case syscall.S_IFCHR:
		return os.ModeCharDevice
	default:
		return 0
	}
}

func toOsFileMode(mode uint32) os.FileMode {
	return toOsFileType(mode) | os.FileMode(mode&07777)
}
