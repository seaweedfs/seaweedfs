package mount

import (
	"context"
	"os"
	"syscall"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func (wfs *WFS) GetAttr(cancel <-chan struct{}, input *fuse.GetAttrIn, out *fuse.AttrOut) (code fuse.Status) {
	glog.V(4).Infof("GetAttr %v", input.NodeId)
	if input.NodeId == 1 {
		wfs.setRootAttr(out)
		if wfs.option.PosixDirNlink {
			wfs.applyDirNlink(&out.Attr, util.FullPath(wfs.option.FilerMountRootPath))
		}
		return fuse.OK
	}

	inode := input.NodeId
	path, fh, entry, status := wfs.maybeReadEntry(inode)
	if status != fuse.OK {
		return status
	}
	out.AttrValid = wfs.attrValidSec
	// An open handle's entry has two sets of writers: async upload workers
	// append chunks under the LockedEntry lock, while Write and the metadata
	// flush rewrite size, times and the whole chunk slice under the handle
	// lock. Hold both for reading, in that order, so FileSize never iterates a
	// slice mid-reallocation. Re-read the entry under them in case SetEntry
	// swapped the pointer since maybeReadEntry.
	if fh != nil {
		fhActiveLock := wfs.fhLockTable.AcquireLock("GetAttr", fh.fh, util.SharedLock)
		fh.entry.RLock()
		entry = fh.entry.Entry
		wfs.setAttrByPbEntry(&out.Attr, inode, entry, true)
		fh.entry.RUnlock()
		wfs.fhLockTable.ReleaseLock(fh.fh, fhActiveLock)
	} else {
		wfs.setAttrByPbEntry(&out.Attr, inode, entry, true)
	}
	wfs.applyInMemoryAtime(&out.Attr, inode)
	applyUnlinkedNlink(&out.Attr, path)
	if entry.GetIsDirectory() {
		wfs.applyInMemoryDirMtime(&out.Attr, inode)
		if wfs.option.PosixDirNlink {
			wfs.applyDirNlink(&out.Attr, path)
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
	if size, ok := input.GetSize(); ok && fh != nil && fh.dirtyPages.HasWrites() && size < filer.FileSize(entry) {
		// The truncation below trims chunks; dirty pages it cannot see. Left
		// alone, pages beyond the new size come back with the next flush and
		// grow the file again, so turn them into chunks first. Runs before
		// the entry locks below: the flush takes its own.
		ctx, cancelFunc := context.WithTimeout(context.Background(), metadataFlushTimeout)
		flushStatus := wfs.doFlush(ctx, fh, input.Uid, input.Gid, false)
		cancelFunc()
		if flushStatus != fuse.OK {
			return flushStatus
		}
	}
	if fh != nil {
		fh.entryLock.Lock()
		defer fh.entryLock.Unlock()
		// entry is the handle's shared LockedEntry.Entry. Async upload workers
		// mutate its Chunks slice under the LockedEntry lock (AddChunks); hold
		// that same lock so the truncate and FileSize reads below don't tear
		// against a concurrent append. Re-read under the lock in case SetEntry
		// swapped the pointer since maybeReadEntry, so we don't mutate an
		// orphaned entry and lose the update.
		fh.entry.Lock()
		defer fh.entry.Unlock()
		entry = fh.entry.Entry
	}

	wormEnforced, wormEnabled := wfs.wormEnforcedForEntry(path, entry)
	if wormEnforced {
		return fuse.EPERM
	}

	if size, ok := input.GetSize(); ok {
		glog.V(4).Infof("%v setattr set size=%v chunks=%d", path, size, len(entry.GetChunks()))
		// Invalidate the open-mtime cache so the next Open does not set
		// FOPEN_KEEP_CACHE with stale kernel page cache data.
		wfs.invalidateOpenMtimeCache(input.NodeId)
		oldFileSize := filer.FileSize(entry)
		if size < oldFileSize {
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
		if size > oldFileSize {
			// The writes that fill the range will not grow the file, so they
			// charge nothing; the growth is counted here or the quota never
			// sees it. Matches Write and Fallocate.
			wfs.AddUncommittedBytes(int64(size - oldFileSize))
		}

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

	out.AttrValid = wfs.attrValidSec
	size, includeSize := input.GetSize()
	if includeSize {
		out.Attr.Size = size
	}
	wfs.setAttrByPbEntry(&out.Attr, input.NodeId, entry, !includeSize)
	wfs.applyInMemoryAtime(&out.Attr, input.NodeId)
	applyUnlinkedNlink(&out.Attr, path)

	if fh != nil {
		fh.dirtyMetadata = true
		return fuse.OK
	}
	if path == "" {
		// removed while open: the remembered entry is all there is to update
		wfs.rememberRemovedDir(input.NodeId, entry)
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
	out.EntryValid = wfs.entryValidSec
	out.AttrValid = wfs.attrValidSec
	wfs.setAttrByPbEntry(&out.Attr, inode, entry, true)
}

func (wfs *WFS) outputFilerEntry(out *fuse.EntryOut, inode uint64, entry *filer.Entry) {
	out.NodeId = inode
	out.Generation = 1
	out.EntryValid = wfs.entryValidSec
	out.AttrValid = wfs.attrValidSec
	wfs.setAttrByFilerEntry(&out.Attr, inode, entry)
}

// touchDirMtimeCtimeBest updates a directory's mtime and ctime using the
// best strategy for the current mode:
//   - WritebackCache: local meta cache only (no filer RPC)
//   - Normal mode: filer UpdateEntry RPC for POSIX correctness
func (wfs *WFS) touchDirMtimeCtimeBest(dirPath util.FullPath) {
	if wfs.option.WritebackCache {
		wfs.touchDirMtimeCtimeLocal(dirPath)
	} else {
		wfs.touchDirMtimeCtime(dirPath)
	}
}

// touchDirMtimeCtime updates a directory's mtime and ctime on the filer.
// POSIX requires this when entries are created or removed in the directory.
func (wfs *WFS) touchDirMtimeCtime(dirPath util.FullPath) {
	dirEntry, _, code := wfs.maybeLoadEntry(dirPath)
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

// touchDirMtimeCtimeLocal updates a directory's mtime and ctime in an in-memory
// overlay, avoiding LevelDB reads and writes entirely. The overlay is applied
// by applyInMemoryDirMtime when GetAttr/Lookup reads the directory's attributes.
func (wfs *WFS) touchDirMtimeCtimeLocal(dirPath util.FullPath) {
	if inode, found := wfs.inodeToPath.GetInode(dirPath); found {
		wfs.setDirMtime(inode, time.Now())
	}
}

const dirMtimeMapMaxSize = 8192

func (wfs *WFS) setDirMtime(inode uint64, t time.Time) {
	wfs.dirMtimeMu.Lock()
	defer wfs.dirMtimeMu.Unlock()
	if len(wfs.dirMtimeMap) >= dirMtimeMapMaxSize {
		for k := range wfs.dirMtimeMap {
			delete(wfs.dirMtimeMap, k)
			break
		}
	}
	wfs.dirMtimeMap[inode] = t
}

// applyInMemoryDirMtime overlays the in-memory mtime/ctime onto fuse.Attr
// for directories that had recent child mutations.
func (wfs *WFS) applyInMemoryDirMtime(out *fuse.Attr, inode uint64) {
	wfs.dirMtimeMu.Lock()
	if t, ok := wfs.dirMtimeMap[inode]; ok {
		sec := uint64(t.Unix())
		nsec := uint32(t.Nanosecond())
		if sec > out.Mtime || (sec == out.Mtime && nsec > out.Mtimensec) {
			out.Mtime = sec
			out.Mtimensec = nsec
			out.Ctime = sec
			out.Ctimensec = nsec
		}
	}
	wfs.dirMtimeMu.Unlock()
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
// forgetInMemoryTimes drops the overlays for an inode. All these maps are keyed
// by inode and inodes are derived from the path, so a delete and recreate can
// hand the same number to a different file — which would then inherit the
// previous one's access or modification time, or a removed directory's entry.
func (wfs *WFS) forgetInMemoryTimes(inode uint64) {
	wfs.atimeMu.Lock()
	delete(wfs.atimeMap, inode)
	wfs.atimeMu.Unlock()

	wfs.dirMtimeMu.Lock()
	delete(wfs.dirMtimeMap, inode)
	wfs.dirMtimeMu.Unlock()

	wfs.removedDirMu.Lock()
	delete(wfs.removedDirs, inode)
	wfs.removedDirMu.Unlock()
}

// rememberRemovedDir keeps the last-known entry of a directory removed while
// the kernel still references its inode. A directory has no file handle to
// live on through, so this is what serves fstat/fchmod/f*xattr on a still-open
// descriptor until the final forget. Mutations publish through here as well,
// replacing the stored entry wholesale.
//
// The final forget's cleanup cannot miss an insert: Rmdir inserts inside
// RemovePath's callback, under the same inode table lock the forget releases
// under, and a publish runs inside a request whose open descriptor keeps the
// kernel from issuing that forget at all.
func (wfs *WFS) rememberRemovedDir(inode uint64, entry *filer_pb.Entry) {
	wfs.removedDirMu.Lock()
	if wfs.removedDirs == nil {
		wfs.removedDirs = make(map[uint64]*filer_pb.Entry)
	}
	wfs.removedDirs[inode] = entry
	wfs.removedDirMu.Unlock()
}

// removedDirEntry hands out a private copy: stored entries are never mutated
// in place, so concurrent readers cannot see a half-applied change.
func (wfs *WFS) removedDirEntry(inode uint64) *filer_pb.Entry {
	wfs.removedDirMu.Lock()
	entry := wfs.removedDirs[inode]
	wfs.removedDirMu.Unlock()
	if entry == nil {
		return nil
	}
	return proto.Clone(entry).(*filer_pb.Entry)
}

func (wfs *WFS) applyInMemoryAtime(out *fuse.Attr, inode uint64) {
	wfs.atimeMu.Lock()
	if t, ok := wfs.atimeMap[inode]; ok {
		out.Atime = uint64(t.Unix())
		out.Atimensec = uint32(t.Nanosecond())
	}
	wfs.atimeMu.Unlock()
}

// applyUnlinkedNlink zeroes nlink for an inode no name points at any more: the
// file was unlinked while open and lives on only through its handle. Every
// reply carrying attributes must say so, or the kernel caches a live nlink.
func applyUnlinkedNlink(out *fuse.Attr, path util.FullPath) {
	if path == "" {
		out.Nlink = 0
	}
}

// applyDirNlink sets nlink = 2 + number_of_subdirectories for a directory.
// Uses the in-memory subdirectory count tracked by mkdir/rmdir/rename.
func (wfs *WFS) applyDirNlink(out *fuse.Attr, dirPath util.FullPath) {
	count := wfs.inodeToPath.GetSubdirCount(dirPath)
	if count > 0 {
		out.Nlink = 2 + uint32(count)
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
