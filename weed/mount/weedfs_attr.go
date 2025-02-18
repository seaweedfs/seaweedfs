package mount

import (
	"os"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

func (wfs *WFS) GetAttr(cancel <-chan struct{}, input *fuse.GetAttrIn, out *fuse.AttrOut) (code fuse.Status) {
	glog.V(4).Infof("GetAttr %v", input.NodeId)
	if input.NodeId == 1 {
		wfs.setRootAttr(out)
		return fuse.OK
	}

	inode := input.NodeId
	_, _, entry, status := wfs.maybeReadEntry(inode)
	if status == fuse.OK {
		out.AttrValid = 1
		wfs.setAttrByPbEntry(&out.Attr, inode, entry, true)
		return status
	} else {
		if fh, found := wfs.fhMap.FindFileHandle(inode); found {
			out.AttrValid = 1
			wfs.setAttrByPbEntry(&out.Attr, inode, fh.entry.GetEntry(), true)
			out.Nlink = 0
			return fuse.OK
		}
	}

	return status
}

func (wfs *WFS) SetAttr(cancel <-chan struct{}, input *fuse.SetAttrIn, out *fuse.AttrOut) (code fuse.Status) {

	if wfs.IsOverQuota {
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
		entry.Attributes.Mtime = time.Now().Unix()
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

	if uid, ok := input.GetUID(); ok {
		entry.Attributes.Uid = uid
		if input.NodeId == 1 {
			wfs.option.MountUid = uid
		}
	}

	if gid, ok := input.GetGID(); ok {
		entry.Attributes.Gid = gid
		if input.NodeId == 1 {
			wfs.option.MountGid = gid
		}
	}

	if atime, ok := input.GetATime(); ok {
		entry.Attributes.Mtime = atime.Unix()
	}

	if mtime, ok := input.GetMTime(); ok {
		entry.Attributes.Mtime = mtime.Unix()
	}

	out.AttrValid = 1
	size, includeSize := input.GetSize()
	if includeSize {
		out.Attr.Size = size
	}
	wfs.setAttrByPbEntry(&out.Attr, input.NodeId, entry, !includeSize)

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
	out.Nlink = 1
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
	out.Ctime = uint64(entry.Attributes.Mtime)
	out.Atime = uint64(entry.Attributes.Mtime)
	out.Mode = toSyscallMode(os.FileMode(entry.Attributes.FileMode))
	if entry.HardLinkCounter > 0 {
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
	out.Mtime = uint64(entry.Attr.Mtime.Unix())
	out.Ctime = uint64(entry.Attr.Mtime.Unix())
	out.Mode = toSyscallMode(entry.Attr.Mode)
	if entry.HardLinkCounter > 0 {
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
