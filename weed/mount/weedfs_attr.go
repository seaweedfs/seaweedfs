package mount

import (
	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/hanwen/go-fuse/v2/fuse"
	"os"
	"syscall"
	"time"
)

func (wfs *WFS) GetAttr(cancel <-chan struct{}, input *fuse.GetAttrIn, out *fuse.AttrOut) (code fuse.Status) {
	println("input node id", input.NodeId)
	if input.NodeId == 1 {
		wfs.setRootAttr(out)
		return fuse.OK
	}

	_, entry, status := wfs.maybeReadEntry(input.NodeId)
	if status != fuse.OK {
		return status
	}
	out.AttrValid = 1
	wfs.setAttrByPbEntry(&out.Attr, input.NodeId, entry)

	return fuse.OK
}

func (wfs *WFS) SetAttr(cancel <-chan struct{}, input *fuse.SetAttrIn, out *fuse.AttrOut) (code fuse.Status) {
	return fuse.ENOSYS
}
func (wfs *WFS) GetXAttr(cancel <-chan struct{}, header *fuse.InHeader, attr string, dest []byte) (size uint32, code fuse.Status) {
	return 0, fuse.ENOSYS
}

func (wfs *WFS) SetXAttr(cancel <-chan struct{}, input *fuse.SetXAttrIn, attr string, data []byte) fuse.Status {
	return fuse.ENOSYS
}

func (wfs *WFS) ListXAttr(cancel <-chan struct{}, header *fuse.InHeader, dest []byte) (n uint32, code fuse.Status) {
	return 0, fuse.ENOSYS
}

func (wfs *WFS) RemoveXAttr(cancel <-chan struct{}, header *fuse.InHeader, attr string) fuse.Status {
	return fuse.ENOSYS
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
	out.Mode = toSystemType(os.ModeDir) | uint32(wfs.option.MountMode)
	out.Nlink = 1
}

func (wfs *WFS) setAttrByPbEntry(out *fuse.Attr, inode uint64, entry *filer_pb.Entry) {
	out.Ino = inode
	out.Uid = entry.Attributes.Uid
	out.Gid = entry.Attributes.Gid
	out.Mode = toSystemMode(os.FileMode(entry.Attributes.FileMode))
	out.Mtime = uint64(entry.Attributes.Mtime)
	out.Ctime = uint64(entry.Attributes.Mtime)
	out.Atime = uint64(entry.Attributes.Mtime)
	if entry.HardLinkCounter > 0 {
		out.Nlink = uint32(entry.HardLinkCounter)
	} else {
		out.Nlink = 1
	}
	out.Size = filer.FileSize(entry)
	out.Blocks = (out.Size + blockSize - 1) / blockSize
	setBlksize(out, blockSize)
}

func (wfs *WFS) setAttrByFilerEntry(out *fuse.Attr, inode uint64, entry *filer.Entry) {
	out.Ino = inode
	out.Size = entry.FileSize
	out.Blocks = (out.Size + blockSize - 1) / blockSize
	setBlksize(out, blockSize)
	out.Atime = uint64(entry.Attr.Mtime.Unix())
	out.Mtime = uint64(entry.Attr.Mtime.Unix())
	out.Ctime = uint64(entry.Attr.Mtime.Unix())
	out.Crtime_ = uint64(entry.Attr.Crtime.Unix())
	out.Mode = toSystemMode(entry.Attr.Mode)
	if entry.HardLinkCounter > 0 {
		out.Nlink = uint32(entry.HardLinkCounter)
	} else {
		out.Nlink = 1
	}
	out.Uid = entry.Attr.Uid
	out.Gid = entry.Attr.Gid
}

func (wfs *WFS) outputEntry(out *fuse.EntryOut, inode uint64, entry *filer.Entry) {
	out.NodeId = inode
	out.Generation = 1
	out.EntryValid = 1
	out.AttrValid = 1
	wfs.setAttrByFilerEntry(&out.Attr, inode, entry)
}

func toSystemMode(mode os.FileMode) uint32 {
	return toSystemType(mode) | uint32(mode)
}

func toSystemType(mode os.FileMode) uint32 {
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
