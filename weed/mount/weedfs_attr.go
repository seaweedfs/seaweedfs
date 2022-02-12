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

	entry, status := wfs.maybeReadEntry(input.NodeId)
	if status != fuse.OK {
		return status
	}
	wfs.setOutAttr(out, input.NodeId, entry)

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
	now := uint64(time.Now().Second())
	out.AttrValid = 119
	out.Ino = 1
	setBlksize(&out.Attr, blockSize)
	out.Uid = wfs.option.MountUid
	out.Gid = wfs.option.MountGid
	out.Mtime = now
	out.Ctime = now
	out.Atime = now
	out.Mode = osToSystemMode(os.ModeDir) | uint32(wfs.option.MountMode)
	out.Nlink = 1
}

func (wfs *WFS) setOutAttr(out *fuse.AttrOut, inode uint64, entry *filer_pb.Entry) {
	out.AttrValid = 1
	out.Ino = inode
	out.Uid = entry.Attributes.Uid
	out.Gid = entry.Attributes.Gid
	out.Mode = modeToSystemMode(entry.Attributes.FileMode)
	out.Mtime = uint64(entry.Attributes.Mtime)
	out.Ctime = uint64(entry.Attributes.Mtime)
	out.Atime = uint64(entry.Attributes.Mtime)
	if entry.HardLinkCounter > 0 {
		out.Nlink = uint32(entry.HardLinkCounter)
	}
	out.Size = filer.FileSize(entry)
	out.Blocks = out.Size/blockSize + 1
	setBlksize(&out.Attr, blockSize)
	out.Nlink = 1
}

func modeToSystemMode(mode uint32) uint32 {
	return osToSystemMode(os.FileMode(mode)) | mode
}

func osToSystemMode(mode os.FileMode) uint32 {
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
