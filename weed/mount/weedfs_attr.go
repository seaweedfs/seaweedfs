package mount

import (
	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/hanwen/go-fuse/v2/fuse"
	sys "golang.org/x/sys/unix"
	"os"
	"runtime"
	"strings"
	"syscall"
	"time"
)

const (
	// https://man7.org/linux/man-pages/man7/xattr.7.html#:~:text=The%20VFS%20imposes%20limitations%20that,in%20listxattr(2)).
	MAX_XATTR_NAME_SIZE  = 255
	MAX_XATTR_VALUE_SIZE = 65536
	XATTR_PREFIX         = "xattr-" // same as filer
)

func (wfs *WFS) GetAttr(cancel <-chan struct{}, input *fuse.GetAttrIn, out *fuse.AttrOut) (code fuse.Status) {
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

	// TODO this is only for directory. Filet setAttr involves open files and truncate to a size

	path, entry, status := wfs.maybeReadEntry(input.NodeId)
	if status != fuse.OK {
		return status
	}

	if mode, ok := input.GetMode(); ok {
		entry.Attributes.FileMode = uint32(mode)
	}

	if uid, ok := input.GetUID(); ok {
		entry.Attributes.Uid = uid
	}

	if gid, ok := input.GetGID(); ok {
		entry.Attributes.Gid = gid
	}

	if mtime, ok := input.GetMTime(); ok {
		entry.Attributes.Mtime = mtime.Unix()
	}

	entry.Attributes.Mtime = time.Now().Unix()
	out.AttrValid = 1
	wfs.setAttrByPbEntry(&out.Attr, input.NodeId, entry)

	return wfs.saveEntry(path, entry)

}

// GetXAttr reads an extended attribute, and should return the
// number of bytes. If the buffer is too small, return ERANGE,
// with the required buffer size.
func (wfs *WFS) GetXAttr(cancel <-chan struct{}, header *fuse.InHeader, attr string, dest []byte) (size uint32, code fuse.Status) {

	//validate attr name
	if len(attr) > MAX_XATTR_NAME_SIZE {
		if runtime.GOOS == "darwin" {
			return 0, fuse.EPERM
		} else {
			return 0, fuse.ERANGE
		}
	}
	if len(attr) == 0 {
		return 0, fuse.EINVAL
	}

	_, entry, status := wfs.maybeReadEntry(header.NodeId)
	if status != fuse.OK {
		return 0, status
	}
	if entry == nil {
		return 0, fuse.ENOENT
	}
	if entry.Extended == nil {
		return 0, fuse.ENOATTR
	}
	data, found := entry.Extended[XATTR_PREFIX+attr]
	if !found {
		return 0, fuse.ENOATTR
	}
	if len(dest) < len(data) {
		return uint32(len(data)), fuse.ERANGE
	}
	copy(dest, data)

	return uint32(len(data)), fuse.OK
}

// SetXAttr writes an extended attribute.
// https://man7.org/linux/man-pages/man2/setxattr.2.html
//        By default (i.e., flags is zero), the extended attribute will be
//       created if it does not exist, or the value will be replaced if
//       the attribute already exists.  To modify these semantics, one of
//       the following values can be specified in flags:
//
//       XATTR_CREATE
//              Perform a pure create, which fails if the named attribute
//              exists already.
//
//       XATTR_REPLACE
//              Perform a pure replace operation, which fails if the named
//              attribute does not already exist.
func (wfs *WFS) SetXAttr(cancel <-chan struct{}, input *fuse.SetXAttrIn, attr string, data []byte) fuse.Status {
	//validate attr name
	if len(attr) > MAX_XATTR_NAME_SIZE {
		if runtime.GOOS == "darwin" {
			return fuse.EPERM
		} else {
			return fuse.ERANGE
		}
	}
	if len(attr) == 0 {
		return fuse.EINVAL
	}
	//validate attr value
	if len(data) > MAX_XATTR_VALUE_SIZE {
		if runtime.GOOS == "darwin" {
			return fuse.Status(syscall.E2BIG)
		} else {
			return fuse.ERANGE
		}
	}

	path, entry, status := wfs.maybeReadEntry(input.NodeId)
	if status != fuse.OK {
		return status
	}
	if entry.Extended == nil {
		entry.Extended = make(map[string][]byte)
	}
	oldData, _ := entry.Extended[XATTR_PREFIX+attr]
	switch input.Flags {
	case sys.XATTR_CREATE:
		if len(oldData) > 0 {
			break
		}
		fallthrough
	case sys.XATTR_REPLACE:
		fallthrough
	default:
		entry.Extended[XATTR_PREFIX+attr] = data
	}

	return wfs.saveEntry(path, entry)

}

// ListXAttr lists extended attributes as '\0' delimited byte
// slice, and return the number of bytes. If the buffer is too
// small, return ERANGE, with the required buffer size.
func (wfs *WFS) ListXAttr(cancel <-chan struct{}, header *fuse.InHeader, dest []byte) (n uint32, code fuse.Status) {
	_, entry, status := wfs.maybeReadEntry(header.NodeId)
	if status != fuse.OK {
		return 0, status
	}
	if entry == nil {
		return 0, fuse.ENOENT
	}
	if entry.Extended == nil {
		return 0, fuse.ENOATTR
	}

	var data []byte
	for k := range entry.Extended {
		if strings.HasPrefix(k, XATTR_PREFIX) {
			data = append(data, k[len(XATTR_PREFIX):]...)
			data = append(data, 0)
		}
	}
	if len(dest) < len(data) {
		return uint32(len(data)), fuse.ERANGE
	}

	copy(dest, data)

	return uint32(len(data)), fuse.OK
}

// RemoveXAttr removes an extended attribute.
func (wfs *WFS) RemoveXAttr(cancel <-chan struct{}, header *fuse.InHeader, attr string) fuse.Status {
	if len(attr) == 0 {
		return fuse.EINVAL
	}
	path, entry, status := wfs.maybeReadEntry(header.NodeId)
	if status != fuse.OK {
		return status
	}
	if entry.Extended == nil {
		return fuse.ENOATTR
	}
	_, found := entry.Extended[XATTR_PREFIX+attr]

	if !found {
		return fuse.ENOATTR
	}

	delete(entry.Extended, XATTR_PREFIX+attr)

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
	out.Mode = toSystemType(os.ModeDir) | uint32(wfs.option.MountMode)
	out.Nlink = 1
}

func (wfs *WFS) setAttrByPbEntry(out *fuse.Attr, inode uint64, entry *filer_pb.Entry) {
	out.Ino = inode
	out.Size = filer.FileSize(entry)
	out.Blocks = (out.Size + blockSize - 1) / blockSize
	setBlksize(out, blockSize)
	out.Mtime = uint64(entry.Attributes.Mtime)
	out.Ctime = uint64(entry.Attributes.Mtime)
	out.Atime = uint64(entry.Attributes.Mtime)
	out.Mode = toSystemMode(os.FileMode(entry.Attributes.FileMode))
	if entry.HardLinkCounter > 0 {
		out.Nlink = uint32(entry.HardLinkCounter)
	} else {
		out.Nlink = 1
	}
	out.Uid = entry.Attributes.Uid
	out.Gid = entry.Attributes.Gid
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
