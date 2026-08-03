package winfsp

import (
	"os"
	"strings"

	cgofuse "github.com/winfsp/cgofuse/fuse"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/mount"
)

const (
	rootInode = 1

	// WinFsp passes this when an operation carries no open handle.
	noHandle = ^uint64(0)

	// How many entries to pull from one readdir round before handing them to
	// WinFsp. Bounded so a directory with millions of children does not
	// materialise in one slice.
	readdirBatch = 4096
)

// never is a nil channel: receiving blocks forever, which is what the raw
// operations expect from a caller that cannot cancel.
var never chan struct{}

// WinFS presents a mount.WFS through the path-based interface WinFsp speaks.
type WinFS struct {
	cgofuse.FileSystemBase

	wfs *mount.WFS
	uid uint32
	gid uint32
}

func NewWinFS(wfs *mount.WFS, uid, gid uint32) *WinFS {
	return &WinFS{wfs: wfs, uid: uid, gid: gid}
}

// resolve walks a WinFsp path down to an inode, one Lookup per component.
// Lookup also registers the mapping the raw operations index by.
func (w *WinFS) resolve(path string) (uint64, fuse.Status) {
	trimmed := strings.Trim(strings.ReplaceAll(path, `\`, "/"), "/")
	if trimmed == "" {
		return rootInode, fuse.OK
	}
	inode := uint64(rootInode)
	for _, name := range strings.Split(trimmed, "/") {
		if name == "" || name == "." {
			continue
		}
		var out fuse.EntryOut
		if status := w.wfs.Lookup(never, &fuse.InHeader{NodeId: inode}, name, &out); status != fuse.OK {
			return 0, status
		}
		inode = out.NodeId
	}
	return inode, fuse.OK
}

// resolveParent resolves everything but the last component, which the create
// and delete operations need separately.
func (w *WinFS) resolveParent(path string) (uint64, string, fuse.Status) {
	trimmed := strings.Trim(strings.ReplaceAll(path, `\`, "/"), "/")
	if trimmed == "" {
		return 0, "", fuse.EINVAL
	}
	dir, name := "", trimmed
	if i := strings.LastIndex(trimmed, "/"); i >= 0 {
		dir, name = trimmed[:i], trimmed[i+1:]
	}
	parent, status := w.resolve(dir)
	if status != fuse.OK {
		return 0, "", status
	}
	return parent, name, fuse.OK
}

func (w *WinFS) attrToStat(attr *fuse.Attr, stat *cgofuse.Stat_t) {
	stat.Ino = attr.Ino
	stat.Mode = attr.Mode
	stat.Nlink = attr.Nlink
	stat.Size = int64(attr.Size)
	stat.Blocks = int64(attr.Blocks)
	stat.Blksize = int64(attr.Blksize)
	stat.Uid = w.uid
	stat.Gid = w.gid
	stat.Atim = cgofuse.Timespec{Sec: int64(attr.Atime), Nsec: int64(attr.Atimensec)}
	stat.Mtim = cgofuse.Timespec{Sec: int64(attr.Mtime), Nsec: int64(attr.Mtimensec)}
	stat.Ctim = cgofuse.Timespec{Sec: int64(attr.Ctime), Nsec: int64(attr.Ctimensec)}
	// Windows shows a creation time and has nothing to derive it from; ctime
	// is the closest the filer tracks.
	stat.Birthtim = stat.Ctim
}

func (w *WinFS) Statfs(path string, stat *cgofuse.Statfs_t) int {
	var out fuse.StatfsOut
	if status := w.wfs.StatFs(never, &fuse.InHeader{NodeId: rootInode}, &out); status != fuse.OK {
		return toErrno(status)
	}
	stat.Bsize = uint64(out.Bsize)
	stat.Frsize = uint64(out.Frsize)
	stat.Blocks = out.Blocks
	stat.Bfree = out.Bfree
	stat.Bavail = out.Bavail
	stat.Files = out.Files
	stat.Ffree = out.Ffree
	stat.Favail = out.Ffree
	stat.Namemax = uint64(out.NameLen)
	return 0
}

func (w *WinFS) Getattr(path string, stat *cgofuse.Stat_t, fh uint64) int {
	inode, status := w.resolve(path)
	if status != fuse.OK {
		return toErrno(status)
	}
	in := &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: inode}}
	if fh != noHandle {
		in.Fh_ = fh
		in.Flags_ = fuse.FUSE_GETATTR_FH
	}
	var out fuse.AttrOut
	if status := w.wfs.GetAttr(never, in, &out); status != fuse.OK {
		return toErrno(status)
	}
	w.attrToStat(&out.Attr, stat)
	return 0
}

func (w *WinFS) Mkdir(path string, mode uint32) int {
	parent, name, status := w.resolveParent(path)
	if status != fuse.OK {
		return toErrno(status)
	}
	in := &fuse.MkdirIn{InHeader: fuse.InHeader{NodeId: parent}, Mode: mode | uint32(os.ModeDir.Perm())}
	var out fuse.EntryOut
	return toErrno(w.wfs.Mkdir(never, in, name, &out))
}

func (w *WinFS) Rmdir(path string) int {
	parent, name, status := w.resolveParent(path)
	if status != fuse.OK {
		return toErrno(status)
	}
	return toErrno(w.wfs.Rmdir(never, &fuse.InHeader{NodeId: parent}, name))
}

func (w *WinFS) Unlink(path string) int {
	parent, name, status := w.resolveParent(path)
	if status != fuse.OK {
		return toErrno(status)
	}
	return toErrno(w.wfs.Unlink(never, &fuse.InHeader{NodeId: parent}, name))
}

func (w *WinFS) Rename(oldpath string, newpath string) int {
	oldParent, oldName, status := w.resolveParent(oldpath)
	if status != fuse.OK {
		return toErrno(status)
	}
	newParent, newName, status := w.resolveParent(newpath)
	if status != fuse.OK {
		return toErrno(status)
	}
	in := &fuse.RenameIn{InHeader: fuse.InHeader{NodeId: oldParent}, Newdir: newParent}
	return toErrno(w.wfs.Rename(never, in, oldName, newName))
}

func (w *WinFS) Create(path string, flags int, mode uint32) (int, uint64) {
	parent, name, status := w.resolveParent(path)
	if status != fuse.OK {
		return toErrno(status), noHandle
	}
	in := &fuse.CreateIn{InHeader: fuse.InHeader{NodeId: parent}, Flags: uint32(flags), Mode: mode}
	var out fuse.CreateOut
	if status := w.wfs.Create(never, in, name, &out); status != fuse.OK {
		return toErrno(status), noHandle
	}
	return 0, out.Fh
}

func (w *WinFS) Open(path string, flags int) (int, uint64) {
	inode, status := w.resolve(path)
	if status != fuse.OK {
		return toErrno(status), noHandle
	}
	in := &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: inode}, Flags: uint32(flags)}
	var out fuse.OpenOut
	if status := w.wfs.Open(never, in, &out); status != fuse.OK {
		return toErrno(status), noHandle
	}
	return 0, out.Fh
}

func (w *WinFS) Read(path string, buff []byte, ofst int64, fh uint64) int {
	inode, status := w.resolve(path)
	if status != fuse.OK {
		return toErrno(status)
	}
	in := &fuse.ReadIn{
		InHeader: fuse.InHeader{NodeId: inode},
		Fh:       fh,
		Offset:   uint64(ofst),
		Size:     uint32(len(buff)),
	}
	result, status := w.wfs.Read(never, in, buff)
	if status != fuse.OK {
		return toErrno(status)
	}
	data, status := result.Bytes(buff)
	if status != fuse.OK {
		return toErrno(status)
	}
	if len(data) > 0 && &data[0] != &buff[0] {
		copy(buff, data)
	}
	return len(data)
}

func (w *WinFS) Write(path string, buff []byte, ofst int64, fh uint64) int {
	inode, status := w.resolve(path)
	if status != fuse.OK {
		return toErrno(status)
	}
	in := &fuse.WriteIn{
		InHeader: fuse.InHeader{NodeId: inode},
		Fh:       fh,
		Offset:   uint64(ofst),
		Size:     uint32(len(buff)),
	}
	written, status := w.wfs.Write(never, in, buff)
	if status != fuse.OK {
		return toErrno(status)
	}
	return int(written)
}

func (w *WinFS) Truncate(path string, size int64, fh uint64) int {
	inode, status := w.resolve(path)
	if status != fuse.OK {
		return toErrno(status)
	}
	in := &fuse.SetAttrIn{}
	in.NodeId = inode
	in.Valid = fuse.FATTR_SIZE
	in.Size = uint64(size)
	if fh != noHandle {
		in.Valid |= fuse.FATTR_FH
		in.Fh = fh
	}
	var out fuse.AttrOut
	return toErrno(w.wfs.SetAttr(never, in, &out))
}

func (w *WinFS) Chmod(path string, mode uint32) int {
	inode, status := w.resolve(path)
	if status != fuse.OK {
		return toErrno(status)
	}
	in := &fuse.SetAttrIn{}
	in.NodeId = inode
	in.Valid = fuse.FATTR_MODE
	in.Mode = mode
	var out fuse.AttrOut
	return toErrno(w.wfs.SetAttr(never, in, &out))
}

func (w *WinFS) Utimens(path string, tmsp []cgofuse.Timespec) int {
	inode, status := w.resolve(path)
	if status != fuse.OK {
		return toErrno(status)
	}
	in := &fuse.SetAttrIn{}
	in.NodeId = inode
	if len(tmsp) < 2 {
		in.Valid = fuse.FATTR_ATIME_NOW | fuse.FATTR_MTIME_NOW
	} else {
		in.Valid = fuse.FATTR_ATIME | fuse.FATTR_MTIME
		in.Atime, in.Atimensec = uint64(tmsp[0].Sec), uint32(tmsp[0].Nsec)
		in.Mtime, in.Mtimensec = uint64(tmsp[1].Sec), uint32(tmsp[1].Nsec)
	}
	var out fuse.AttrOut
	return toErrno(w.wfs.SetAttr(never, in, &out))
}

func (w *WinFS) Flush(path string, fh uint64) int {
	inode, status := w.resolve(path)
	if status != fuse.OK {
		return toErrno(status)
	}
	in := &fuse.FlushIn{InHeader: fuse.InHeader{NodeId: inode}, Fh: fh}
	return toErrno(w.wfs.Flush(never, in))
}

func (w *WinFS) Fsync(path string, datasync bool, fh uint64) int {
	inode, status := w.resolve(path)
	if status != fuse.OK {
		return toErrno(status)
	}
	in := &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: inode}, Fh: fh}
	return toErrno(w.wfs.Fsync(never, in))
}

func (w *WinFS) Release(path string, fh uint64) int {
	inode, status := w.resolve(path)
	if status != fuse.OK {
		return toErrno(status)
	}
	w.wfs.Release(never, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: inode}, Fh: fh})
	return 0
}

func (w *WinFS) Opendir(path string) (int, uint64) {
	inode, status := w.resolve(path)
	if status != fuse.OK {
		return toErrno(status), noHandle
	}
	var out fuse.OpenOut
	if status := w.wfs.OpenDir(never, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: inode}}, &out); status != fuse.OK {
		return toErrno(status), noHandle
	}
	return 0, out.Fh
}

func (w *WinFS) Releasedir(path string, fh uint64) int {
	inode, status := w.resolve(path)
	if status != fuse.OK {
		return toErrno(status)
	}
	w.wfs.ReleaseDir(&fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: inode}, Fh: fh})
	return 0
}

// readdirSink collects one readdir round. The raw operation fills the returned
// EntryOut after AddEntryPlus returns, so nothing can be converted until the
// round is over.
type readdirSink struct {
	names   []string
	offsets []uint64
	attrs   []*fuse.EntryOut
	limit   int
}

func (s *readdirSink) AddEntry(entry fuse.DirEntry) bool {
	if len(s.names) >= s.limit {
		return false
	}
	s.names = append(s.names, entry.Name)
	s.offsets = append(s.offsets, entry.Off)
	s.attrs = append(s.attrs, nil)
	return true
}

func (s *readdirSink) AddEntryPlus(entry fuse.DirEntry) *fuse.EntryOut {
	if len(s.names) >= s.limit {
		return nil
	}
	out := &fuse.EntryOut{}
	s.names = append(s.names, entry.Name)
	s.offsets = append(s.offsets, entry.Off)
	s.attrs = append(s.attrs, out)
	return out
}

func (w *WinFS) Readdir(path string, fill func(name string, stat *cgofuse.Stat_t, ofst int64) bool, ofst int64, fh uint64) int {
	inode, status := w.resolve(path)
	if status != fuse.OK {
		return toErrno(status)
	}
	offset := uint64(ofst)
	for {
		sink := &readdirSink{limit: readdirBatch}
		in := &fuse.ReadIn{
			InHeader: fuse.InHeader{NodeId: inode},
			Fh:       fh,
			Offset:   offset,
			Size:     1 << 20,
		}
		if status := w.wfs.ReadDirectoryInto(in, sink, true); status != fuse.OK {
			return toErrno(status)
		}
		if len(sink.names) == 0 {
			return 0
		}
		for i, name := range sink.names {
			var stat cgofuse.Stat_t
			var statp *cgofuse.Stat_t
			if attr := sink.attrs[i]; attr != nil {
				w.attrToStat(&attr.Attr, &stat)
				statp = &stat
			}
			if !fill(name, statp, int64(sink.offsets[i])) {
				return 0
			}
		}
		next := sink.offsets[len(sink.offsets)-1]
		if next <= offset {
			return 0
		}
		offset = next
	}
}

func (w *WinFS) Readlink(path string) (int, string) {
	inode, status := w.resolve(path)
	if status != fuse.OK {
		return toErrno(status), ""
	}
	target, status := w.wfs.Readlink(never, &fuse.InHeader{NodeId: inode})
	if status != fuse.OK {
		return toErrno(status), ""
	}
	return 0, string(target)
}

func (w *WinFS) Symlink(target string, newpath string) int {
	parent, name, status := w.resolveParent(newpath)
	if status != fuse.OK {
		return toErrno(status)
	}
	var out fuse.EntryOut
	return toErrno(w.wfs.Symlink(never, &fuse.InHeader{NodeId: parent}, target, name, &out))
}

// Link is not implemented: WinFsp has no hard links.
func (w *WinFS) Link(oldpath string, newpath string) int {
	return -eNOSYS
}
