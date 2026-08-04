package winfsp

import (
	"sync"
	"syscall"

	cgofuse "github.com/winfsp/cgofuse/fuse"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mount"
)

const (
	rootInode = 1

	// WinFsp passes this when an operation carries no open handle.
	noHandle = ^uint64(0)

	// utimeOmit is the nanosecond marker asking for a timestamp to be left as
	// it is; cgofuse hands it through rather than resolving it.
	utimeOmit = (1 << 30) - 2

	// windowsEpochCutoff is 1601-01-02 in unix seconds. Anything at or below
	// it is Windows' own epoch leaking through rather than a real time.
	windowsEpochCutoff = -11644387200

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

	wfs      *mount.WFS
	uid      uint32
	gid      uint32
	readOnly bool

	// An open handle holds the lookup reference for its inode until Release,
	// the way the kernel keeps one for an open file. WinFsp hands back only
	// the handle, and the raw filesystem reuses one handle for repeated opens
	// of the same inode, so the references are counted.
	mu         sync.Mutex
	fileInodes map[uint64]*handleRef
	dirInodes  map[uint64]*handleRef
}

func NewWinFS(wfs *mount.WFS, uid, gid uint32, readOnly bool) *WinFS {
	return &WinFS{
		wfs:        wfs,
		uid:        uid,
		gid:        gid,
		readOnly:   readOnly,
		fileInodes: make(map[uint64]*handleRef),
		dirInodes:  make(map[uint64]*handleRef),
	}
}

// handleRef is the lookup references an open handle is holding, one per open
// that has not yet been released.
type handleRef struct {
	inode uint64
	count int
}

// retain parks one reference under a handle.
func (w *WinFS) retain(table map[uint64]*handleRef, handle, inode uint64) {
	if handle == noHandle {
		return
	}
	var stranded uint64
	w.mu.Lock()
	switch existing, found := table[handle]; {
	case found && existing.inode == inode:
		existing.count++
	case found:
		// The handle was reissued for a different inode; its old reference
		// would otherwise never be returned.
		stranded = existing.inode
		table[handle] = &handleRef{inode: inode, count: 1}
	default:
		table[handle] = &handleRef{inode: inode, count: 1}
	}
	w.mu.Unlock()
	w.forget(stranded)
}

// releaseRetained hands back one reference, reporting the inode only once the
// last open of that handle is gone, so a repeated release cannot forget twice.
func (w *WinFS) releaseRetained(table map[uint64]*handleRef, handle uint64) uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	existing, found := table[handle]
	if !found {
		return 0
	}
	existing.count--
	if existing.count > 0 {
		return 0
	}
	delete(table, handle)
	return existing.inode
}

// caller identifies who the raw filesystem should record as the owner of
// anything it creates. Windows has no uid to pass through, so entries carry
// the identity the mount was started with rather than root.
// denied reports whether a modification should be refused outright, which is
// how a read-only mount is enforced: WinFsp discards its own "ro" option.
func (w *WinFS) denied() bool { return w.readOnly }

// inodeForHandle reports the inode an open handle is holding, or 0. WinFsp
// keeps the path it opened with and never updates it across a rename, so a
// handle is the more reliable of the two.
func (w *WinFS) inodeForHandle(table map[uint64]*handleRef, handle uint64) uint64 {
	if handle == noHandle {
		return 0
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if existing, found := table[handle]; found {
		return existing.inode
	}
	return 0
}

// ptr is for the operations that take the header by pointer.
func ptr(h fuse.InHeader) *fuse.InHeader { return &h }

func (w *WinFS) caller(inode uint64) fuse.InHeader {
	return fuse.InHeader{
		NodeId: inode,
		Caller: fuse.Caller{Owner: fuse.Owner{Uid: w.uid, Gid: w.gid}},
	}
}

// lookupRef collects the references a resolution took. Every operation that
// hands back an EntryOut grants the caller one, which the Linux kernel returns
// with FORGET. WinFsp has no FORGET, so anything the adapter looks up it has
// to release itself or the mount's inode table grows for the life of the
// process.
type lookupRef struct {
	w      *WinFS
	inodes []uint64
}

// release returns every reference still held.
func (r *lookupRef) release() {
	if r == nil {
		return
	}
	for _, inode := range r.inodes {
		r.w.forget(inode)
	}
	r.inodes = nil
}

// keepLast hands the reference on the final component to the caller, which
// becomes responsible for releasing it. An open handle takes it this way and
// gives it back on Release.
func (r *lookupRef) keepLast() {
	if r != nil && len(r.inodes) > 0 {
		r.inodes = r.inodes[:len(r.inodes)-1]
	}
}

// forget returns one reference. The root is never looked up, so it never holds
// one to give back.
func (w *WinFS) forget(inode uint64) {
	if inode == rootInode || inode == 0 {
		return
	}
	w.wfs.Forget(inode, 1)
}

// walk resolves a path one component at a time. Lookup does more than find an
// inode: it refreshes what the mount knows about the entry, so the result of a
// preceding truncate or write is visible to the caller.
func (w *WinFS) walk(parts []string) (uint64, *lookupRef, fuse.Status) {
	ref := &lookupRef{w: w}
	inode := uint64(rootInode)
	for _, name := range parts {
		var out fuse.EntryOut
		if status := w.wfs.Lookup(never, ptr(w.caller(inode)), name, &out); status != fuse.OK {
			ref.release()
			return 0, nil, status
		}
		inode = out.NodeId
		ref.inodes = append(ref.inodes, inode)
	}
	return inode, ref, fuse.OK
}

// resolve walks a WinFsp path down to an inode. The caller must release the
// returned reference.
func (w *WinFS) resolve(path string) (uint64, *lookupRef, fuse.Status) {
	return w.walk(splitPath(path))
}

// resolveParent resolves everything but the last component, which the create
// and delete operations need separately. The caller must release the reference.
func (w *WinFS) resolveParent(path string) (uint64, string, *lookupRef, fuse.Status) {
	parentParts, name, ok := splitParent(path)
	if !ok {
		return 0, "", nil, fuse.EINVAL
	}
	parent, ref, status := w.walk(parentParts)
	if status != fuse.OK {
		return 0, "", nil, status
	}
	return parent, name, ref, fuse.OK
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

// translateOpenFlags converts cgofuse's open flags, which follow MSVC's
// numbering, into the values the raw filesystem tests against. Only the access
// mode and O_TRUNC happen to agree; O_EXCL would otherwise read as O_APPEND.
func translateOpenFlags(flags int) uint32 {
	out := uint32(flags & cgofuse.O_ACCMODE)
	for _, pair := range []struct {
		from int
		to   int
	}{
		{cgofuse.O_APPEND, syscall.O_APPEND},
		{cgofuse.O_CREAT, syscall.O_CREAT},
		{cgofuse.O_TRUNC, syscall.O_TRUNC},
		{cgofuse.O_EXCL, syscall.O_EXCL},
	} {
		if flags&pair.from != 0 {
			out |= uint32(pair.to)
		}
	}
	return out
}

// logResolveFailure keeps a missing file quiet. Windows probes for entries
// that do not exist as a matter of course, so ENOENT is an answer rather than
// a fault; anything else is worth a line.
func logResolveFailure(op, path string, status fuse.Status) {
	if status == fuse.ENOENT {
		glog.V(4).Infof("%s %s: no such entry", op, path)
		return
	}
	glog.Errorf("%s %s: resolving: %v", op, path, status)
}

func (w *WinFS) Statfs(path string, stat *cgofuse.Statfs_t) int {
	var out fuse.StatfsOut
	if status := w.wfs.StatFs(never, ptr(w.caller(rootInode)), &out); status != fuse.OK {
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
	inode := w.inodeForHandle(w.fileInodes, fh)
	if inode == 0 {
		var ref *lookupRef
		var status fuse.Status
		inode, ref, status = w.resolve(path)
		if status != fuse.OK {
			logResolveFailure("getattr", path, status)
			return toErrno(status)
		}
		defer ref.release()
	}
	in := &fuse.GetAttrIn{InHeader: w.caller(inode)}
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
	if w.denied() {
		return -eROFS
	}
	parent, name, ref, status := w.resolveParent(path)
	if status != fuse.OK {
		return toErrno(status)
	}
	defer ref.release()
	in := &fuse.MkdirIn{InHeader: w.caller(parent), Mode: mode}
	var out fuse.EntryOut
	status = w.wfs.Mkdir(never, in, name, &out)
	if status == fuse.OK {
		// Mkdir hands back an EntryOut, and nothing on this side will forget it.
		w.forget(out.NodeId)
	}
	return toErrno(status)
}

func (w *WinFS) Rmdir(path string) int {
	if w.denied() {
		return -eROFS
	}
	parent, name, ref, status := w.resolveParent(path)
	if status != fuse.OK {
		return toErrno(status)
	}
	defer ref.release()
	return toErrno(w.wfs.Rmdir(never, ptr(w.caller(parent)), name))
}

func (w *WinFS) Unlink(path string) int {
	if w.denied() {
		return -eROFS
	}
	parent, name, ref, status := w.resolveParent(path)
	if status != fuse.OK {
		return toErrno(status)
	}
	defer ref.release()
	return toErrno(w.wfs.Unlink(never, ptr(w.caller(parent)), name))
}

func (w *WinFS) Rename(oldpath string, newpath string) int {
	if w.denied() {
		return -eROFS
	}
	oldParent, oldName, oldRef, status := w.resolveParent(oldpath)
	if status != fuse.OK {
		return toErrno(status)
	}
	defer oldRef.release()
	newParent, newName, newRef, status := w.resolveParent(newpath)
	if status != fuse.OK {
		return toErrno(status)
	}
	defer newRef.release()
	in := &fuse.RenameIn{InHeader: w.caller(oldParent), Newdir: newParent}
	return toErrno(w.wfs.Rename(never, in, oldName, newName))
}

func (w *WinFS) Create(path string, flags int, mode uint32) (int, uint64) {
	if w.denied() {
		return -eROFS, noHandle
	}
	parent, name, ref, status := w.resolveParent(path)
	if status != fuse.OK {
		glog.Errorf("create %s: resolving the parent directory: %v", path, status)
		return toErrno(status), noHandle
	}
	defer ref.release()
	in := &fuse.CreateIn{InHeader: w.caller(parent), Flags: translateOpenFlags(flags), Mode: mode}
	var out fuse.CreateOut
	if status := w.wfs.Create(never, in, name, &out); status != fuse.OK {
		glog.Errorf("create %s in inode %d: %v", name, parent, status)
		return toErrno(status), noHandle
	}
	// Create grants a reference on the new inode; hold it for as long as the
	// handle lives and give it back in Release.
	w.retain(w.fileInodes, out.Fh, out.NodeId)
	return 0, out.Fh
}

func (w *WinFS) Open(path string, flags int) (int, uint64) {
	inode, ref, status := w.resolve(path)
	if status != fuse.OK {
		logResolveFailure("open", path, status)
		return toErrno(status), noHandle
	}
	defer ref.release()
	in := &fuse.OpenIn{InHeader: w.caller(inode), Flags: translateOpenFlags(flags)}
	var out fuse.OpenOut
	if status := w.wfs.Open(never, in, &out); status != fuse.OK {
		glog.Errorf("open %s inode %d: %v", path, inode, status)
		return toErrno(status), noHandle
	}
	ref.keepLast()
	w.retain(w.fileInodes, out.Fh, inode)
	return 0, out.Fh
}

func (w *WinFS) Read(path string, buff []byte, ofst int64, fh uint64) int {
	if fh == noHandle {
		return -eBADF
	}
	in := &fuse.ReadIn{
		Fh:     fh,
		Offset: uint64(ofst),
		Size:   uint32(len(buff)),
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
	if w.denied() {
		return -eROFS
	}
	if fh == noHandle {
		return -eBADF
	}
	in := &fuse.WriteIn{
		Fh:     fh,
		Offset: uint64(ofst),
		Size:   uint32(len(buff)),
	}
	written, status := w.wfs.Write(never, in, buff)
	if status != fuse.OK {
		if status == fuse.ENOENT {
			glog.Errorf("write %s: handle %d is not open", path, fh)
		}
		return toErrno(status)
	}
	return int(written)
}

func (w *WinFS) Truncate(path string, size int64, fh uint64) int {
	if w.denied() {
		return -eROFS
	}
	inode := w.inodeForHandle(w.fileInodes, fh)
	if inode == 0 {
		var ref *lookupRef
		var status fuse.Status
		inode, ref, status = w.resolve(path)
		if status != fuse.OK {
			return toErrno(status)
		}
		defer ref.release()
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
	if w.denied() {
		return -eROFS
	}
	inode, ref, status := w.resolve(path)
	if status != fuse.OK {
		return toErrno(status)
	}
	defer ref.release()
	in := &fuse.SetAttrIn{}
	in.NodeId = inode
	in.Valid = fuse.FATTR_MODE
	in.Mode = mode
	var out fuse.AttrOut
	return toErrno(w.wfs.SetAttr(never, in, &out))
}

func (w *WinFS) Utimens(path string, tmsp []cgofuse.Timespec) int {
	if w.denied() {
		return -eROFS
	}
	inode, ref, status := w.resolve(path)
	if status != fuse.OK {
		return toErrno(status)
	}
	defer ref.release()
	in := &fuse.SetAttrIn{}
	in.NodeId = inode
	if len(tmsp) < 2 {
		in.Valid = fuse.FATTR_ATIME_NOW | fuse.FATTR_MTIME_NOW
	} else {
		// Windows sends times around its own 1601 epoch, which arrive here as
		// a large negative second count and would be stored as a year-1601
		// timestamp that every other client then reads. Leave those alone.
		if applyTimespec(tmsp[0]) {
			in.Valid |= fuse.FATTR_ATIME
			in.Atime, in.Atimensec = uint64(tmsp[0].Sec), uint32(tmsp[0].Nsec)
		}
		if applyTimespec(tmsp[1]) {
			in.Valid |= fuse.FATTR_MTIME
			in.Mtime, in.Mtimensec = uint64(tmsp[1].Sec), uint32(tmsp[1].Nsec)
		}
		if in.Valid == 0 {
			return 0
		}
	}
	var out fuse.AttrOut
	return toErrno(w.wfs.SetAttr(never, in, &out))
}

// applyTimespec reports whether a timestamp should be written. UTIME_OMIT asks
// for the existing value to be kept, and a time at or below Windows' own 1601
// epoch arrives as a large negative second count that would be stored verbatim.
func applyTimespec(ts cgofuse.Timespec) bool {
	return ts.Nsec != utimeOmit && ts.Sec > windowsEpochCutoff
}

func (w *WinFS) Flush(path string, fh uint64) int {
	if fh == noHandle {
		return 0
	}
	return toErrno(w.wfs.Flush(never, &fuse.FlushIn{InHeader: w.caller(0), Fh: fh}))
}

func (w *WinFS) Fsync(path string, datasync bool, fh uint64) int {
	if fh == noHandle {
		return 0
	}
	return toErrno(w.wfs.Fsync(never, &fuse.FsyncIn{Fh: fh}))
}

func (w *WinFS) Release(path string, fh uint64) int {
	if fh == noHandle {
		return 0
	}
	w.wfs.Release(never, &fuse.ReleaseIn{Fh: fh})
	w.forget(w.releaseRetained(w.fileInodes, fh))
	return 0
}

func (w *WinFS) Opendir(path string) (int, uint64) {
	inode, ref, status := w.resolve(path)
	if status != fuse.OK {
		return toErrno(status), noHandle
	}
	defer ref.release()
	var out fuse.OpenOut
	if status := w.wfs.OpenDir(never, &fuse.OpenIn{InHeader: w.caller(inode)}, &out); status != fuse.OK {
		return toErrno(status), noHandle
	}
	ref.keepLast()
	w.retain(w.dirInodes, out.Fh, inode)
	return 0, out.Fh
}

func (w *WinFS) Releasedir(path string, fh uint64) int {
	if fh == noHandle {
		return 0
	}
	w.wfs.ReleaseDir(&fuse.ReleaseIn{Fh: fh})
	w.forget(w.releaseRetained(w.dirInodes, fh))
	return 0
}

// readdirSink collects one readdir round. The raw operation fills the returned
// EntryOut after AddEntryPlus returns, so nothing can be converted until the
// round is over.
type readdirSink struct {
	names   []string
	offsets []uint64
	inodes  []uint64
	modes   []uint32
	attrs   []*fuse.EntryOut
	limit   int

	// lastOffset advances over dropped entries too, so a batch that is all
	// dot entries still moves the enumeration along.
	lastOffset uint64
	seen       int
}

// WinFsp strips "." and ".." for the root directory itself and expects every
// other directory to report them, the way a real NTFS enumeration does.
func (s *readdirSink) AddEntry(entry fuse.DirEntry) bool {
	if len(s.names) >= s.limit {
		return false
	}
	s.seen++
	s.lastOffset = entry.Off
	s.names = append(s.names, entry.Name)
	s.offsets = append(s.offsets, entry.Off)
	s.inodes = append(s.inodes, entry.Ino)
	s.modes = append(s.modes, entry.Mode)
	s.attrs = append(s.attrs, nil)
	return true
}

func (s *readdirSink) AddEntryPlus(entry fuse.DirEntry) *fuse.EntryOut {
	if len(s.names) >= s.limit {
		return nil
	}
	s.seen++
	s.lastOffset = entry.Off
	out := &fuse.EntryOut{}
	s.names = append(s.names, entry.Name)
	s.offsets = append(s.offsets, entry.Off)
	s.inodes = append(s.inodes, entry.Ino)
	s.modes = append(s.modes, entry.Mode)
	s.attrs = append(s.attrs, out)
	return out
}

func (w *WinFS) Readdir(path string, fill func(name string, stat *cgofuse.Stat_t, ofst int64) bool, ofst int64, fh uint64) int {
	inode := w.inodeForHandle(w.dirInodes, fh)
	if inode == 0 {
		var ref *lookupRef
		var status fuse.Status
		inode, ref, status = w.resolve(path)
		if status != fuse.OK {
			return toErrno(status)
		}
		defer ref.release()
	}
	offset := uint64(ofst)
	for {
		sink := &readdirSink{limit: readdirBatch}
		in := &fuse.ReadIn{
			InHeader: w.caller(inode),
			Fh:       fh,
			Offset:   offset,
			Size:     1 << 20,
		}
		if status := w.wfs.ReadDirectoryInto(in, sink, true); status != fuse.OK {
			return toErrno(status)
		}
		if sink.seen == 0 {
			return 0
		}
		filled := true
		for i, name := range sink.names {
			var stat cgofuse.Stat_t
			var statp *cgofuse.Stat_t
			if attr := sink.attrs[i]; attr != nil && attr.Attr.Mode != 0 {
				w.attrToStat(&attr.Attr, &stat)
				statp = &stat
			} else if sink.modes[i] != 0 {
				// "." and ".." are reported without attributes: the readdir
				// only fills a block for real children. Windows still needs
				// their type, and enumerating a directory whose first entry is
				// not marked as one fails outright.
				stat.Mode = sink.modes[i]
				stat.Ino = sink.inodes[i]
				stat.Nlink = 1
				stat.Uid, stat.Gid = w.uid, w.gid
				statp = &stat
			}
			if filled && !fill(name, statp, int64(sink.offsets[i])) {
				filled = false
			}
		}
		// A readdirplus entry carries a reference of its own. Give every one
		// of them back, including any the fill above stopped short of, or a
		// single walk of a wide directory strands one reference per child.
		for _, child := range sink.inodes {
			w.forget(child)
		}
		if !filled {
			return 0
		}
		if sink.lastOffset <= offset {
			return 0
		}
		offset = sink.lastOffset
	}
}

func (w *WinFS) Readlink(path string) (int, string) {
	// WinFsp probes the root to decide whether the volume has symlinks, and
	// turns them on unless this fails. Leaving them on costs a getattr per
	// path component on every open, for a feature Symlink already refuses.
	if len(splitPath(path)) == 0 {
		return -eNOSYS, ""
	}
	inode, ref, status := w.resolve(path)
	if status != fuse.OK {
		return toErrno(status), ""
	}
	defer ref.release()
	target, status := w.wfs.Readlink(never, ptr(w.caller(inode)))
	if status != fuse.OK {
		return toErrno(status), ""
	}
	return 0, string(target)
}

// Symlink is refused for now. The entry is easy to create, but WinFsp only
// follows it once the reparse point is wired up, so it would otherwise read
// back as an empty file.
func (w *WinFS) Symlink(target string, newpath string) int {
	return -eNOSYS
}

// Chown accepts and discards. Windows has no uid to record, but WinFsp passes
// a chown failure straight out of SetSecurity, so refusing it breaks Explorer's
// Security tab and icacls for changes that are not about ownership at all.
func (w *WinFS) Chown(path string, uid uint32, gid uint32) int {
	return 0
}

// Link is not implemented: WinFsp has no hard links.
func (w *WinFS) Link(oldpath string, newpath string) int {
	return -eNOSYS
}
