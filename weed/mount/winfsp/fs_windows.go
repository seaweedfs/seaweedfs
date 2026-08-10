package winfsp

import (
	"strings"
	"sync"
	"syscall"
	"time"

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

	// stealAttempts bounds the resolve-then-steal loop in the open paths. A
	// miss needs the entry to be evicted in the instant between the two calls,
	// so a second round is already unlikely.
	stealAttempts = 4
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

	// paths holds the lookup references for resolved paths, standing in for
	// the kernel caches of the unix mounts.
	paths *pathCache

	// An open handle holds a lookup reference for its inode until Release,
	// the way the kernel keeps one for an open file. WinFsp hands back only
	// the handle, and the raw filesystem reuses one handle for repeated opens
	// of the same inode, so the references are counted.
	mu         sync.Mutex
	fileInodes map[uint64]*handleRef
	dirInodes  map[uint64]*handleRef
}

func NewWinFS(wfs *mount.WFS, uid, gid uint32, readOnly bool, cacheTimeout time.Duration) *WinFS {
	w := &WinFS{
		wfs:        wfs,
		uid:        uid,
		gid:        gid,
		readOnly:   readOnly,
		fileInodes: make(map[uint64]*handleRef),
		dirInodes:  make(map[uint64]*handleRef),
	}
	w.paths = newPathCache(cacheTimeout, w.forget)
	return w
}

// invalidatePath is what the notifier calls when a metadata event arrives:
// whatever is cached for that path is out of date. Directories take their
// subtree with them, since the children's cached paths point through them.
func (w *WinFS) invalidatePath(path string, isDirectory bool) {
	w.paths.purge(cacheKey(path), isDirectory)
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

// forget returns one reference. The root is never looked up, so it never holds
// one to give back.
func (w *WinFS) forget(inode uint64) {
	if inode == rootInode || inode == 0 {
		return
	}
	w.wfs.Forget(inode, 1)
}

// walk resolves a path one component at a time, filling the path cache as it
// goes. Every reference a lookup grants is owned by the cache, which keeps the
// returned inode alive for at least one cache sweep; an operation that needs
// to hold the inode longer steals the reference from the cache.
func (w *WinFS) walk(parts []string) (uint64, fuse.Status) {
	inode := uint64(rootInode)
	for i, name := range parts {
		key := strings.Join(parts[:i+1], "/")
		if cached, _, ok := w.paths.lookup(key); ok {
			inode = cached
			continue
		}
		var out fuse.EntryOut
		if status := w.wfs.Lookup(never, ptr(w.caller(inode)), name, &out); status != fuse.OK {
			return 0, status
		}
		w.paths.insert(key, out.NodeId, out.Attr)
		inode = out.NodeId
	}
	return inode, fuse.OK
}

// resolve walks a WinFsp path down to an inode, valid at least until the next
// cache sweep.
func (w *WinFS) resolve(path string) (uint64, fuse.Status) {
	return w.walk(splitPath(path))
}

// resolveParent resolves everything but the last component, which the create
// and delete operations need separately.
func (w *WinFS) resolveParent(path string) (uint64, string, fuse.Status) {
	parentParts, name, ok := splitParent(path)
	if !ok {
		return 0, "", fuse.EINVAL
	}
	parent, status := w.walk(parentParts)
	if status != fuse.OK {
		return 0, "", status
	}
	return parent, name, fuse.OK
}

// resolveAndSteal resolves path and takes over the cache's reference on the
// final inode, for the open paths that hold it for the life of a handle. The
// root is handed out without a reference; it does not need one.
func (w *WinFS) resolveAndSteal(path string) (uint64, fuse.Status) {
	key := cacheKey(path)
	for attempt := 0; attempt < stealAttempts; attempt++ {
		inode, status := w.resolve(path)
		if status != fuse.OK {
			return 0, status
		}
		if key == "" {
			return inode, fuse.OK
		}
		if stolen, ok := w.paths.steal(key); ok {
			return stolen, fuse.OK
		}
	}
	return 0, fuse.EIO
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
	// The filer records a creation time, but the raw protocol's Attr carries no
	// field for it outside darwin, so ctime stands in until there is a way to
	// read it through.
	stat.Birthtim = stat.Ctim
	// Windows expects a regular file to be marked archived; with no flags at
	// all it synthesises NORMAL, which is a different thing and what
	// create_fileattr_test checks.
	if attr.Mode&cgofuse.S_IFMT == cgofuse.S_IFREG {
		stat.Flags |= cgofuse.UF_ARCHIVE
	}
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

// getattrFromCache serves attributes the way the kernel would from its
// attribute cache. A file with an open handle is excluded: its live size is
// on the handle, not in the cache.
func (w *WinFS) getattrFromCache(key string, stat *cgofuse.Stat_t) bool {
	if key == "" {
		return false
	}
	inode, attr, ok := w.paths.lookup(key)
	if !ok || w.wfs.IsFileOpen(inode) {
		return false
	}
	w.attrToStat(&attr, stat)
	return true
}

func (w *WinFS) Getattr(path string, stat *cgofuse.Stat_t, fh uint64) int {
	inode := w.inodeForHandle(w.fileInodes, fh)
	if inode == 0 {
		key := cacheKey(path)
		if fh == noHandle && w.getattrFromCache(key, stat) {
			return 0
		}
		var status fuse.Status
		inode, status = w.resolve(path)
		if status != fuse.OK {
			logResolveFailure("getattr", path, status)
			return toErrno(status)
		}
		// The walk just refreshed the cache, so this hits unless the file is
		// open or is the root, and saves the second load of the same entry.
		if fh == noHandle && w.getattrFromCache(key, stat) {
			return 0
		}
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

// purgeWithParent drops a mutated path from the cache along with its parent,
// whose cached mtime the mutation just outdated.
func (w *WinFS) purgeWithParent(path string, prefix bool) {
	key := cacheKey(path)
	w.paths.purge(key, prefix)
	if i := strings.LastIndexByte(key, '/'); i > 0 {
		w.paths.purge(key[:i], false)
	}
}

func (w *WinFS) Mkdir(path string, mode uint32) int {
	if w.denied() {
		return -eROFS
	}
	parent, name, status := w.resolveParent(path)
	if status != fuse.OK {
		return toErrno(status)
	}
	in := &fuse.MkdirIn{InHeader: w.caller(parent), Mode: mode}
	var out fuse.EntryOut
	status = w.wfs.Mkdir(never, in, name, &out)
	if status == fuse.OK {
		w.purgeWithParent(path, false)
		// The new directory is about to be filled; cache it, reference and all.
		w.paths.insert(cacheKey(path), out.NodeId, out.Attr)
	}
	return toErrno(status)
}

func (w *WinFS) Rmdir(path string) int {
	if w.denied() {
		return -eROFS
	}
	parent, name, status := w.resolveParent(path)
	if status != fuse.OK {
		return toErrno(status)
	}
	status = w.wfs.Rmdir(never, ptr(w.caller(parent)), name)
	if status == fuse.OK {
		w.purgeWithParent(path, true)
	}
	return toErrno(status)
}

func (w *WinFS) Unlink(path string) int {
	if w.denied() {
		return -eROFS
	}
	parent, name, status := w.resolveParent(path)
	if status != fuse.OK {
		return toErrno(status)
	}
	status = w.wfs.Unlink(never, ptr(w.caller(parent)), name)
	if status == fuse.OK {
		w.purgeWithParent(path, false)
	}
	return toErrno(status)
}

func (w *WinFS) Rename(oldpath string, newpath string) int {
	if w.denied() {
		return -eROFS
	}
	oldParent, oldName, status := w.resolveParent(oldpath)
	if status != fuse.OK {
		return toErrno(status)
	}
	newParent, newName, status := w.resolveParent(newpath)
	if status != fuse.OK {
		return toErrno(status)
	}
	in := &fuse.RenameIn{InHeader: w.caller(oldParent), Newdir: newParent}
	status = w.wfs.Rename(never, in, oldName, newName)
	if status == fuse.OK {
		// Directories carry their subtree's cached paths with them, and the
		// rename may have replaced an entry at the destination.
		w.purgeWithParent(oldpath, true)
		w.purgeWithParent(newpath, true)
	}
	return toErrno(status)
}

func (w *WinFS) Create(path string, flags int, mode uint32) (int, uint64) {
	if w.denied() {
		return -eROFS, noHandle
	}
	parent, name, status := w.resolveParent(path)
	if status != fuse.OK {
		glog.Errorf("create %s: resolving the parent directory: %v", path, status)
		return toErrno(status), noHandle
	}
	in := &fuse.CreateIn{InHeader: w.caller(parent), Flags: translateOpenFlags(flags), Mode: mode}
	var out fuse.CreateOut
	if status := w.wfs.Create(never, in, name, &out); status != fuse.OK {
		glog.Errorf("create %s in inode %d: %v", name, parent, status)
		return toErrno(status), noHandle
	}
	// Whatever the cache held for this path was replaced by the create.
	w.purgeWithParent(path, false)
	// Create grants a reference on the new inode; hold it for as long as the
	// handle lives and give it back in Release.
	w.retain(w.fileInodes, out.Fh, out.NodeId)
	return 0, out.Fh
}

func (w *WinFS) Open(path string, flags int) (int, uint64) {
	inode, status := w.resolveAndSteal(path)
	if status != fuse.OK {
		logResolveFailure("open", path, status)
		return toErrno(status), noHandle
	}
	in := &fuse.OpenIn{InHeader: w.caller(inode), Flags: translateOpenFlags(flags)}
	var out fuse.OpenOut
	if status := w.wfs.Open(never, in, &out); status != fuse.OK {
		glog.Errorf("open %s inode %d: %v", path, inode, status)
		w.forget(inode)
		return toErrno(status), noHandle
	}
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
		var status fuse.Status
		inode, status = w.resolve(path)
		if status != fuse.OK {
			return toErrno(status)
		}
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
	status := w.wfs.SetAttr(never, in, &out)
	if status == fuse.OK {
		w.paths.purge(cacheKey(path), false)
	}
	return toErrno(status)
}

func (w *WinFS) Chmod(path string, mode uint32) int {
	if w.denied() {
		return -eROFS
	}
	inode, status := w.resolve(path)
	if status != fuse.OK {
		return toErrno(status)
	}
	in := &fuse.SetAttrIn{}
	in.NodeId = inode
	in.Valid = fuse.FATTR_MODE
	in.Mode = mode
	var out fuse.AttrOut
	status = w.wfs.SetAttr(never, in, &out)
	if status == fuse.OK {
		w.paths.purge(cacheKey(path), false)
	}
	return toErrno(status)
}

func (w *WinFS) Utimens(path string, tmsp []cgofuse.Timespec) int {
	if w.denied() {
		return -eROFS
	}
	inode, status := w.resolve(path)
	if status != fuse.OK {
		return toErrno(status)
	}
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
	status = w.wfs.SetAttr(never, in, &out)
	if status == fuse.OK {
		w.paths.purge(cacheKey(path), false)
	}
	return toErrno(status)
}

// applyTimespec reports whether a timestamp should be written. UTIME_OMIT asks
// for the existing value to be kept, and a time at or below Windows' own 1601
// epoch arrives as a large negative second count. Exactly zero is what Windows
// sends for a field it is not setting; storing it put 1970 in the atime
// overlay, which then read back as the file's access time. A date genuinely
// before 1970 is still allowed through — only the epoch itself is the
// sentinel.
func applyTimespec(ts cgofuse.Timespec) bool {
	if ts.Nsec == utimeOmit {
		return false
	}
	if ts.Sec == 0 && ts.Nsec == 0 {
		return false
	}
	return ts.Sec > windowsEpochCutoff
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
	// The close flushed writes, so cached attributes for the path are behind.
	w.paths.purge(cacheKey(path), false)
	return 0
}

func (w *WinFS) Opendir(path string) (int, uint64) {
	inode, status := w.resolveAndSteal(path)
	if status != fuse.OK {
		return toErrno(status), noHandle
	}
	var out fuse.OpenOut
	if status := w.wfs.OpenDir(never, &fuse.OpenIn{InHeader: w.caller(inode)}, &out); status != fuse.OK {
		w.forget(inode)
		return toErrno(status), noHandle
	}
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

// WinFsp has no FORGET, and every EntryOut is converted before the round ends.
func (s *readdirSink) TakesLookupRef() bool { return false }

func (w *WinFS) Readdir(path string, fill func(name string, stat *cgofuse.Stat_t, ofst int64) bool, ofst int64, fh uint64) int {
	inode := w.inodeForHandle(w.dirInodes, fh)
	if inode == 0 {
		var status fuse.Status
		inode, status = w.resolve(path)
		if status != fuse.OK {
			return toErrno(status)
		}
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
	inode, status := w.resolve(path)
	if status != fuse.OK {
		return toErrno(status), ""
	}
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
