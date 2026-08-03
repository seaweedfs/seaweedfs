package mount

import (
	"github.com/seaweedfs/go-fuse/v2/fuse"
)

// DirEntrySink receives the entries a readdir produces. The FUSE server packs
// them straight into the kernel's reply buffer; a front end that is not the
// kernel reads them out instead of re-parsing that wire format.
type DirEntrySink interface {
	// AddEntry reports one entry, returning false once the sink is full. A
	// full sink ends the batch; the client resumes from the entry's Off.
	AddEntry(entry fuse.DirEntry) bool

	// AddEntryPlus is AddEntry for readdirplus, returning the attribute block
	// to fill in, or nil once the sink is full.
	AddEntryPlus(entry fuse.DirEntry) *fuse.EntryOut
}

// fuseDirEntryList adapts the kernel reply buffer to DirEntrySink.
type fuseDirEntryList struct {
	*fuse.DirEntryList
}

func (l fuseDirEntryList) AddEntry(entry fuse.DirEntry) bool {
	return l.AddDirEntry(entry)
}

func (l fuseDirEntryList) AddEntryPlus(entry fuse.DirEntry) *fuse.EntryOut {
	return l.AddDirLookupEntry(entry)
}

// ReadDirectoryInto runs a readdir against sink. ReadDir and ReadDirPlus are
// this with the kernel reply buffer as the sink.
func (wfs *WFS) ReadDirectoryInto(input *fuse.ReadIn, sink DirEntrySink, isPlusMode bool) fuse.Status {
	return wfs.doReadDirectory(input, sink, isPlusMode)
}

// KnownInode returns the inode already tracked for a path below the mount
// root, if this mount has seen it. A front end that addresses files by path
// uses this to avoid re-walking a directory chain it has walked before: the
// walk costs a filer lookup per component, and a lookup that races a meta
// cache refresh reports ENOENT for a directory that plainly exists.
func (wfs *WFS) KnownInode(relativePath string) (uint64, bool) {
	root, status := wfs.inodeToPath.GetPath(1)
	if status != fuse.OK {
		return 0, false
	}
	full := root
	if relativePath != "" {
		full = root.Child(relativePath)
	}
	return wfs.inodeToPath.GetInode(full)
}
