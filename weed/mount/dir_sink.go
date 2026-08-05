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
