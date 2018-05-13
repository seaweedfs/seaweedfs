package filer2

import (
	"errors"
	"os"
	"time"
	"path/filepath"
)

type FileId string //file id in SeaweedFS
type FullPath string

func (fp FullPath) DirAndName() (string, string) {
	dir, name := filepath.Split(string(fp))
	if dir == "/" {
		return dir, name
	}
	if len(dir) < 1 {
		return "/", ""
	}
	return dir[:len(dir)-1], name
}

type Attr struct {
	Mtime  time.Time   // time of last modification
	Crtime time.Time   // time of creation (OS X only)
	Mode   os.FileMode // file mode
	Uid    uint32      // owner uid
	Gid    uint32      // group gid
	Size   uint64      // total size in bytes
}

type Entry struct {
	FullPath

	Attr

	// the following is for files
	Chunks []FileChunk `json:"chunks,omitempty"`
}

type FileChunk struct {
	Fid    FileId `json:"fid,omitempty"`
	Offset int64  `json:"offset,omitempty"`
	Size   uint64 `json:"size,omitempty"` // size in bytes
}

type AbstractFiler interface {
	CreateEntry(*Entry) (error)
	AppendFileChunk(FullPath, FileChunk) (err error)
	FindEntry(FullPath) (found bool, fileEntry *Entry, err error)
	DeleteEntry(FullPath) (fileEntry *Entry, err error)

	ListDirectoryEntries(dirPath FullPath) ([]*Entry, error)
	UpdateEntry(*Entry) (error)
}

var ErrNotFound = errors.New("filer: no entry is found in filer store")

type FilerStore interface {
	InsertEntry(*Entry) (error)
	AppendFileChunk(FullPath, FileChunk) (err error)
	FindEntry(FullPath) (found bool, entry *Entry, err error)
	DeleteEntry(FullPath) (fileEntry *Entry, err error)

	ListDirectoryEntries(dirPath FullPath) ([]*Entry, error)
}
