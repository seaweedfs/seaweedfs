package filer2

import (
	"errors"
	"os"
	"time"
)

type FileId string //file id in SeaweedFS
type FullPath struct {
	Dir  string //full path of the parent dir
	Name string //file name without path
}

type Attr struct {
	Mtime       time.Time   // time of last modification
	Crtime      time.Time   // time of creation (OS X only)
	Mode        os.FileMode // file mode
	Uid         uint32      // owner uid
	Gid         uint32      // group gid
	IsDirectory bool
	Size        uint64 // total size in bytes
	Nlink       uint32 // number of links (usually 1)
}

type Entry struct {
	Dir  string `json:"dir,omitempty"`  //full path of the parent dir
	Name string `json:"name,omitempty"` //file name without path

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
	CreateEntry(Entry) (error)
	AppendFileChunk(FullPath, FileChunk) (err error)
	FindEntry(FullPath) (found bool, fileEntry Entry, err error)
	DeleteEntry(FullPath) (fileEntry Entry, err error)

	ListDirectoryEntries(dirPath FullPath) ([]Entry, error)
	UpdateEntry(Entry) (error)
}

var ErrNotFound = errors.New("filer: no entry is found in filer store")

type FilerStore interface {
	CreateEntry(Entry) (error)
	AppendFileChunk(FullPath, FileChunk) (err error)
	FindEntry(FullPath) (found bool, fileEntry Entry, err error)
	DeleteEntry(FullPath) (fileEntry Entry, err error)

	ListDirectoryEntries(dirPath FullPath) ([]Entry, error)
	UpdateEntry(Entry) (error)
}
