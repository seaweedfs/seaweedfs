package filer

import (
	"errors"
)

type FileId string //file id in SeaweedFS

type FileEntry struct {
	Name string `json:"name,omitempty"` //file name without path
	Id   FileId `json:"fid,omitempty"`
}

type DirectoryName string

type Filer interface {
	CreateFile(fullFileName string, fid string) (err error)
	FindFile(fullFileName string) (fid string, err error)
	DeleteFile(fullFileName string) (fid string, err error)

	//Optional functions. embedded filer support these
	ListDirectories(dirPath string) (dirs []DirectoryName, err error)
	ListFiles(dirPath string, lastFileName string, limit int) (files []FileEntry, err error)
	DeleteDirectory(dirPath string, recursive bool) (err error)
	Move(fromPath string, toPath string) (err error)
	LookupDirectoryEntry(dirPath string, name string) (found bool, fileId string, err error)
}

var ErrNotFound = errors.New("filer: no entry is found in filer store")
