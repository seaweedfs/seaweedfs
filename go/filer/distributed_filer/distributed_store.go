package distributed_filer

import "github.com/chrislusf/seaweedfs/go/filer"

type DistributedStore interface {
	Put(fullFileName string, fid string) (err error)
	Get(fullFileName string) (fid string, err error)
	Delete(fullFileName string) (fid string, err error)

	FindDirectory(dirPath string) (dirId filer.DirectoryId, err error)
	ListDirectories(dirPath string) (dirs []filer.DirectoryEntry, err error)
	ListFiles(dirPath string, lastFileName string, limit int) (files []filer.FileEntry, err error)
	DeleteDirectory(dirPath string, recursive bool) (err error)
	Move(fromPath string, toPath string) error
}
