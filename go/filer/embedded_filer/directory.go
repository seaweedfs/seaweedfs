package embedded_filer

import (
	"github.com/chrislusf/weed-fs/go/filer"
)

type DirectoryManager interface {
	FindDirectory(dirPath string) (filer.DirectoryId, error)
	ListDirectories(dirPath string) (dirs []filer.DirectoryEntry, err error)
	MakeDirectory(currentDirPath string, dirName string) (filer.DirectoryId, error)
	MoveUnderDirectory(oldDirPath string, newParentDirPath string) error
	DeleteDirectory(dirPath string) error
	//functions used by FUSE
	FindDirectoryById(filer.DirectoryId, error)
}
