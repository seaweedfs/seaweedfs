package embedded_filer

import (
	"github.com/chrislusf/seaweedfs/weed/filer"
)

type DirectoryManager interface {
	FindDirectory(dirPath string) (DirectoryId, error)
	ListDirectories(dirPath string) (dirs []filer.DirectoryName, err error)
	MakeDirectory(currentDirPath string, dirName string) (DirectoryId, error)
	MoveUnderDirectory(oldDirPath string, newParentDirPath string) error
	DeleteDirectory(dirPath string) error
	//functions used by FUSE
	FindDirectoryById(DirectoryId, error)
}
