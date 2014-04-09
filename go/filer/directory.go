package filer

import ()

type DirectoryId int32

type DirectoryEntry struct {
	Name string //dir name without path
	Id   DirectoryId
}

type DirectoryManager interface {
	FindDirectory(dirPath string) (DirectoryId, error)
	ListDirectories(dirPath string) (dirNames []DirectoryEntry, err error)
	MakeDirectory(currentDirPath string, dirName string) (DirectoryId, error)
	MoveUnderDirectory(oldDirPath string, newParentDirPath string) error
	DeleteDirectory(dirPath string) error
}
