package filer

import ()

type FileId string //file id on weedfs

type FileEntry struct {
	Name string //file name without path
	Id   FileId
}

type Filer interface {
	CreateFile(filePath string, fid string) (err error)
	FindFile(filePath string) (fid string, err error)
	ListDirectories(dirPath string) (dirs []DirectoryEntry, err error)
	ListFiles(dirPath string, lastFileName string, limit int) (files []FileEntry, err error)
	DeleteDirectory(dirPath string) (err error)
	DeleteFile(filePath string) (fid string, err error)
}
