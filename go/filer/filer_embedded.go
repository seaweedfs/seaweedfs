package filer

import (
	"path/filepath"
)

type FilerEmbedded struct {
	directories *DirectoryManagerInMap
	files       *FileListInLevelDb
}

func NewFilerEmbedded(dir string) (filer *FilerEmbedded, err error) {
	dm, de := NewDirectoryManagerInMap(filepath.Join(dir, "dir.log"))
	if de != nil {
		return nil, de
	}
	fl, fe := NewFileListInLevelDb(dir)
	if fe != nil {
		return nil, fe
	}
	filer = &FilerEmbedded{
		directories: dm,
		files:       fl,
	}
	return
}

func (filer *FilerEmbedded) CreateFile(filePath string, fid string) (err error) {
	dir, file := filepath.Split(filePath)
	dirId, e := filer.directories.MakeDirectory(dir)
	if e != nil {
		return e
	}
	return filer.files.CreateFile(dirId, file, fid)
}
func (filer *FilerEmbedded) FindFile(filePath string) (fid string, err error) {
	dir, file := filepath.Split(filePath)
	dirId, e := filer.directories.FindDirectory(dir)
	if e != nil {
		return "", e
	}
	return filer.files.FindFile(dirId, file)
}
func (filer *FilerEmbedded) ListDirectories(dirPath string) (dirs []DirectoryEntry, err error) {
	return filer.directories.ListDirectories(dirPath)
}
func (filer *FilerEmbedded) ListFiles(dirPath string, lastFileName string, limit int) (files []FileEntry, err error) {
	dirId, e := filer.directories.FindDirectory(dirPath)
	if e != nil {
		return nil, e
	}
	return filer.files.ListFiles(dirId, lastFileName, limit), nil
}
func (filer *FilerEmbedded) DeleteDirectory(dirPath string) (err error) {
	return filer.directories.DeleteDirectory(dirPath)
}
func (filer *FilerEmbedded) DeleteFile(filePath string) (fid string, err error) {
	dir, file := filepath.Split(filePath)
	dirId, e := filer.directories.FindDirectory(dir)
	if e != nil {
		return "", e
	}
	return filer.files.DeleteFile(dirId, file)
}
