package filer

import (
	"code.google.com/p/weed-fs/go/operation"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
)

type FilerEmbedded struct {
	master      string
	directories *DirectoryManagerInMap
	files       *FileListInLevelDb
}

func NewFilerEmbedded(master string, dir string) (filer *FilerEmbedded, err error) {
	dm, de := NewDirectoryManagerInMap(filepath.Join(dir, "dir.log"))
	if de != nil {
		return nil, de
	}
	fl, fe := NewFileListInLevelDb(dir)
	if fe != nil {
		return nil, fe
	}
	filer = &FilerEmbedded{
		master:      master,
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
func (filer *FilerEmbedded) DeleteDirectory(dirPath string, recursive bool) (err error) {
	dirId, e := filer.directories.FindDirectory(dirPath)
	if e != nil {
		return e
	}
	if sub_dirs, sub_err := filer.directories.ListDirectories(dirPath); sub_err == nil {
		if len(sub_dirs) > 0 && !recursive {
			return fmt.Errorf("Fail to delete directory %s: %d sub directories found!", dirPath, len(sub_dirs))
		}
		for _, sub := range sub_dirs {
			if delete_sub_err := filer.DeleteDirectory(filepath.Join(dirPath, sub.Name), recursive); delete_sub_err != nil {
				return delete_sub_err
			}
		}
	}
	list := filer.files.ListFiles(dirId, "", 100)
	if len(list) != 0 && !recursive {
		if !recursive {
			return fmt.Errorf("Fail to delete non-empty directory %s!", dirPath)
		}
	}
	for {
		if len(list) == 0 {
			return filer.directories.DeleteDirectory(dirPath)
		}
		var fids []string
		for _, fileEntry := range list {
			fids = append(fids, string(fileEntry.Id))
		}
		if result_list, delete_file_err := operation.DeleteFiles(filer.master, fids); delete_file_err != nil {
			return delete_file_err
		} else {
			if len(result_list.Errors) > 0 {
				return errors.New(strings.Join(result_list.Errors, "\n"))
			}
		}
		lastFile := list[len(list)-1]
		list = filer.files.ListFiles(dirId, lastFile.Name, 100)
	}

}
func (filer *FilerEmbedded) DeleteFile(filePath string) (fid string, err error) {
	dir, file := filepath.Split(filePath)
	dirId, e := filer.directories.FindDirectory(dir)
	if e != nil {
		return "", e
	}
	return filer.files.DeleteFile(dirId, file)
}
