package flat_namespace

import (
	"errors"

	"github.com/chrislusf/weed-fs/go/filer"
)

type FlatNamesapceFiler struct {
	master string
	store  FlatNamespaceStore
}

var (
	NotImplemented = errors.New("Not Implemented for flat namespace meta data store!")
)

func NewFlatNamesapceFiler(master string, store FlatNamespaceStore) *FlatNamesapceFiler {
	return &FlatNamesapceFiler{
		master: master,
		store:  store,
	}
}

func (filer *FlatNamesapceFiler) CreateFile(fullFileName string, fid string) (err error) {
	return filer.store.Put(fullFileName, fid)
}
func (filer *FlatNamesapceFiler) FindFile(fullFileName string) (fid string, err error) {
	return filer.store.Get(fullFileName)
}
func (filer *FlatNamesapceFiler) FindDirectory(dirPath string) (dirId filer.DirectoryId, err error) {
	return 0, NotImplemented
}
func (filer *FlatNamesapceFiler) ListDirectories(dirPath string) (dirs []filer.DirectoryEntry, err error) {
	return nil, NotImplemented
}
func (filer *FlatNamesapceFiler) ListFiles(dirPath string, lastFileName string, limit int) (files []filer.FileEntry, err error) {
	return nil, NotImplemented
}
func (filer *FlatNamesapceFiler) DeleteDirectory(dirPath string, recursive bool) (err error) {
	return NotImplemented
}

func (filer *FlatNamesapceFiler) DeleteFile(fullFileName string) (fid string, err error) {
	return filer.store.Delete(fullFileName)
}

func (filer *FlatNamesapceFiler) Move(fromPath string, toPath string) error {
	return NotImplemented
}
