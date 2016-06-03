package flat_namespace

import (
	"errors"

	"github.com/chrislusf/seaweedfs/weed/filer"
)

type FlatNamespaceFiler struct {
	master string
	store  FlatNamespaceStore
}

var (
	ErrNotImplemented = errors.New("Not Implemented for flat namespace meta data store")
)

func NewFlatNamespaceFiler(master string, store FlatNamespaceStore) *FlatNamespaceFiler {
	return &FlatNamespaceFiler{
		master: master,
		store:  store,
	}
}

func (filer *FlatNamespaceFiler) CreateFile(fullFileName string, fid string) (err error) {
	return filer.store.Put(fullFileName, fid)
}
func (filer *FlatNamespaceFiler) FindFile(fullFileName string) (fid string, err error) {
	return filer.store.Get(fullFileName)
}
func (filer *FlatNamespaceFiler) FindDirectory(dirPath string) (dirId filer.DirectoryId, err error) {
	return 0, ErrNotImplemented
}
func (filer *FlatNamespaceFiler) ListDirectories(dirPath string) (dirs []filer.DirectoryEntry, err error) {
	return nil, ErrNotImplemented
}
func (filer *FlatNamespaceFiler) ListFiles(dirPath string, lastFileName string, limit int) (files []filer.FileEntry, err error) {
	return nil, ErrNotImplemented
}
func (filer *FlatNamespaceFiler) DeleteDirectory(dirPath string, recursive bool) (err error) {
	return ErrNotImplemented
}

func (filer *FlatNamespaceFiler) DeleteFile(fullFileName string) (fid string, err error) {
	return filer.store.Delete(fullFileName)
}

func (filer *FlatNamespaceFiler) Move(fromPath string, toPath string) error {
	return ErrNotImplemented
}
