package flat_namespace

import (
	"errors"

	"github.com/chrislusf/seaweedfs/weed/filer"
	"path/filepath"
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
func (filer *FlatNamespaceFiler) LookupDirectoryEntry(dirPath string, name string) (found bool, fileId string, err error) {
	if fileId, err = filer.FindFile(filepath.Join(dirPath, name)); err == nil {
		return true, fileId, nil
	}
	return false, "", err
}
func (filer *FlatNamespaceFiler) ListDirectories(dirPath string) (dirs []filer.DirectoryName, err error) {
	return nil, ErrNotImplemented
}
func (filer *FlatNamespaceFiler) ListFiles(dirPath string, lastFileName string, limit int) (files []filer.FileEntry, err error) {
	return nil, ErrNotImplemented
}
func (filer *FlatNamespaceFiler) DeleteDirectory(dirPath string, recursive bool) (err error) {
	return ErrNotImplemented
}

func (filer *FlatNamespaceFiler) DeleteFile(fullFileName string) (fid string, err error) {
	fid, err = filer.FindFile(fullFileName)
	if err != nil {
		return "", err
	}

	err = filer.store.Delete(fullFileName)
	if err != nil {
		return "", err
	}

	return fid, nil
	//return filer.store.Delete(fullFileName)
	//are you kidding me!!!!
}

func (filer *FlatNamespaceFiler) Move(fromPath string, toPath string) error {
	return ErrNotImplemented
}
