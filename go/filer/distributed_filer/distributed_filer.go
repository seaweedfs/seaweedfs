package distributed_filer

import "github.com/chrislusf/seaweedfs/go/filer"

type DistributedFiler struct {
	master string
	store  DistributedStore
}

func NewDistributedFiler(master string, store DistributedStore) *DistributedFiler {
	return &DistributedFiler{
		master: master,
		store:  store,
	}
}

func (filer *DistributedFiler) CreateFile(fullFileName string, fid string) (err error) {
	return filer.store.Put(fullFileName, fid)
}
func (filer *DistributedFiler) FindFile(fullFileName string) (fid string, err error) {
	return filer.store.Get(fullFileName)
}
func (filer *DistributedFiler) FindDirectory(dirPath string) (dirId filer.DirectoryId, err error) {
	return filer.store.FindDirectory(dirPath)
}
func (filer *DistributedFiler) ListDirectories(dirPath string) (dirs []filer.DirectoryEntry, err error) {
	return filer.store.ListDirectories(dirPath)
}
func (filer *DistributedFiler) ListFiles(dirPath string, lastFileName string, limit int) (files []filer.FileEntry, err error) {
	return filer.store.ListFiles(dirPath, lastFileName, limit)
}
func (filer *DistributedFiler) DeleteDirectory(dirPath string, recursive bool) (err error) {
	return filer.store.DeleteDirectory(dirPath, recursive)
}

func (filer *DistributedFiler) DeleteFile(fullFileName string) (fid string, err error) {
	return filer.store.Delete(fullFileName)
}

func (filer *DistributedFiler) Move(fromPath string, toPath string) error {
	return filer.store.Move(fromPath, toPath)
}
