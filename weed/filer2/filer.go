package filer2

import (
	"fmt"

	"github.com/chrislusf/seaweedfs/weed/filer2/embedded"
	"github.com/karlseguin/ccache"
	"strings"
)

type Filer struct {
	master         string
	store          FilerStore
	directoryCache *ccache.Cache
}

func NewFiler(master string) *Filer {
	return &Filer{
		master:         master,
		directoryCache: ccache.New(ccache.Configure().MaxSize(1000).ItemsToPrune(100)),
	}
}

func NewEmbeddedFiler(master string, dir string) (*Filer, error) {
	_, err := embedded.NewEmbeddedStore(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to create embedded filer store: %v", err)
	}
	return &Filer{
		master: master,
		// store:  store,
	}, nil
}

func (f *Filer) CreateEntry(entry Entry) (error) {
	/*
	1. recursively ensure parent directory is created.
	2. get current parent directory, add link,
	3. add the file entry
	*/

	recursivelyEnsureDirectory(entry.Dir, func(parent, name string) error {
		return nil
	})

	return f.store.CreateEntry(entry)
}

func (f *Filer) AppendFileChunk(p FullPath, c FileChunk) (err error) {
	return f.store.AppendFileChunk(p, c)
}

func (f *Filer) FindEntry(p FullPath) (found bool, fileEntry Entry, err error) {
	return f.store.FindEntry(p)
}

func (f *Filer) DeleteEntry(p FullPath) (fileEntry Entry, err error) {
	return f.store.DeleteEntry(p)
}

func (f *Filer) ListDirectoryEntries(p FullPath) ([]Entry, error) {
	return f.store.ListDirectoryEntries(p)
}

func (f *Filer) UpdateEntry(entry Entry) (error) {
	return f.store.UpdateEntry(entry)
}

func recursivelyEnsureDirectory(fullPath string, fn func(parent, name string) error) (error) {
	if strings.HasSuffix(fullPath, "/") {
		fullPath = fullPath[0:len(fullPath)-1]
	}
	nextPathEnd := strings.LastIndex(fullPath, "/")
	if nextPathEnd < 0 {
		return nil
	}

	dirName := fullPath[nextPathEnd+1:]
	parentDirPath := fullPath[0:nextPathEnd]

	if parentDirPath == "" {
		parentDirPath = "/"
	}

	if err := recursivelyEnsureDirectory(parentDirPath, fn); err != nil {
		return err
	}

	if err := fn(parentDirPath, dirName); err != nil {
		return err
	}

	return nil
}

func (f *Filer) cacheGetDirectory(dirpath string) (error) {
	return nil
}
