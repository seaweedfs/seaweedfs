package filer2

import (
	"fmt"

	"github.com/karlseguin/ccache"
	"strings"
	"path/filepath"
	"time"
	"os"
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

func (f *Filer) SetStore(store FilerStore) () {
	f.store = store
}

func (f *Filer) DisableDirectoryCache() () {
	f.directoryCache = nil
}

func (f *Filer) CreateEntry(entry *Entry) (error) {

	dirParts := strings.Split(string(entry.FullPath), "/")

	// fmt.Printf("directory parts: %+v\n", dirParts)

	var lastDirectoryEntry *Entry

	for i := 1; i < len(dirParts); i++ {
		dirPath := "/" + filepath.Join(dirParts[:i]...)
		// fmt.Printf("%d directory: %+v\n", i, dirPath)

		dirFound := false

		// first check local cache
		dirEntry := f.cacheGetDirectory(dirPath)

		// not found, check the store directly
		if dirEntry == nil {
			var dirFindErr error
			dirFound, dirEntry, dirFindErr = f.FindEntry(FullPath(dirPath))
			if dirFindErr != nil {
				return fmt.Errorf("findDirectory %s: %v", dirPath, dirFindErr)
			}
		}

		// no such existing directory
		if !dirFound {

			// create the directory
			now := time.Now()

			dirEntry = &Entry{
				FullPath: FullPath(dirPath),
				Attr: Attr{
					Mtime:  now,
					Crtime: now,
					Mode:   os.ModeDir | 0660,
					Uid:    entry.Uid,
					Gid:    entry.Gid,
				},
			}

			mkdirErr := f.store.InsertEntry(dirEntry)
			if mkdirErr != nil {
				return fmt.Errorf("mkdir %s: %v", dirPath, mkdirErr)
			}
		}

		// cache the directory entry
		f.cacheSetDirectory(dirPath, dirEntry, i)

		// remember the direct parent directory entry
		if i == len(dirParts)-1 {
			lastDirectoryEntry = dirEntry
		}

	}

	if lastDirectoryEntry == nil {
		return fmt.Errorf("parent folder not found: %v", entry.FullPath)
	}

	if !hasWritePermission(lastDirectoryEntry, entry) {
		return fmt.Errorf("no write permission in folder %v", lastDirectoryEntry.FullPath)
	}

	if err := f.store.InsertEntry(entry); err != nil {
		return fmt.Errorf("insert entry %s: %v", entry.FullPath, err)
	}

	return nil
}

func (f *Filer) AppendFileChunk(p FullPath, c FileChunk) (err error) {
	return f.store.AppendFileChunk(p, c)
}

func (f *Filer) FindEntry(p FullPath) (found bool, entry *Entry, err error) {
	return f.store.FindEntry(p)
}

func (f *Filer) DeleteEntry(p FullPath) (fileEntry *Entry, err error) {
	return f.store.DeleteEntry(p)
}

func (f *Filer) ListDirectoryEntries(p FullPath) ([]*Entry, error) {
	if strings.HasSuffix(string(p), "/") {
		p = p[0:len(p)-1]
	}
	return f.store.ListDirectoryEntries(p)
}

func (f *Filer) cacheGetDirectory(dirpath string) (*Entry) {
	if f.directoryCache == nil {
		return nil
	}
	item := f.directoryCache.Get(dirpath)
	if item == nil {
		return nil
	}
	return item.Value().(*Entry)
}

func (f *Filer) cacheSetDirectory(dirpath string, dirEntry *Entry, level int) {

	if f.directoryCache == nil {
		return
	}

	minutes := 60
	if level < 10 {
		minutes -= level * 6
	}

	f.directoryCache.Set(dirpath, dirEntry, time.Duration(minutes)*time.Minute)
}
