package filer2

import (
	"fmt"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/karlseguin/ccache"
	"os"
	"path/filepath"
	"strings"
	"time"
	"github.com/chrislusf/seaweedfs/weed/operation"
)

type Filer struct {
	masters        []string
	store          FilerStore
	directoryCache *ccache.Cache

	currentMaster string
}

func NewFiler(masters []string) *Filer {
	return &Filer{
		masters:        masters,
		directoryCache: ccache.New(ccache.Configure().MaxSize(1000).ItemsToPrune(100)),
	}
}

func (f *Filer) SetStore(store FilerStore) {
	f.store = store
}

func (f *Filer) DisableDirectoryCache() {
	f.directoryCache = nil
}

func (f *Filer) CreateEntry(entry *Entry) error {

	dirParts := strings.Split(string(entry.FullPath), "/")

	// fmt.Printf("directory parts: %+v\n", dirParts)

	var lastDirectoryEntry *Entry

	for i := 1; i < len(dirParts); i++ {
		dirPath := "/" + filepath.Join(dirParts[:i]...)
		// fmt.Printf("%d directory: %+v\n", i, dirPath)

		// first check local cache
		dirEntry := f.cacheGetDirectory(dirPath)

		// not found, check the store directly
		if dirEntry == nil {
			glog.V(4).Infof("find uncached directory: %s", dirPath)
			dirEntry, _ = f.FindEntry(FullPath(dirPath))
		} else {
			glog.V(4).Infof("found cached directory: %s", dirPath)
		}

		// no such existing directory
		if dirEntry == nil {

			// create the directory
			now := time.Now()

			dirEntry = &Entry{
				FullPath: FullPath(dirPath),
				Attr: Attr{
					Mtime:  now,
					Crtime: now,
					Mode:   os.ModeDir | 0770,
					Uid:    entry.Uid,
					Gid:    entry.Gid,
				},
			}

			glog.V(2).Infof("create directory: %s %v", dirPath, dirEntry.Mode)
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

	/*
		if !hasWritePermission(lastDirectoryEntry, entry) {
			glog.V(0).Infof("directory %s: %v, entry: uid=%d gid=%d",
				lastDirectoryEntry.FullPath, lastDirectoryEntry.Attr, entry.Uid, entry.Gid)
			return fmt.Errorf("no write permission in folder %v", lastDirectoryEntry.FullPath)
		}
	*/

	oldEntry, _ := f.FindEntry(entry.FullPath)

	if err := f.store.InsertEntry(entry); err != nil {
		return fmt.Errorf("insert entry %s: %v", entry.FullPath, err)
	}

	f.deleteChunks(oldEntry)

	return nil
}

func (f *Filer) UpdateEntry(entry *Entry) (err error) {
	return f.store.UpdateEntry(entry)
}

func (f *Filer) FindEntry(p FullPath) (entry *Entry, err error) {
	return f.store.FindEntry(p)
}

func (f *Filer) DeleteEntryMetaAndData(p FullPath, shouldDeleteChunks bool) (err error) {
	entry, err := f.FindEntry(p)
	if err != nil {
		return err
	}

	if entry.IsDirectory() {
		entries, err := f.ListDirectoryEntries(p, "", false, 1)
		if err != nil {
			return fmt.Errorf("list folder %s: %v", p, err)
		}
		if len(entries) > 0 {
			return fmt.Errorf("folder %s is not empty", p)
		}
		f.cacheDelDirectory(string(p))
	}

	if shouldDeleteChunks {
		f.deleteChunks(entry)
	}

	return f.store.DeleteEntry(p)
}

func (f *Filer) ListDirectoryEntries(p FullPath, startFileName string, inclusive bool, limit int) ([]*Entry, error) {
	if strings.HasSuffix(string(p), "/") && len(p) > 1 {
		p = p[0: len(p)-1]
	}
	return f.store.ListDirectoryEntries(p, startFileName, inclusive, limit)
}

func (f *Filer) cacheDelDirectory(dirpath string) {
	if f.directoryCache == nil {
		return 
	}
	f.directoryCache.Delete(dirpath)
	return
}

func (f *Filer) cacheGetDirectory(dirpath string) *Entry {
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

func (f *Filer) deleteChunks(entry *Entry) {

	if entry == nil {
		return
	}
	for _, chunk := range entry.Chunks {
		if err := operation.DeleteFile(f.GetMaster(), chunk.FileId, ""); err != nil {
			glog.V(0).Infof("deleting file %s: %v", chunk.FileId, err)
		}
	}
}
