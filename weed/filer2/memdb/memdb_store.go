package memdb

import (
	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/google/btree"
	"strings"
	"fmt"
	"time"
	"github.com/spf13/viper"
)

func init() {
	filer2.Stores = append(filer2.Stores, &MemDbStore{})
}

type MemDbStore struct {
	tree *btree.BTree
}

type Entry struct {
	*filer2.Entry
}

func (a Entry) Less(b btree.Item) bool {
	return strings.Compare(string(a.FullPath), string(b.(Entry).FullPath)) < 0
}

func (filer *MemDbStore) GetName() string {
	return "memory"
}

func (filer *MemDbStore) Initialize(viper *viper.Viper) (err error) {
	filer.tree = btree.New(8)
	return nil
}

func (filer *MemDbStore) InsertEntry(entry *filer2.Entry) (err error) {
	// println("inserting", entry.FullPath)
	entry.Crtime = time.Now()
	filer.tree.ReplaceOrInsert(Entry{entry})
	return nil
}

func (filer *MemDbStore) UpdateEntry(entry *filer2.Entry) (err error) {
	if _, err = filer.FindEntry(entry.FullPath); err != nil {
		return fmt.Errorf("no such file %s : %v", entry.FullPath, err)
	}
	entry.Mtime = time.Now()
	filer.tree.ReplaceOrInsert(Entry{entry})
	return nil
}

func (filer *MemDbStore) FindEntry(fullpath filer2.FullPath) (entry *filer2.Entry, err error) {
	item := filer.tree.Get(Entry{&filer2.Entry{FullPath: fullpath}})
	if item == nil {
		return nil, nil
	}
	entry = item.(Entry).Entry
	return entry, nil
}

func (filer *MemDbStore) DeleteEntry(fullpath filer2.FullPath) (entry *filer2.Entry, err error) {
	item := filer.tree.Delete(Entry{&filer2.Entry{FullPath: fullpath}})
	if item == nil {
		return nil, nil
	}
	entry = item.(Entry).Entry
	return entry, nil
}

func (filer *MemDbStore) ListDirectoryEntries(fullpath filer2.FullPath, startFileName string, inclusive bool, limit int) (entries []*filer2.Entry, err error) {

	startFrom := string(fullpath)
	if startFileName != "" {
		startFrom = startFrom + "/" + startFileName
	}

	filer.tree.AscendGreaterOrEqual(Entry{&filer2.Entry{FullPath: filer2.FullPath(startFrom)}},
		func(item btree.Item) bool {
			if limit <= 0 {
				return false
			}
			entry := item.(Entry).Entry
			// println("checking", entry.FullPath)

			if entry.FullPath == fullpath {
				// skipping the current directory
				// println("skipping the folder", entry.FullPath)
				return true
			}

			dir, name := entry.FullPath.DirAndName()
			if name == startFileName {
				if inclusive {
					limit--
					entries = append(entries, entry)
				}
				return true
			}

			// only iterate the same prefix
			if !strings.HasPrefix(string(entry.FullPath), string(fullpath)) {
				// println("breaking from", entry.FullPath)
				return false
			}

			if dir != string(fullpath) {
				// this could be items in deeper directories
				// println("skipping deeper folder", entry.FullPath)
				return true
			}
			// now process the directory items
			// println("adding entry", entry.FullPath)
			limit--
			entries = append(entries, entry)
			return true
		},
	)
	return entries, nil
}
