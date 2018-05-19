package memdb

import (
	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/google/btree"
	"strings"
	"fmt"
	"time"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
)

type MemDbStore struct {
	tree *btree.BTree
}

type Entry struct {
	*filer2.Entry
}

func (a Entry) Less(b btree.Item) bool {
	return strings.Compare(string(a.FullPath), string(b.(Entry).FullPath)) < 0
}

func NewMemDbStore() (filer *MemDbStore) {
	filer = &MemDbStore{}
	filer.tree = btree.New(8)
	return
}

func (filer *MemDbStore) InsertEntry(entry *filer2.Entry) (err error) {
	// println("inserting", entry.FullPath)
	filer.tree.ReplaceOrInsert(Entry{entry})
	return nil
}

func (filer *MemDbStore) AppendFileChunk(fullpath filer2.FullPath, fileChunks []*filer_pb.FileChunk) (err error) {
	found, entry, err := filer.FindEntry(fullpath)
	if !found {
		return fmt.Errorf("No such file: %s", fullpath)
	}
	entry.Chunks = append(entry.Chunks, fileChunks...)
	entry.Mtime = time.Now()
	println("appending to entry", entry.Name(), len(entry.Chunks))
	filer.tree.ReplaceOrInsert(Entry{entry})
	return nil
}

func (filer *MemDbStore) FindEntry(fullpath filer2.FullPath) (found bool, entry *filer2.Entry, err error) {
	item := filer.tree.Get(Entry{&filer2.Entry{FullPath: fullpath}})
	if item == nil {
		return false, nil, nil
	}
	entry = item.(Entry).Entry
	return true, entry, nil
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
