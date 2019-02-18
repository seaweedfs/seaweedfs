package leveldb

import (
	"github.com/chrislusf/seaweedfs/weed/filer2"
	"io/ioutil"
	"os"
	"testing"
)

func TestCreateAndFind(t *testing.T) {
	filer := filer2.NewFiler(nil, nil)
	dir, _ := ioutil.TempDir("", "seaweedfs_filer_test")
	defer os.RemoveAll(dir)
	store := &LevelDBStore{}
	store.initialize(dir)
	filer.SetStore(store)
	filer.DisableDirectoryCache()

	fullpath := filer2.FullPath("/home/chris/this/is/one/file1.jpg")

	entry1 := &filer2.Entry{
		FullPath: fullpath,
		Attr: filer2.Attr{
			Mode: 0440,
			Uid:  1234,
			Gid:  5678,
		},
	}

	if err := filer.CreateEntry(entry1); err != nil {
		t.Errorf("create entry %v: %v", entry1.FullPath, err)
		return
	}

	entry, err := filer.FindEntry(fullpath)

	if err != nil {
		t.Errorf("find entry: %v", err)
		return
	}

	if entry.FullPath != entry1.FullPath {
		t.Errorf("find wrong entry: %v", entry.FullPath)
		return
	}

	// checking one upper directory
	entries, _ := filer.ListDirectoryEntries(filer2.FullPath("/home/chris/this/is/one"), "", false, 100)
	if len(entries) != 1 {
		t.Errorf("list entries count: %v", len(entries))
		return
	}

	// checking one upper directory
	entries, _ = filer.ListDirectoryEntries(filer2.FullPath("/"), "", false, 100)
	if len(entries) != 1 {
		t.Errorf("list entries count: %v", len(entries))
		return
	}

}

func TestEmptyRoot(t *testing.T) {
	filer := filer2.NewFiler(nil, nil)
	dir, _ := ioutil.TempDir("", "seaweedfs_filer_test2")
	defer os.RemoveAll(dir)
	store := &LevelDBStore{}
	store.initialize(dir)
	filer.SetStore(store)
	filer.DisableDirectoryCache()

	// checking one upper directory
	entries, err := filer.ListDirectoryEntries(filer2.FullPath("/"), "", false, 100)
	if err != nil {
		t.Errorf("list entries: %v", err)
		return
	}
	if len(entries) != 0 {
		t.Errorf("list entries count: %v", len(entries))
		return
	}

}
