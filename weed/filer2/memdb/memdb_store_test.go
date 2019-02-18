package memdb

import (
	"github.com/chrislusf/seaweedfs/weed/filer2"
	"testing"
)

func TestCreateAndFind(t *testing.T) {
	filer := filer2.NewFiler(nil, nil)
	store := &MemDbStore{}
	store.Initialize(nil)
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

}

func TestCreateFileAndList(t *testing.T) {
	filer := filer2.NewFiler(nil, nil)
	store := &MemDbStore{}
	store.Initialize(nil)
	filer.SetStore(store)
	filer.DisableDirectoryCache()

	entry1 := &filer2.Entry{
		FullPath: filer2.FullPath("/home/chris/this/is/one/file1.jpg"),
		Attr: filer2.Attr{
			Mode: 0440,
			Uid:  1234,
			Gid:  5678,
		},
	}

	entry2 := &filer2.Entry{
		FullPath: filer2.FullPath("/home/chris/this/is/one/file2.jpg"),
		Attr: filer2.Attr{
			Mode: 0440,
			Uid:  1234,
			Gid:  5678,
		},
	}

	filer.CreateEntry(entry1)
	filer.CreateEntry(entry2)

	// checking the 2 files
	entries, err := filer.ListDirectoryEntries(filer2.FullPath("/home/chris/this/is/one/"), "", false, 100)

	if err != nil {
		t.Errorf("list entries: %v", err)
		return
	}

	if len(entries) != 2 {
		t.Errorf("list entries count: %v", len(entries))
		return
	}

	if entries[0].FullPath != entry1.FullPath {
		t.Errorf("find wrong entry 1: %v", entries[0].FullPath)
		return
	}

	if entries[1].FullPath != entry2.FullPath {
		t.Errorf("find wrong entry 2: %v", entries[1].FullPath)
		return
	}

	// checking the offset
	entries, err = filer.ListDirectoryEntries(filer2.FullPath("/home/chris/this/is/one/"), "file1.jpg", false, 100)
	if len(entries) != 1 {
		t.Errorf("list entries count: %v", len(entries))
		return
	}

	// checking one upper directory
	entries, _ = filer.ListDirectoryEntries(filer2.FullPath("/home/chris/this/is"), "", false, 100)
	if len(entries) != 1 {
		t.Errorf("list entries count: %v", len(entries))
		return
	}

	// checking root directory
	entries, _ = filer.ListDirectoryEntries(filer2.FullPath("/"), "", false, 100)
	if len(entries) != 1 {
		t.Errorf("list entries count: %v", len(entries))
		return
	}

	// add file3
	file3Path := filer2.FullPath("/home/chris/this/is/file3.jpg")
	entry3 := &filer2.Entry{
		FullPath: file3Path,
		Attr: filer2.Attr{
			Mode: 0440,
			Uid:  1234,
			Gid:  5678,
		},
	}
	filer.CreateEntry(entry3)

	// checking one upper directory
	entries, _ = filer.ListDirectoryEntries(filer2.FullPath("/home/chris/this/is"), "", false, 100)
	if len(entries) != 2 {
		t.Errorf("list entries count: %v", len(entries))
		return
	}

	// delete file and count
	filer.DeleteEntryMetaAndData(file3Path, false, false)
	entries, _ = filer.ListDirectoryEntries(filer2.FullPath("/home/chris/this/is"), "", false, 100)
	if len(entries) != 1 {
		t.Errorf("list entries count: %v", len(entries))
		return
	}

}
