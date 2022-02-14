package leveldb

import (
	"context"
	"testing"

	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func TestCreateAndFind(t *testing.T) {
	testFiler := filer.NewFiler(nil, nil, "", "", "", "", nil)
	dir := t.TempDir()
	store := &LevelDB3Store{}
	store.initialize(dir)
	testFiler.SetStore(store)

	fullpath := util.FullPath("/home/chris/this/is/one/file1.jpg")

	ctx := context.Background()

	entry1 := &filer.Entry{
		FullPath: fullpath,
		Attr: filer.Attr{
			Mode: 0440,
			Uid:  1234,
			Gid:  5678,
		},
	}

	if err := testFiler.CreateEntry(ctx, entry1, false, false, nil); err != nil {
		t.Errorf("create entry %v: %v", entry1.FullPath, err)
		return
	}

	entry, err := testFiler.FindEntry(ctx, fullpath)

	if err != nil {
		t.Errorf("find entry: %v", err)
		return
	}

	if entry.FullPath != entry1.FullPath {
		t.Errorf("find wrong entry: %v", entry.FullPath)
		return
	}

	// checking one upper directory
	entries, _, _ := testFiler.ListDirectoryEntries(ctx, util.FullPath("/home/chris/this/is/one"), "", false, 100, "", "", "")
	if len(entries) != 1 {
		t.Errorf("list entries count: %v", len(entries))
		return
	}

	// checking one upper directory
	entries, _, _ = testFiler.ListDirectoryEntries(ctx, util.FullPath("/"), "", false, 100, "", "", "")
	if len(entries) != 1 {
		t.Errorf("list entries count: %v", len(entries))
		return
	}

}

func TestEmptyRoot(t *testing.T) {
	testFiler := filer.NewFiler(nil, nil, "", "", "", "", nil)
	dir := t.TempDir()
	store := &LevelDB3Store{}
	store.initialize(dir)
	testFiler.SetStore(store)

	ctx := context.Background()

	// checking one upper directory
	entries, _, err := testFiler.ListDirectoryEntries(ctx, util.FullPath("/"), "", false, 100, "", "", "")
	if err != nil {
		t.Errorf("list entries: %v", err)
		return
	}
	if len(entries) != 0 {
		t.Errorf("list entries count: %v", len(entries))
		return
	}

}
