package leveldb

import (
	"context"
	"io/ioutil"
	"os"
	"testing"

	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func TestCreateAndFind(t *testing.T) {
	filer := filer2.NewFiler(nil, nil, "", 0, "", "", nil)
	dir, _ := ioutil.TempDir("", "seaweedfs_filer_test")
	defer os.RemoveAll(dir)
	store := &LevelDB2Store{}
	store.initialize(dir, 2)
	filer.SetStore(store)
	filer.DisableDirectoryCache()

	fullpath := util.FullPath("/home/chris/this/is/one/file1.jpg")

	ctx := context.Background()

	entry1 := &filer2.Entry{
		FullPath: fullpath,
		Attr: filer2.Attr{
			Mode: 0440,
			Uid:  1234,
			Gid:  5678,
		},
	}

	if err := filer.CreateEntry(ctx, entry1, false); err != nil {
		t.Errorf("create entry %v: %v", entry1.FullPath, err)
		return
	}

	entry, err := filer.FindEntry(ctx, fullpath)

	if err != nil {
		t.Errorf("find entry: %v", err)
		return
	}

	if entry.FullPath != entry1.FullPath {
		t.Errorf("find wrong entry: %v", entry.FullPath)
		return
	}

	// checking one upper directory
	entries, _ := filer.ListDirectoryEntries(ctx, util.FullPath("/home/chris/this/is/one"), "", false, 100)
	if len(entries) != 1 {
		t.Errorf("list entries count: %v", len(entries))
		return
	}

	// checking one upper directory
	entries, _ = filer.ListDirectoryEntries(ctx, util.FullPath("/"), "", false, 100)
	if len(entries) != 1 {
		t.Errorf("list entries count: %v", len(entries))
		return
	}

}

func TestEmptyRoot(t *testing.T) {
	filer := filer2.NewFiler(nil, nil, "", 0, "", "", nil)
	dir, _ := ioutil.TempDir("", "seaweedfs_filer_test2")
	defer os.RemoveAll(dir)
	store := &LevelDB2Store{}
	store.initialize(dir, 2)
	filer.SetStore(store)
	filer.DisableDirectoryCache()

	ctx := context.Background()

	// checking one upper directory
	entries, err := filer.ListDirectoryEntries(ctx, util.FullPath("/"), "", false, 100)
	if err != nil {
		t.Errorf("list entries: %v", err)
		return
	}
	if len(entries) != 0 {
		t.Errorf("list entries count: %v", len(entries))
		return
	}

}
