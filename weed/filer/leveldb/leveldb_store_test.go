package leveldb

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func TestCreateAndFind(t *testing.T) {
	testFiler := filer.NewFiler(pb.ServerDiscovery{}, nil, "", "", "", "", "", 255, nil)
	dir := t.TempDir()
	store := &LevelDBStore{}
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

	if err := testFiler.CreateEntry(ctx, entry1, nil, false, false, nil, false, testFiler.MaxFilenameLength); err != nil {
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
	testFiler := filer.NewFiler(pb.ServerDiscovery{}, nil, "", "", "", "", "", 255, nil)
	dir := t.TempDir()
	store := &LevelDBStore{}
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

func BenchmarkInsertEntry(b *testing.B) {
	testFiler := filer.NewFiler(pb.ServerDiscovery{}, nil, "", "", "", "", "", 255, nil)
	dir := b.TempDir()
	store := &LevelDBStore{}
	store.initialize(dir)
	testFiler.SetStore(store)

	ctx := context.Background()

	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		entry := &filer.Entry{
			FullPath: util.FullPath(fmt.Sprintf("/file%d.txt", i)),
			Attr: filer.Attr{
				Crtime: time.Now(),
				Mtime:  time.Now(),
				Mode:   os.FileMode(0644),
			},
		}
		store.InsertEntry(ctx, entry)
	}
}

func TestListDirectoryPrefixedEntries_StartBelowPrefix(t *testing.T) {
	store := &LevelDBStore{}
	store.initialize(t.TempDir())
	defer store.Shutdown()

	ctx := context.Background()
	for _, name := range []string{"item-a", "item-a.versions", "item-b.versions"} {
		if err := store.InsertEntry(ctx, &filer.Entry{FullPath: util.FullPath("/d/" + name)}); err != nil {
			t.Fatalf("insert %s: %v", name, err)
		}
	}

	var names []string
	_, err := store.ListDirectoryPrefixedEntries(ctx, util.FullPath("/d"), "item", true, 100, "item-b", func(entry *filer.Entry) (bool, error) {
		names = append(names, entry.Name())
		return true, nil
	})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(names) != 1 || names[0] != "item-b.versions" {
		t.Fatalf("want [item-b.versions], got %v", names)
	}
}
