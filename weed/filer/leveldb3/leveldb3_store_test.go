package leveldb

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func TestCreateAndFind(t *testing.T) {
	testFiler := filer.NewFiler(pb.ServerDiscovery{}, nil, "", "", "", "", "", 255, nil)
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

type fakeConfiguration map[string]interface{}

func (c fakeConfiguration) GetString(key string) string {
	value, _ := c[key].(string)
	return value
}

func (c fakeConfiguration) GetBool(key string) bool {
	value, _ := c[key].(bool)
	return value
}

func (c fakeConfiguration) GetInt(key string) int {
	value, _ := c[key].(int)
	return value
}

func (c fakeConfiguration) GetStringSlice(key string) []string {
	value, _ := c[key].([]string)
	return value
}

func (c fakeConfiguration) SetDefault(key string, value interface{}) {
	if _, found := c[key]; !found {
		c[key] = value
	}
}

func TestInitializeOptions(t *testing.T) {
	store := &LevelDB3Store{}
	if err := store.Initialize(fakeConfiguration{
		"leveldb3.dir":                      t.TempDir(),
		"leveldb3.block_cache_size_mb":      64,
		"leveldb3.write_buffer_size_mb":     32,
		"leveldb3.block_size_kb":            16,
		"leveldb3.compaction_table_size_mb": 8,
	}, "leveldb3."); err != nil {
		t.Fatalf("initialize: %v", err)
	}
	defer store.Shutdown()

	for _, name := range []string{DEFAULT, "bucket-a"} {
		opts := store.buildOptions(name)
		if opts.BlockCacheCapacity != 64*1024*1024 {
			t.Errorf("%s block cache %d, want 64MiB", name, opts.BlockCacheCapacity)
		}
		if opts.WriteBuffer != 32*1024*1024 {
			t.Errorf("%s write buffer %d, want 32MiB", name, opts.WriteBuffer)
		}
		if opts.BlockSize != 16*1024 {
			t.Errorf("%s block size %d, want 16KiB", name, opts.BlockSize)
		}
		if opts.CompactionTableSize != 8*1024*1024 {
			t.Errorf("%s compaction table size %d, want 8MiB", name, opts.CompactionTableSize)
		}
	}
}

func TestInitializeDefaultOptions(t *testing.T) {
	store := &LevelDB3Store{}
	if err := store.Initialize(fakeConfiguration{
		"leveldb3.dir":                  t.TempDir(),
		"leveldb3.block_cache_size_mb":  -1,
		"leveldb3.write_buffer_size_mb": 0,
	}, "leveldb3."); err != nil {
		t.Fatalf("initialize: %v", err)
	}
	defer store.Shutdown()

	opts := store.buildOptions(DEFAULT)
	if opts.BlockCacheCapacity != 32*1024*1024 {
		t.Errorf("block cache %d, want the 32MiB default", opts.BlockCacheCapacity)
	}
	if opts.WriteBuffer != 16*1024*1024 {
		t.Errorf("write buffer %d, want the 16MiB default", opts.WriteBuffer)
	}

	bucketOpts := store.buildOptions("bucket-a")
	if bucketOpts.BlockCacheCapacity != 16*1024*1024 {
		t.Errorf("bucket block cache %d, want the 16MiB default", bucketOpts.BlockCacheCapacity)
	}
	if bucketOpts.WriteBuffer != 8*1024*1024 {
		t.Errorf("bucket write buffer %d, want the 8MiB default", bucketOpts.WriteBuffer)
	}
}

func TestInitializeRejectsOversizedOption(t *testing.T) {
	store := &LevelDB3Store{}
	err := store.Initialize(fakeConfiguration{
		"leveldb3.dir":                  t.TempDir(),
		"leveldb3.write_buffer_size_mb": math.MaxInt/(1<<20) + 1,
	}, "leveldb3.")
	if err == nil || !strings.Contains(err.Error(), "write_buffer_size_mb") {
		t.Fatalf("expected overflow rejection, got %v", err)
	}
}

func TestEmptyRoot(t *testing.T) {
	testFiler := filer.NewFiler(pb.ServerDiscovery{}, nil, "", "", "", "", "", 255, nil)
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

func TestListDirectoryPrefixedEntries_StartBelowPrefix(t *testing.T) {
	store := &LevelDB3Store{}
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

func TestBucketEventsStayInsideStoreDir(t *testing.T) {
	parent := t.TempDir()
	dir := filepath.Join(parent, "store")
	store := &LevelDB3Store{}
	if err := store.initialize(dir); err != nil {
		t.Fatal(err)
	}
	defer store.Shutdown()

	sentinel := filepath.Join(parent, "keep")
	if err := os.WriteFile(sentinel, []byte("keep"), 0644); err != nil {
		t.Fatal(err)
	}
	for _, bucket := range []string{"..", ".", "../outside", "", DEFAULT} {
		store.OnBucketCreation(bucket)
		store.OnBucketDeletion(bucket)
	}
	if _, err := os.Stat(sentinel); err != nil {
		t.Fatalf("bucket events removed %s: %v", sentinel, err)
	}
	if _, err := os.Stat(filepath.Join(parent, "outside")); !os.IsNotExist(err) {
		t.Fatalf("bucket event created a folder outside %s: %v", dir, err)
	}
	if _, err := os.Stat(filepath.Join(parent, "CURRENT")); !os.IsNotExist(err) {
		t.Fatalf("bucket event opened a db in %s: %v", parent, err)
	}
	if _, err := os.Stat(filepath.Join(dir, DEFAULT)); err != nil {
		t.Fatalf("default db folder missing: %v", err)
	}
	ctx := context.Background()
	if err := store.InsertEntry(ctx, &filer.Entry{FullPath: "/a/b"}); err != nil {
		t.Fatalf("default db unusable after bucket events: %v", err)
	}
}
