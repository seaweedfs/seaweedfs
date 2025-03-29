package store_test

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestFilerStore(t *testing.T, store filer.FilerStore) {
	ctx := context.Background()

	store.InsertEntry(ctx, makeEntry(util.FullPath("/"), true))
	store.InsertEntry(ctx, makeEntry(util.FullPath("/a"), true))
	store.InsertEntry(ctx, makeEntry(util.FullPath("/a/b"), true))
	store.InsertEntry(ctx, makeEntry(util.FullPath("/a/b/c"), true))
	for i := 0; i < 2000; i++ {
		store.InsertEntry(ctx, makeEntry(util.FullPath(fmt.Sprintf("/a/b/c/f%05d", i)), false))
	}

	{
		var counter int
		lastFileName, err := store.ListDirectoryEntries(ctx, util.FullPath("/a/b/c"), "", false, 3, func(entry *filer.Entry) bool {
			counter++
			return true
		})
		assert.Nil(t, err, "list directory")
		assert.Equal(t, 3, counter, "directory list counter")
		assert.Equal(t, "f00002", lastFileName, "directory list last file")
		lastFileName, err = store.ListDirectoryEntries(ctx, util.FullPath("/a/b/c"), lastFileName, false, 1024, func(entry *filer.Entry) bool {
			counter++
			return true
		})
		assert.Nil(t, err, "list directory")
		assert.Equal(t, 1027, counter, "directory list counter")
		assert.Equal(t, "f01026", lastFileName, "directory list last file")
	}

	testKey := []byte("test_key")
	testValue1 := []byte("test_value1")
	testValue2 := []byte("test_value2")

	err := store.KvPut(ctx, testKey, testValue1)
	assert.Nil(t, err, "KV put")
	value, err := store.KvGet(ctx, testKey)
	assert.Equal(t, value, testValue1, "KV get")

	err = store.KvPut(ctx, testKey, testValue2)
	assert.Nil(t, err, "KV update")
	value, err = store.KvGet(ctx, testKey)
	assert.Equal(t, value, testValue2, "KV get after update")
}

func makeEntry(fullPath util.FullPath, isDirectory bool) *filer.Entry {
	var mode os.FileMode
	if isDirectory {
		mode = os.ModeDir
	}
	return &filer.Entry{
		FullPath: fullPath,
		Attr: filer.Attr{
			Mode: mode,
		},
	}
}
