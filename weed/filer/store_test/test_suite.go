package store_test

import (
	"context"
	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/util"
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
	store.InsertEntry(ctx, makeEntry(util.FullPath("/a/b/c/f1"), false))
	store.InsertEntry(ctx, makeEntry(util.FullPath("/a/b/c/f2"), false))
	store.InsertEntry(ctx, makeEntry(util.FullPath("/a/b/c/f3"), false))
	store.InsertEntry(ctx, makeEntry(util.FullPath("/a/b/c/f4"), false))
	store.InsertEntry(ctx, makeEntry(util.FullPath("/a/b/c/f5"), false))

	{
		var counter int
		lastFileName, err := store.ListDirectoryEntries(ctx, util.FullPath("/a/b/c"), "", false, 3, func(entry *filer.Entry) bool {
			counter++
			return true
		})
		if err != nil {
			t.Errorf("list directory: %v", err)
		}
		if counter != 3 {
			assert.Equal(t, 3, counter, "directory list counter")
		}
		if lastFileName != "f3" {
			assert.Equal(t, "f3", lastFileName, "directory list last file")
		}
		lastFileName, err = store.ListDirectoryEntries(ctx, util.FullPath("/a/b/c"), lastFileName, false, 3, func(entry *filer.Entry) bool {
			counter++
			return true
		})
		if err != nil {
			t.Errorf("list directory: %v", err)
		}
		if counter != 5 {
			assert.Equal(t, 5, counter, "directory list counter")
		}
		if lastFileName != "f5" {
			assert.Equal(t, "f5", lastFileName, "directory list last file")
		}
	}

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
