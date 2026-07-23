package filer

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func inodeIndexConf(prefixes ...string) *FilerConf {
	fc := NewFilerConf()
	for _, prefix := range prefixes {
		_ = fc.SetLocationConf(&filer_pb.FilerConf_PathConf{LocationPrefix: prefix, InodeIndex: true})
	}
	return fc
}

func newInodeIndexWrapper(prefixes ...string) (*FilerStoreWrapper, *stubFilerStore) {
	store := newStubFilerStore()
	wrapper := NewFilerStoreWrapper(store)
	fc := inodeIndexConf(prefixes...)
	wrapper.filerConfFn = func() *FilerConf { return fc }
	return wrapper, store
}

func countInodeIndexRows(store *stubFilerStore) int {
	store.mu.Lock()
	defer store.mu.Unlock()
	count := 0
	for key := range store.kv {
		if strings.HasPrefix(key, inodeIndexKeyPrefix) {
			count++
		}
	}
	return count
}

func TestFilerStoreWrapperMaintainsInodeIndexLifecycle(t *testing.T) {
	wrapper, _ := newInodeIndexWrapper("/")
	ctx := context.Background()

	created := &Entry{
		FullPath: util.FullPath("/docs/report.txt"),
		Attr: Attr{
			Mode:  0o644,
			Inode: 42,
		},
	}

	require.NoError(t, wrapper.InsertEntry(ctx, created))
	path, err := wrapper.lookupInodePath(ctx, created.Attr.Inode)
	require.NoError(t, err)
	assert.Equal(t, created.FullPath, path)
	paths, err := wrapper.lookupInodePaths(ctx, created.Attr.Inode)
	require.NoError(t, err)
	assert.Equal(t, []util.FullPath{created.FullPath}, paths)
	record, err := wrapper.lookupInodeIndex(ctx, created.Attr.Inode)
	require.NoError(t, err)
	assert.Equal(t, InodeIndexInitialGeneration, record.Generation)

	updated := &Entry{
		FullPath: util.FullPath("/docs/report.txt"),
		Attr: Attr{
			Mode:  0o600,
			Inode: 42,
		},
	}
	require.NoError(t, wrapper.UpdateEntry(ctx, updated))
	path, err = wrapper.lookupInodePath(ctx, updated.Attr.Inode)
	require.NoError(t, err)
	assert.Equal(t, updated.FullPath, path)

	require.NoError(t, wrapper.DeleteEntry(ctx, created.FullPath))
	_, err = wrapper.lookupInodePath(ctx, created.Attr.Inode)
	require.ErrorIs(t, err, ErrKvNotFound)
}

func TestFilerStoreWrapperMaintainsMultiplePathsPerInode(t *testing.T) {
	wrapper, _ := newInodeIndexWrapper("/")
	ctx := context.Background()
	inode := uint64(88)
	hardLinkId := NewHardLinkId()

	require.NoError(t, wrapper.InsertEntry(ctx, &Entry{
		FullPath: util.FullPath("/links/b.txt"),
		Attr: Attr{
			Mode:  0o644,
			Inode: inode,
		},
		HardLinkId:      hardLinkId,
		HardLinkCounter: 2,
	}))
	require.NoError(t, wrapper.InsertEntry(ctx, &Entry{
		FullPath: util.FullPath("/links/a.txt"),
		Attr: Attr{
			Mode:  0o644,
			Inode: inode,
		},
		HardLinkId:      hardLinkId,
		HardLinkCounter: 2,
	}))

	paths, err := wrapper.lookupInodePaths(ctx, inode)
	require.NoError(t, err)
	assert.Equal(t, []util.FullPath{"/links/a.txt", "/links/b.txt"}, paths)
	record, err := wrapper.lookupInodeIndex(ctx, inode)
	require.NoError(t, err)
	assert.Equal(t, InodeIndexInitialGeneration, record.Generation)

	path, err := wrapper.lookupInodePath(ctx, inode)
	require.NoError(t, err)
	assert.Equal(t, util.FullPath("/links/a.txt"), path)

	require.NoError(t, wrapper.DeleteEntry(ctx, util.FullPath("/links/a.txt")))

	paths, err = wrapper.lookupInodePaths(ctx, inode)
	require.NoError(t, err)
	assert.Equal(t, []util.FullPath{"/links/b.txt"}, paths)

	path, err = wrapper.lookupInodePath(ctx, inode)
	require.NoError(t, err)
	assert.Equal(t, util.FullPath("/links/b.txt"), path)
}

func TestFilerStoreWrapperUpgradesLegacySinglePathInodeIndexRecords(t *testing.T) {
	wrapper := NewFilerStoreWrapper(newStubFilerStore())
	ctx := context.Background()
	inode := uint64(91)

	require.NoError(t, wrapper.KvPut(ctx, InodeIndexKey(inode), []byte("/legacy/path.txt")))

	path, err := wrapper.lookupInodePath(ctx, inode)
	require.NoError(t, err)
	assert.Equal(t, util.FullPath("/legacy/path.txt"), path)

	paths, err := wrapper.lookupInodePaths(ctx, inode)
	require.NoError(t, err)
	assert.Equal(t, []util.FullPath{"/legacy/path.txt"}, paths)

	require.NoError(t, wrapper.storeInodeIndex(ctx, util.FullPath("/legacy/second.txt"), inode))

	paths, err = wrapper.lookupInodePaths(ctx, inode)
	require.NoError(t, err)
	assert.Equal(t, []util.FullPath{"/legacy/path.txt", "/legacy/second.txt"}, paths)

	value, err := wrapper.KvGet(ctx, InodeIndexKey(inode))
	require.NoError(t, err)
	assert.JSONEq(t, `{"generation":1,"paths":["/legacy/path.txt","/legacy/second.txt"]}`, string(value))
}

func TestFilerStoreWrapperKeepsInodeIndexWhenDeleteArrivesAfterRenameInsert(t *testing.T) {
	wrapper, _ := newInodeIndexWrapper("/")
	ctx := context.Background()
	inode := uint64(77)

	require.NoError(t, wrapper.InsertEntry(ctx, &Entry{
		FullPath: util.FullPath("/old/name.txt"),
		Attr: Attr{
			Mode:  0o644,
			Inode: inode,
		},
	}))
	require.NoError(t, wrapper.InsertEntry(ctx, &Entry{
		FullPath: util.FullPath("/new/name.txt"),
		Attr: Attr{
			Mode:  0o644,
			Inode: inode,
		},
	}))
	require.NoError(t, wrapper.DeleteEntry(ctx, util.FullPath("/old/name.txt")))

	path, err := wrapper.lookupInodePath(ctx, inode)
	require.NoError(t, err)
	assert.Equal(t, util.FullPath("/new/name.txt"), path)

	paths, err := wrapper.lookupInodePaths(ctx, inode)
	require.NoError(t, err)
	assert.Equal(t, []util.FullPath{"/new/name.txt"}, paths)
}

func TestInodeIndexDisabledWritesNothing(t *testing.T) {
	// No filer.conf wired at all, and a conf with no inode_index rules: both
	// must leave the KV store free of index rows.
	t.Run("no conf", func(t *testing.T) {
		store := newStubFilerStore()
		runInodeIndexDisabledScenario(t, NewFilerStoreWrapper(store), store)
	})
	t.Run("conf without rules", func(t *testing.T) {
		wrapper, store := newInodeIndexWrapper()
		runInodeIndexDisabledScenario(t, wrapper, store)
	})
}

func runInodeIndexDisabledScenario(t *testing.T, wrapper *FilerStoreWrapper, store *stubFilerStore) {
	ctx := context.Background()
	entry := &Entry{
		FullPath: util.FullPath("/docs/report.txt"),
		Attr: Attr{
			Mode:  0o644,
			Inode: 42,
		},
	}
	require.NoError(t, wrapper.InsertEntry(ctx, entry))
	require.NoError(t, wrapper.UpdateEntry(ctx, entry))
	assert.Zero(t, countInodeIndexRows(store))
	require.NoError(t, wrapper.DeleteEntry(ctx, entry.FullPath))
	assert.Zero(t, countInodeIndexRows(store))
}

func TestInodeIndexScopedToConfiguredPrefixes(t *testing.T) {
	wrapper, store := newInodeIndexWrapper("/exports")
	ctx := context.Background()

	require.NoError(t, wrapper.InsertEntry(ctx, &Entry{
		FullPath: util.FullPath("/exports/docs/in.txt"),
		Attr:     Attr{Mode: 0o644, Inode: 1001},
	}))
	require.NoError(t, wrapper.InsertEntry(ctx, &Entry{
		FullPath: util.FullPath("/other/out.txt"),
		Attr:     Attr{Mode: 0o644, Inode: 1002},
	}))

	assert.Equal(t, 1, countInodeIndexRows(store))
	path, err := wrapper.lookupInodePath(ctx, 1001)
	require.NoError(t, err)
	assert.Equal(t, util.FullPath("/exports/docs/in.txt"), path)
	_, err = wrapper.lookupInodePath(ctx, 1002)
	require.ErrorIs(t, err, ErrKvNotFound)

	require.NoError(t, wrapper.InsertEntry(ctx, &Entry{
		FullPath: util.FullPath("/exports/docs"),
		Attr:     Attr{Mode: os.ModeDir | 0o755, Inode: 1000},
	}))
	require.NoError(t, wrapper.DeleteFolderChildren(ctx, util.FullPath("/exports")))
	_, err = wrapper.lookupInodePath(ctx, 1001)
	require.ErrorIs(t, err, ErrKvNotFound)
}

func TestRecursiveDeleteRemovesDescendantInodeIndexes(t *testing.T) {
	f, _ := newTestFilerWithStubStore()
	fc := inodeIndexConf("/tree")
	f.Store.(*FilerStoreWrapper).filerConfFn = func() *FilerConf { return fc }
	ctx := context.Background()

	entries := []*Entry{
		{
			FullPath: util.FullPath("/tree"),
			Attr: Attr{
				Mode:  os.ModeDir | 0o755,
				Inode: 100,
			},
		},
		{
			FullPath: util.FullPath("/tree/file.txt"),
			Attr: Attr{
				Mode:  0o644,
				Inode: 101,
			},
		},
		{
			FullPath: util.FullPath("/tree/subdir"),
			Attr: Attr{
				Mode:  os.ModeDir | 0o755,
				Inode: 102,
			},
		},
		{
			FullPath: util.FullPath("/tree/subdir/nested.txt"),
			Attr: Attr{
				Mode:  0o644,
				Inode: 103,
			},
		},
	}

	for _, entry := range entries {
		require.NoError(t, f.Store.InsertEntry(ctx, entry))
	}

	require.NoError(t, f.DeleteEntryMetaAndData(ctx, util.FullPath("/tree"), true, false, false, false, nil, 0))

	for _, inode := range []uint64{100, 101, 102, 103} {
		_, err := f.Store.(*FilerStoreWrapper).lookupInodePath(ctx, inode)
		require.ErrorIs(t, err, ErrKvNotFound)
	}
}

func TestInodeIndexActiveUnder(t *testing.T) {
	fc := inodeIndexConf("/exports")
	assert.True(t, fc.InodeIndexActiveUnder("/"))
	assert.True(t, fc.InodeIndexActiveUnder("/exports"))
	assert.True(t, fc.InodeIndexActiveUnder("/exports/sub"))
	assert.False(t, fc.InodeIndexActiveUnder("/other"))

	assert.False(t, NewFilerConf().InodeIndexActiveUnder("/"))
}
