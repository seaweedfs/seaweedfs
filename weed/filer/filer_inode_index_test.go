package filer

import (
	"context"
	"os"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFilerStoreWrapperMaintainsInodeIndexLifecycle(t *testing.T) {
	wrapper := NewFilerStoreWrapper(newStubFilerStore())
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
	wrapper := NewFilerStoreWrapper(newStubFilerStore())
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

	require.NoError(t, wrapper.KvPut(ctx, inodeIndexKey(inode), []byte("/legacy/path.txt")))

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

	value, err := wrapper.KvGet(ctx, inodeIndexKey(inode))
	require.NoError(t, err)
	assert.JSONEq(t, `{"paths":["/legacy/path.txt","/legacy/second.txt"]}`, string(value))
}

func TestFilerStoreWrapperKeepsInodeIndexWhenDeleteArrivesAfterRenameInsert(t *testing.T) {
	wrapper := NewFilerStoreWrapper(newStubFilerStore())
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

func TestRecursiveDeleteRemovesDescendantInodeIndexes(t *testing.T) {
	f, store := newTestFilerWithStubStore()
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

	for _, path := range []string{"/tree", "/tree/file.txt", "/tree/subdir", "/tree/subdir/nested.txt"} {
		_, err := store.FindEntry(ctx, util.FullPath(path))
		require.Error(t, err)
	}
}
