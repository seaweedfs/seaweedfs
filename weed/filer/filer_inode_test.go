package filer

import (
	"context"
	"math"
	"os"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEnsureEntryInodeMatchesFuseDerivation(t *testing.T) {
	f := &Filer{}
	crtime := time.Unix(1700000000, 0)

	entry := &Entry{
		FullPath: util.FullPath("/dir/file.txt"),
		Attr:     Attr{Crtime: crtime},
	}
	f.ensureEntryInode(entry)

	// The filer stores exactly what the FUSE mount would compute for a
	// non-hard-linked entry, and it is deterministic across calls.
	assert.Equal(t, entry.FullPath.AsInode(crtime.Unix()), entry.Attr.Inode)
	again := &Entry{FullPath: entry.FullPath, Attr: Attr{Crtime: crtime}}
	f.ensureEntryInode(again)
	assert.Equal(t, entry.Attr.Inode, again.Attr.Inode)
}

func TestEnsureEntryInodeSharesAcrossHardLinks(t *testing.T) {
	f := &Filer{}
	hardLinkId := NewHardLinkId()

	a := &Entry{
		FullPath:   util.FullPath("/links/a.txt"),
		Attr:       Attr{Crtime: time.Unix(1700000000, 0)},
		HardLinkId: hardLinkId,
	}
	b := &Entry{
		FullPath:   util.FullPath("/links/b.txt"),
		Attr:       Attr{Crtime: time.Unix(1800000000, 0)},
		HardLinkId: hardLinkId,
	}
	f.ensureEntryInode(a)
	f.ensureEntryInode(b)

	// Every link to the same target resolves to one inode, independent of path
	// or creation time.
	expectedInode := uint64(util.HashStringToLong(string(hardLinkId))) & uint64(math.MaxInt64)
	if expectedInode == 0 {
		expectedInode = 1
	}
	assert.Equal(t, expectedInode, a.Attr.Inode)
	assert.Equal(t, a.Attr.Inode, b.Attr.Inode)
}

func TestEnsureEntryInodeFitsSignedLong(t *testing.T) {
	f := &Filer{}
	entries := []*Entry{
		{FullPath: util.FullPath("/topics/.system/log/2026-08-18/07-24.741c9e0e"), Attr: Attr{Crtime: time.Unix(1787038150, 0)}},
		{FullPath: util.FullPath("/hard-link"), Attr: Attr{Crtime: time.Unix(1787038150, 0)}, HardLinkId: HardLinkId{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
	}

	for _, entry := range entries {
		f.ensureEntryInode(entry)
		require.NotZero(t, entry.Attr.Inode)
		require.LessOrEqual(t, entry.Attr.Inode, uint64(math.MaxInt64))
	}
}

func newTestFilerWithStubStore() (*Filer, *stubFilerStore) {
	store := newStubFilerStore()
	f := NewFiler(pb.ServerDiscovery{}, nil, "", "", "", "", "", 255, nil)
	f.Store = NewFilerStoreWrapper(store)
	return f, store
}

func TestCreateEntryAssignsInodeWhenMissing(t *testing.T) {
	f, store := newTestFilerWithStubStore()

	entry := &Entry{
		FullPath: util.FullPath("/dir/file.txt"),
		Attr: Attr{
			Mode: 0o644,
		},
	}

	err := f.CreateEntry(context.Background(), entry, nil, false, false, nil, false, f.MaxFilenameLength)
	require.NoError(t, err)

	stored, findErr := store.FindEntry(context.Background(), entry.FullPath)
	require.NoError(t, findErr)
	require.NotNil(t, stored)
	assert.NotZero(t, stored.Attr.Inode)
	assert.NotEqual(t, uint64(1), stored.Attr.Inode)
}

func TestCreateEntryAssignsInodesToAutoCreatedParents(t *testing.T) {
	f, store := newTestFilerWithStubStore()

	entry := &Entry{
		FullPath: util.FullPath("/a/b/c.txt"),
		Attr: Attr{
			Mode: 0o644,
		},
	}

	err := f.CreateEntry(context.Background(), entry, nil, false, false, nil, false, f.MaxFilenameLength)
	require.NoError(t, err)

	for _, path := range []string{"/a", "/a/b", "/a/b/c.txt"} {
		stored, findErr := store.FindEntry(context.Background(), util.FullPath(path))
		require.NoError(t, findErr, path)
		require.NotNil(t, stored, path)
		assert.NotZero(t, stored.Attr.Inode, path)
	}
}

func TestUpdateEntryPreservesExistingInode(t *testing.T) {
	f, store := newTestFilerWithStubStore()

	original := &Entry{
		FullPath: util.FullPath("/doc.txt"),
		Attr: Attr{
			Mode:  0o644,
			Inode: 12345,
		},
	}
	require.NoError(t, store.InsertEntry(context.Background(), original))

	updated := &Entry{
		FullPath: util.FullPath("/doc.txt"),
		Attr: Attr{
			Mode: os.ModeDir | 0o755,
		},
	}

	err := f.UpdateEntry(context.Background(), original, updated)
	require.Error(t, err)

	updated = &Entry{
		FullPath: util.FullPath("/doc.txt"),
		Attr: Attr{
			Mode: 0o600,
		},
	}
	err = f.UpdateEntry(context.Background(), original, updated)
	require.NoError(t, err)

	stored, findErr := store.FindEntry(context.Background(), original.FullPath)
	require.NoError(t, findErr)
	require.NotNil(t, stored)
	assert.Equal(t, uint64(12345), stored.Attr.Inode)
}

func TestUpdateEntryBackfillsMissingLegacyInode(t *testing.T) {
	f, store := newTestFilerWithStubStore()

	original := &Entry{
		FullPath: util.FullPath("/legacy.txt"),
		Attr: Attr{
			Mode: 0o644,
		},
	}
	require.NoError(t, store.InsertEntry(context.Background(), original))

	updated := &Entry{
		FullPath: util.FullPath("/legacy.txt"),
		Attr: Attr{
			Mode: 0o640,
		},
	}
	err := f.UpdateEntry(context.Background(), original, updated)
	require.NoError(t, err)

	stored, findErr := store.FindEntry(context.Background(), original.FullPath)
	require.NoError(t, findErr)
	require.NotNil(t, stored)
	assert.NotZero(t, stored.Attr.Inode)
	assert.NotEqual(t, uint64(1), stored.Attr.Inode)
}
