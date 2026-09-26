package filer

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
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
	assert.Equal(t, uint64(util.HashStringToLong(string(hardLinkId))), a.Attr.Inode)
	assert.Equal(t, a.Attr.Inode, b.Attr.Inode)
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

func TestCreateEntryOExclPreservesExistingEntry(t *testing.T) {
	f, store := newTestFilerWithStubStore()

	original := &Entry{
		FullPath: util.FullPath("/buckets/my-bucket"),
		Attr:     Attr{Mode: os.ModeDir | 0o777},
		Extended: map[string][]byte{
			"lifecycle": []byte(`<LifecycleConfiguration/>`),
			"owner":     []byte("alice"),
		},
	}
	require.NoError(t, store.InsertEntry(context.Background(), original))

	replacement := &Entry{
		FullPath: util.FullPath("/buckets/my-bucket"),
		Attr:     Attr{Mode: os.ModeDir | 0o777},
	}
	err := f.CreateEntry(context.Background(), replacement, original, true, false, nil, false, f.MaxFilenameLength)
	require.ErrorIs(t, err, filer_pb.ErrEntryAlreadyExists)

	stored, findErr := store.FindEntry(context.Background(), original.FullPath)
	require.NoError(t, findErr)
	assert.Equal(t, original.Extended, stored.Extended)
}

func TestCreateEntryOExclFailsOnLookupError(t *testing.T) {
	f, store := newTestFilerWithStubStore()

	original := &Entry{
		FullPath: util.FullPath("/buckets/my-bucket"),
		Attr:     Attr{Mode: os.ModeDir | 0o777},
		Extended: map[string][]byte{"owner": []byte("alice")},
	}
	require.NoError(t, store.InsertEntry(context.Background(), original))

	// a failed lookup must not masquerade as "not found": without the check
	// the insert path would upsert over the stored bucket entry
	store.findErr = errors.New("transient store failure")
	err := f.CreateEntry(context.Background(), &Entry{
		FullPath: util.FullPath("/buckets/my-bucket"),
		Attr:     Attr{Mode: os.ModeDir | 0o777},
	}, nil, true, false, nil, false, f.MaxFilenameLength)
	require.Error(t, err)
	assert.NotErrorIs(t, err, filer_pb.ErrEntryAlreadyExists)

	store.findErr = nil
	stored, findErr := store.FindEntry(context.Background(), original.FullPath)
	require.NoError(t, findErr)
	assert.Equal(t, original.Extended, stored.Extended)
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
