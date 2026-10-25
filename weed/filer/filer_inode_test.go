package filer

import (
	"context"
	"errors"
	"fmt"
	"math"
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
	assert.Equal(t, util.NormalizeInode(uint64(util.HashStringToLong(string(hardLinkId)))), a.Attr.Inode)
	assert.Equal(t, a.Attr.Inode, b.Attr.Inode)
}

// TestEnsureEntryInodeFitsSignedLong pins the invariant that every generated
// inode is storable: the filer hands Attr.Inode to the backing store verbatim,
// and the Elasticsearch store indexes it as a signed `long`. Roughly half of
// the unsigned hash space sits above math.MaxInt64, so a store that rejects
// those values used to fail the metadata write for half of all entries.
func TestEnsureEntryInodeFitsSignedLong(t *testing.T) {
	f := &Filer{}
	crtime := time.Unix(1700000000, 0)

	seen := make(map[uint64]string)
	for i := 0; i < 5000; i++ {
		fullPath := util.FullPath(fmt.Sprintf("/topics/.system/log/2026-10-02/entry-%d", i))
		entry := &Entry{FullPath: fullPath, Attr: Attr{Crtime: crtime}}
		f.ensureEntryInode(entry)

		if entry.Attr.Inode > math.MaxInt64 {
			t.Fatalf("ensureEntryInode(%q) = %d, above math.MaxInt64", fullPath, entry.Attr.Inode)
		}
		// Folding must not collapse distinct paths onto one inode.
		if other, ok := seen[entry.Attr.Inode]; ok {
			t.Fatalf("ensureEntryInode(%q) collided with %q on inode %d", fullPath, other, entry.Attr.Inode)
		}
		seen[entry.Attr.Inode] = string(fullPath)
	}
}

// TestEnsureEntryInodeHardLinkFitsSignedLong covers the hard-link branch, which
// hashes HardLinkId instead of the path and so has no path-derived crtime term.
func TestEnsureEntryInodeHardLinkFitsSignedLong(t *testing.T) {
	f := &Filer{}
	crtime := time.Unix(1700000000, 0)

	for i := 0; i < 5000; i++ {
		entry := &Entry{
			FullPath:   util.FullPath(fmt.Sprintf("/links/target-%d.txt", i)),
			Attr:       Attr{Crtime: crtime},
			HardLinkId: NewHardLinkId(),
		}
		f.ensureEntryInode(entry)

		if entry.Attr.Inode > math.MaxInt64 {
			t.Fatalf("ensureEntryInode(%q) = %d, above math.MaxInt64", entry.FullPath, entry.Attr.Inode)
		}
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

	err := f.UpdateEntry(context.Background(), original, updated, false)
	require.Error(t, err)

	updated = &Entry{
		FullPath: util.FullPath("/doc.txt"),
		Attr: Attr{
			Mode: 0o600,
		},
	}
	err = f.UpdateEntry(context.Background(), original, updated, false)
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
	err := f.UpdateEntry(context.Background(), original, updated, false)
	require.NoError(t, err)

	stored, findErr := store.FindEntry(context.Background(), original.FullPath)
	require.NoError(t, findErr)
	require.NotNil(t, stored)
	assert.NotZero(t, stored.Attr.Inode)
	assert.NotEqual(t, uint64(1), stored.Attr.Inode)
}

// A transient store error must fail the write instead of being mistaken for a
// missing entry: on upsert stores proceeding would replace the entry without
// loading the old chunks, orphaning them from cleanup.
func TestCreateEntryFailsWhenLookupErrors(t *testing.T) {
	f, store := newTestFilerWithStubStore()
	path := util.FullPath("/dir/file.txt")
	require.NoError(t, store.InsertEntry(context.Background(), &Entry{
		FullPath: path,
		Content:  []byte("existing"),
	}))
	store.findErr = errors.New("transient store failure")

	entry := &Entry{
		FullPath: path,
		Attr:     Attr{Mode: 0o644},
		Content:  []byte("overwrite"),
	}
	require.Error(t, f.CreateEntry(context.Background(), entry, nil, false, false, nil, false, f.MaxFilenameLength))

	store.findErr = nil
	stored, findErr := store.FindEntry(context.Background(), path)
	require.NoError(t, findErr)
	assert.Equal(t, "existing", string(stored.Content))
}
