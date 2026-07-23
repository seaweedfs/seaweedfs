package filer

import (
	"context"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// countInodeIndexRows counts KV rows whose key carries the inode→path index
// prefix. With index writes disabled it must always be 0.
func countInodeIndexRows(s *stubFilerStore) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for k := range s.kv {
		if strings.HasPrefix(k, inodeIndexKeyPrefix) {
			n++
		}
	}
	return n
}

// restoreInodeIndexPrefixes restores the package-global prefix filter after a
// test mutates it, so tests cannot leak scoping state into each other.
func restoreInodeIndexPrefixes(t *testing.T) {
	t.Cleanup(func() { inodeIndexPrefixes = nil })
}

func TestSetInodeIndexPrefixesDedupsAndTrims(t *testing.T) {
	restoreInodeIndexPrefixes(t)

	SetInodeIndexPrefixes([]string{"/exports", " /exports ", "", "  ", "/data"})
	// sorted, trimmed, empties dropped
	assert.Equal(t, []string{"/data", "/exports"}, inodeIndexPrefixes)
	assert.True(t, inodeIndexEnabled())

	SetInodeIndexPrefixes(nil)
	assert.Nil(t, inodeIndexPrefixes)
	assert.False(t, inodeIndexEnabled())

	SetInodeIndexPrefixes([]string{"", "  "})
	assert.False(t, inodeIndexEnabled(), "only-blank input must disable the index")
}

func TestPathInInodeIndexScope(t *testing.T) {
	restoreInodeIndexPrefixes(t)

	// Empty filter: nothing is in scope, matching upstream 4.38 behaviour.
	SetInodeIndexPrefixes(nil)
	assert.False(t, pathInInodeIndexScope("/exports/a"))
	assert.False(t, pathInInodeIndexScope("/"))

	SetInodeIndexPrefixes([]string{"/exports", "/data"})
	for _, p := range []string{
		"/exports",
		"/exports/",
		"/exports/docs/big.bin",
		"/data",
		"/data/x/y",
	} {
		assert.True(t, pathInInodeIndexScope(util.FullPath(p)), "expected in scope: %s", p)
	}
	for _, p := range []string{
		"/buckets/s3-object", // outside any prefix → must NOT be indexed
		"/exportsx",           // shares a stem but is a different path
		"/",
	} {
		assert.False(t, pathInInodeIndexScope(util.FullPath(p)), "expected out of scope: %s", p)
	}
}

// TestInodeIndexDisabledWritesZeroRows is the hard regression guard for the
// opt-in guarantee: with the default (empty) flag, NONE of the wrapper write
// paths may produce an inode→path index row — behaviour identical to upstream
// 4.38, which has no index at all.
func TestInodeIndexDisabledWritesZeroRows(t *testing.T) {
	restoreInodeIndexPrefixes(t)
	SetInodeIndexPrefixes(nil) // default: disabled

	store := newStubFilerStore()
	wrapper := NewFilerStoreWrapper(store)
	ctx := context.Background()

	inScopedEntry := &Entry{
		FullPath: util.FullPath("/exports/a"),
		Attr:     Attr{Mode: 0o644, Inode: 1001},
	}
	outScopedEntry := &Entry{
		FullPath: util.FullPath("/buckets/s3-obj"),
		Attr:     Attr{Mode: 0o644, Inode: 1002},
	}

	require.NoError(t, wrapper.InsertEntry(ctx, inScopedEntry.ShallowClone()))
	require.NoError(t, wrapper.InsertEntryKnownAbsent(ctx, outScopedEntry.ShallowClone()))
	require.NoError(t, wrapper.UpdateEntry(ctx, inScopedEntry.ShallowClone()))
	require.NoError(t, wrapper.KvPut(ctx, []byte("unrelated"), []byte("v")))

	// Zero index rows despite inserts/updates with non-zero inodes.
	assert.Equal(t, 0, countInodeIndexRows(store), "disabled index must write zero rows after inserts/updates")

	// Removal paths must also touch no index KV.
	require.NoError(t, wrapper.DeleteOneEntry(ctx, inScopedEntry.ShallowClone()))
	require.NoError(t, wrapper.DeleteEntry(ctx, outScopedEntry.FullPath))
	require.NoError(t, wrapper.InsertEntry(ctx, &Entry{
		FullPath: util.FullPath("/exports/sub/child"),
		Attr:     Attr{Mode: 0o644, Inode: 1003},
	}))
	require.NoError(t, wrapper.DeleteFolderChildren(ctx, "/exports/sub"))
	assert.Equal(t, 0, countInodeIndexRows(store), "disabled index must write zero rows through deletes")
}

// TestInodeIndexScopedWritesOnlyInScopeRows verifies that once a prefix is set,
// only entries under that prefix get index rows and the rest of the namespace
// (e.g. S3 buckets) stays index-free.
func TestInodeIndexScopedWritesOnlyInScopeRows(t *testing.T) {
	restoreInodeIndexPrefixes(t)
	SetInodeIndexPrefixes([]string{"/exports"})

	store := newStubFilerStore()
	wrapper := NewFilerStoreWrapper(store)
	ctx := context.Background()

	scoped := &Entry{
		FullPath: util.FullPath("/exports/docs/file"),
		Attr:     Attr{Mode: 0o644, Inode: 2001},
	}
	s3obj := &Entry{
		FullPath: util.FullPath("/buckets/s3-object"),
		Attr:     Attr{Mode: 0o644, Inode: 2002},
	}

	require.NoError(t, wrapper.InsertEntry(ctx, scoped.ShallowClone()))
	require.NoError(t, wrapper.InsertEntry(ctx, s3obj.ShallowClone()))

	// Exactly one index row: the scoped entry. The S3 object must NOT be indexed.
	assert.Equal(t, 1, countInodeIndexRows(store))
	rec, err := store.KvGet(ctx, InodeIndexKey(2001))
	require.NoError(t, err)
	decoded, err := DecodeInodeIndexRecord(rec)
	require.NoError(t, err)
	assert.Equal(t, util.FullPath("/exports/docs/file"), decoded.CanonicalPath())

	_, err = store.KvGet(ctx, InodeIndexKey(2002))
	assert.ErrorIs(t, err, ErrKvNotFound, "out-of-scope entry must have no index row")

	// Deleting the scoped entry removes its index row.
	require.NoError(t, wrapper.DeleteEntry(ctx, scoped.FullPath))
	assert.Equal(t, 0, countInodeIndexRows(store))
}
