package filer

import (
	"context"
	"errors"
	"os"
	"sync/atomic"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// missHookStore runs onMiss once, outside the store lock, right after a lookup
// of path finds nothing.
type missHookStore struct {
	*stubFilerStore
	path   util.FullPath
	fired  atomic.Bool
	onMiss func()
}

func (s *missHookStore) FindEntry(ctx context.Context, p util.FullPath) (*Entry, error) {
	e, err := s.stubFilerStore.FindEntry(ctx, p)
	if p == s.path && errors.Is(err, filer_pb.ErrNotFound) && s.fired.CompareAndSwap(false, true) {
		s.onMiss()
	}
	return e, err
}

// A directory created while a nested create is filling in that directory as
// a missing parent keeps what it was created with: the parent row is only
// written where none exists.
func TestImplicitParentKeepsConcurrentDirectory(t *testing.T) {
	f, stub := newTestFilerWithStubStore(t)
	ctx := context.Background()
	dirPath := util.FullPath("/buckets/bkt/markers/dir")

	store := &missHookStore{stubFilerStore: stub, path: dirPath}
	f.Store = NewFilerStoreWrapper(store)
	store.onMiss = func() {
		marker := &Entry{
			FullPath: dirPath,
			Attr:     Attr{Mode: os.ModeDir | 0o755, Mime: s3_constants.FolderMimeType},
			Extended: map[string][]byte{s3_constants.ExtETagKey: []byte("d41d8cd98f00b204e9800998ecf8427e")},
		}
		require.NoError(t, f.CreateEntry(ctx, marker, nil, false, false, nil, false, f.MaxFilenameLength))
	}

	child := &Entry{FullPath: dirPath.Child("child"), Attr: Attr{Mode: 0o644}}
	require.NoError(t, f.CreateEntry(ctx, child, nil, false, false, nil, false, f.MaxFilenameLength))

	dir, err := stub.FindEntry(ctx, dirPath)
	require.NoError(t, err)
	assert.True(t, dir.IsDirectory())
	assert.Equal(t, s3_constants.FolderMimeType, dir.Attr.Mime, "the implicit parent replaced the directory created meanwhile")
	assert.Contains(t, dir.Extended, s3_constants.ExtETagKey)
}
