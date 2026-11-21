package filer

import (
	"context"
	"errors"
	"os"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// missHookStore runs onMiss, outside the store lock, right after the miss-th
// lookup of path that finds nothing.
type missHookStore struct {
	*stubFilerStore
	path   util.FullPath
	miss   int32
	misses atomic.Int32
	onMiss func()
}

func (s *missHookStore) FindEntry(ctx context.Context, p util.FullPath) (*Entry, error) {
	e, err := s.stubFilerStore.FindEntry(ctx, p)
	if p == s.path && errors.Is(err, filer_pb.ErrNotFound) && s.misses.Add(1) == s.miss {
		s.onMiss()
	}
	return e, err
}

func directoryMarker(p util.FullPath) *Entry {
	return &Entry{
		FullPath: p,
		Attr:     Attr{Mode: os.ModeDir | 0o755, Mime: s3_constants.FolderMimeType},
		Extended: map[string][]byte{s3_constants.ExtETagKey: []byte("d41d8cd98f00b204e9800998ecf8427e")},
	}
}

func assertDirectoryMarker(t *testing.T, store *stubFilerStore, p util.FullPath) {
	t.Helper()
	dir, err := store.FindEntry(context.Background(), p)
	require.NoError(t, err)
	assert.True(t, dir.IsDirectory())
	assert.Equal(t, s3_constants.FolderMimeType, dir.Attr.Mime, "the implicit parent replaced the directory created meanwhile")
	assert.Contains(t, dir.Extended, s3_constants.ExtETagKey)
}

// A directory created while a nested create is filling in that directory as
// a missing parent keeps what it was created with: the parent row is only
// written where none exists.
func TestImplicitParentKeepsConcurrentDirectory(t *testing.T) {
	f, stub := newTestFilerWithStubStore(t)
	ctx := context.Background()
	dirPath := util.FullPath("/buckets/bkt/markers/dir")

	store := &missHookStore{stubFilerStore: stub, path: dirPath, miss: 1}
	f.Store = NewFilerStoreWrapper(store)
	store.onMiss = func() {
		require.NoError(t, f.CreateEntry(ctx, directoryMarker(dirPath), nil, false, false, nil, false, f.MaxFilenameLength))
	}

	child := &Entry{FullPath: dirPath.Child("child"), Attr: Attr{Mode: 0o644}}
	require.NoError(t, f.CreateEntry(ctx, child, nil, false, false, nil, false, f.MaxFilenameLength))

	assertDirectoryMarker(t, stub, dirPath)
}

// A directory create arriving after the parent's re-check, before its insert,
// waits for that insert and then updates the row rather than being replaced.
func TestDirectoryCreateWaitsForImplicitParentInsert(t *testing.T) {
	f, stub := newTestFilerWithStubStore(t)
	ctx := context.Background()
	dirPath := util.FullPath("/buckets/bkt/markers/dir")

	markerDone := make(chan error, 1)
	store := &missHookStore{stubFilerStore: stub, path: dirPath, miss: 2}
	f.Store = NewFilerStoreWrapper(store)
	store.onMiss = func() {
		go func() {
			markerDone <- f.CreateEntry(ctx, directoryMarker(dirPath), nil, false, false, nil, false, f.MaxFilenameLength)
		}()
		waitForDirectoryCreateWaiter(t, markerDone)
	}

	child := &Entry{FullPath: dirPath.Child("child"), Attr: Attr{Mode: 0o644}}
	require.NoError(t, f.CreateEntry(ctx, child, nil, false, false, nil, false, f.MaxFilenameLength))
	require.GreaterOrEqual(t, store.misses.Load(), int32(2), "the parent was inserted without a re-check")
	require.NoError(t, <-markerDone)

	assertDirectoryMarker(t, stub, dirPath)
}

// waitForDirectoryCreateWaiter returns once a goroutine waits in
// lockDirectoryCreate, or once the create has finished without waiting.
func waitForDirectoryCreateWaiter(t *testing.T, done chan error) {
	t.Helper()
	buf := make([]byte, 1<<20)
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
		select {
		case err := <-done:
			done <- err
			return
		default:
		}
		for _, g := range strings.Split(string(buf[:runtime.Stack(buf, true)]), "\n\n") {
			if strings.Contains(g, "sync.(*Cond).Wait") && strings.Contains(g, "lockDirectoryCreate") {
				return
			}
		}
	}
	t.Fatal("the directory create neither waited nor finished")
}
