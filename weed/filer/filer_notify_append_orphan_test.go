package filer

import (
	"context"
	"errors"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const appendTestLog = util.FullPath("/topics/.system/log/2026-11-05/10-00.00000001")

func newAppendTestFiler(store *stubFilerStore) *Filer {
	f := NewFiler(pb.ServerDiscovery{}, nil, "", "", "", "", "", 255, nil)
	f.Store = NewFilerStoreWrapper(store)
	return f
}

func queuedForDeletion(f *Filer, fid string) bool {
	f.deletionLedgerLock.Lock()
	defer f.deletionLedgerLock.Unlock()
	_, queued := f.pendingDeletions[fid]
	return queued
}

func TestCommitAppendedChunkReleasesChunkWhenCommitFails(t *testing.T) {
	store := newStubFilerStore()
	store.insertErr = errors.New("store unavailable")
	f := newAppendTestFiler(store)

	err := f.commitAppendedChunk(context.Background(), appendTestLog, &filer_pb.FileChunk{FileId: "7,0abc", Size: 5})

	require.Error(t, err)
	assert.True(t, queuedForDeletion(f, "7,0abc"), "a chunk no entry references must be released")
}

func TestCommitAppendedChunkReleasesChunkWhenLookupFails(t *testing.T) {
	store := newStubFilerStore()
	store.findErrByPath = map[string]error{string(appendTestLog): errors.New("store unavailable")}
	f := newAppendTestFiler(store)

	err := f.commitAppendedChunk(context.Background(), appendTestLog, &filer_pb.FileChunk{FileId: "7,0abc", Size: 5})

	require.Error(t, err)
	assert.True(t, queuedForDeletion(f, "7,0abc"), "a chunk no entry references must be released")
}

func TestCommitAppendedChunkKeepsChunkTheFailedCreateStored(t *testing.T) {
	store := newStubFilerStore()
	parent, _ := appendTestLog.DirAndName()
	store.findErrByPath = map[string]error{parent: errors.New("store unavailable")}
	f := newAppendTestFiler(store)

	err := f.commitAppendedChunk(context.Background(), appendTestLog, &filer_pb.FileChunk{FileId: "7,0abc", Size: 5})

	require.NoError(t, err, "a stored piece must not be appended again by the flush retry")
	stored, findErr := store.FindEntry(context.Background(), appendTestLog)
	require.NoError(t, findErr, "the entry is stored before its parent folders are created")
	require.Len(t, stored.Chunks, 1)
	assert.False(t, queuedForDeletion(f, "7,0abc"), "a chunk the stored entry references must stay")
}

func TestCommitAppendedChunkAppendsAfterExistingChunks(t *testing.T) {
	store := newStubFilerStore()
	f := newAppendTestFiler(store)
	ctx := context.Background()

	require.NoError(t, f.commitAppendedChunk(ctx, appendTestLog, &filer_pb.FileChunk{FileId: "7,0abc", Size: 5}))
	require.NoError(t, f.commitAppendedChunk(ctx, appendTestLog, &filer_pb.FileChunk{FileId: "7,0abd", Size: 3}))

	stored, err := store.FindEntry(ctx, appendTestLog)
	require.NoError(t, err)
	require.Len(t, stored.Chunks, 2)
	assert.Equal(t, int64(5), stored.Chunks[1].Offset)
	assert.False(t, queuedForDeletion(f, "7,0abc"))
	assert.False(t, queuedForDeletion(f, "7,0abd"))
}
