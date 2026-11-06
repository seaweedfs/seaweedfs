package filer

import (
	"context"
	"errors"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const orphanTestFid = "7,0abc"

func orphanTestChunk() (*operation.AssignResult, *operation.UploadResult) {
	return &operation.AssignResult{Fid: orphanTestFid}, &operation.UploadResult{Name: "chunk", Size: 5}
}

func queuedForDeletion(f *Filer, fid string) bool {
	f.deletionLedgerLock.Lock()
	defer f.deletionLedgerLock.Unlock()
	_, queued := f.pendingDeletions[fid]
	return queued
}

// TestCommitAppendedChunkReleasesChunkWhenCommitFails covers the orphan-chunk
// leak behind unbounded metadata-log growth.
//
// appendToFile uploads the chunk first and commits the filer entry afterwards.
// When the commit fails the chunk is already on a volume server but nothing
// references it: no entry points at that fid, so no later delete can ever mark
// it, and neither vacuum nor fsck can see it because both work from the entry
// index. The volume server keeps counting those bytes as live data, which is
// how a cluster with a few hundred megabytes of real logs ends up carrying
// tens of gigabytes of "live" volume data.
func TestCommitAppendedChunkReleasesChunkWhenCommitFails(t *testing.T) {
	store := newStubFilerStore()
	store.insertErr = errors.New("store unavailable")
	f := NewFiler(pb.ServerDiscovery{}, nil, "", "", "", "", "", 255, nil)
	f.Store = NewFilerStoreWrapper(store)

	assignResult, uploadResult := orphanTestChunk()
	path := util.FullPath("/topics/.system/log/2026-11-05")

	err := f.commitAppendedChunk(context.Background(), path, &Entry{FullPath: path}, assignResult, uploadResult, 0)

	require.Error(t, err, "the commit failure must be surfaced to the caller")
	assert.True(t, queuedForDeletion(f, orphanTestFid),
		"the uploaded chunk must be released after a failed entry commit, "+
			"otherwise it stays orphaned on the volume server and only volume.fsck can reclaim it")
}

// TestCommitAppendedChunkKeepsChunkOnSuccess guards against over-correcting: a
// successful commit must never delete the chunk it just attached.
func TestCommitAppendedChunkKeepsChunkOnSuccess(t *testing.T) {
	store := newStubFilerStore()
	f := NewFiler(pb.ServerDiscovery{}, nil, "", "", "", "", "", 255, nil)
	f.Store = NewFilerStoreWrapper(store)

	assignResult, uploadResult := orphanTestChunk()
	path := util.FullPath("/topics/.system/log/2026-11-05")

	err := f.commitAppendedChunk(context.Background(), path, &Entry{FullPath: path}, assignResult, uploadResult, 0)

	require.NoError(t, err)
	assert.False(t, queuedForDeletion(f, orphanTestFid),
		"a successfully committed chunk must never be queued for deletion")
}

func entryWithChunks(n int) *filer_pb.Entry {
	e := &filer_pb.Entry{Name: "journal.jsonl"}
	for i := 0; i < n; i++ {
		e.Chunks = append(e.Chunks, &filer_pb.FileChunk{FileId: "7,0abc", Offset: int64(i), Size: 1})
	}
	return e
}

// TestCapEventChunksDropsOversizedChunkLists covers the quadratic metadata-log
// growth: a metadata event embeds the whole old and new entry, so an
// append-only file makes every append re-serialize every chunk it ever had.
func TestCapEventChunksDropsOversizedChunkLists(t *testing.T) {
	atLimit := entryWithChunks(metadataEventChunkLimit)
	capped, truncated := capEventChunks(atLimit)
	assert.False(t, truncated, "an entry at the limit must be carried intact")
	assert.Len(t, capped.Chunks, metadataEventChunkLimit)

	overLimit := entryWithChunks(metadataEventChunkLimit + 1)
	capped, truncated = capEventChunks(overLimit)
	assert.True(t, truncated, "an oversized entry must be reported as truncated")
	assert.Empty(t, capped.Chunks, "the chunk list must be dropped from the event")
	assert.Len(t, overLimit.Chunks, metadataEventChunkLimit+1,
		"the caller's entry must not be mutated")

	_, truncated = capEventChunks(nil)
	assert.False(t, truncated, "a nil entry must not be reported as truncated")
}
