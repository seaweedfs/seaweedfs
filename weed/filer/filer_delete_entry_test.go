package filer

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/stretchr/testify/require"
)

// The path in a non-empty-folder failure is the client's, so the marker has to
// lead the message for a caller reading it off the wire to trust it.
func TestNonEmptyFolderClassification(t *testing.T) {
	err := fmt.Errorf("%w: %s", ErrNonEmptyFolder, "/buckets/b/photos")
	if !IsNonEmptyFolderError(err) {
		t.Errorf("expected the sentinel to be recognized: %v", err)
	}
	if !errors.Is(DeleteEntryError(err.Error()), ErrNonEmptyFolder) {
		t.Errorf("expected the wire text to classify: %v", err)
	}

	// an entry named after the marker cannot forge one: every other delete
	// failure the filer builds leads with its own wrapper
	spoofed := "delete file /buckets/b/" + MsgFailDelNonEmptyFolder + ": filer store delete: disk full"
	if errors.Is(DeleteEntryError(spoofed), ErrNonEmptyFolder) {
		t.Errorf("expected no forgery from the entry name: %v", spoofed)
	}
	if IsNonEmptyFolderError(errors.New(spoofed)) {
		t.Errorf("expected no forgery from the entry name: %v", spoofed)
	}
}

func TestKeepRemoteObjectMarkerOnlyLivesOnDeleteEvents(t *testing.T) {
	store := newStubFilerStore()
	f := newTestFiler(t, store, NewFilerRemoteStorage())
	ctx := context.Background()
	planted := func() map[string][]byte {
		return map[string][]byte{ExtKeepRemoteObjectKey: []byte("true"), "Seaweed-Other": []byte("kept")}
	}
	dir := &Entry{FullPath: "/buckets/b/dir", Attr: Attr{Mode: os.ModeDir | 0755}}
	file := &Entry{FullPath: "/buckets/b/dir/obj.bin", Attr: Attr{Mode: 0644}, Extended: planted()}
	require.NoError(t, f.CreateEntry(ctx, dir, nil, false, false, nil, true, 255))
	require.NoError(t, f.CreateEntry(ctx, file, nil, false, false, nil, true, 255))
	updated := file.ShallowClone()
	updated.Extended = planted()
	require.NoError(t, f.UpdateEntry(ctx, file, updated, false))

	stored := store.entries[string(file.FullPath)]
	require.NotContains(t, stored.Extended, ExtKeepRemoteObjectKey, "a client must not be able to store the marker")
	require.Contains(t, stored.Extended, "Seaweed-Other")

	ordinaryCtx, ordinary := WithMetadataEventSink(ctx)
	require.NoError(t, f.DeleteEntryMetaAndData(ordinaryCtx, file.FullPath, false, false, false, false, nil, time.Time{}))
	require.False(t, IsMetadataOnlyDelete(ordinary.Last().EventNotification.OldEntry))

	require.NoError(t, f.CreateEntry(ctx, &Entry{FullPath: file.FullPath, Attr: Attr{Mode: 0644}}, nil, false, false, nil, true, 255))
	keepCtx, keep := WithMetadataEventSink(WithKeepRemoteObject(ctx))
	require.NoError(t, f.DeleteEntryMetaAndData(keepCtx, dir.FullPath, true, false, false, false, nil, time.Time{}))
	require.Equal(t, "dir", keep.Last().EventNotification.OldEntry.Name)
	require.True(t, IsMetadataOnlyDelete(keep.Last().EventNotification.OldEntry))
}

// A recursive delete that removes the last link to an inode must free the
// shared chunks: the record that owned them is gone, so nothing else will.
func TestDeleteFolderFreesLastHardLinkChunks(t *testing.T) {
	store := newStubFilerStore()
	f := newTestFiler(t, store, NewFilerRemoteStorage())
	ctx := context.Background()

	hardLinkId := NewHardLinkId()
	require.NoError(t, f.CreateEntry(ctx, &Entry{FullPath: "/d", Attr: Attr{Mode: os.ModeDir | 0755}}, nil, false, false, nil, true, 255))
	linkEntry := &Entry{
		FullPath:        "/d/a.txt",
		Attr:            Attr{Mode: 0644, Mtime: time.Now()},
		HardLinkId:      hardLinkId,
		HardLinkCounter: 1,
		Chunks:          []*filer_pb.FileChunk{{FileId: "3,01637037d6"}},
	}
	require.NoError(t, f.CreateEntry(ctx, linkEntry, nil, false, false, nil, true, 255))

	require.NoError(t, f.DeleteEntryMetaAndData(ctx, "/d", true, false, true, false, nil, time.Time{}))

	_, err := store.KvGet(ctx, hardLinkId)
	require.ErrorIs(t, err, ErrKvNotFound)
	require.Equal(t, 1, f.pendingDeletionCount())
}

// The same delete keeps the shared chunks while another link outside the
// folder still owns them.
func TestDeleteFolderKeepsSharedHardLinkChunks(t *testing.T) {
	store := newStubFilerStore()
	f := newTestFiler(t, store, NewFilerRemoteStorage())
	ctx := context.Background()

	hardLinkId := NewHardLinkId()
	for _, dir := range []string{"/d", "/keep"} {
		require.NoError(t, f.CreateEntry(ctx, &Entry{FullPath: util.FullPath(dir), Attr: Attr{Mode: os.ModeDir | 0755}}, nil, false, false, nil, true, 255))
	}
	for _, path := range []string{"/d/a.txt", "/keep/b.txt"} {
		require.NoError(t, f.CreateEntry(ctx, &Entry{
			FullPath:        util.FullPath(path),
			Attr:            Attr{Mode: 0644, Mtime: time.Now()},
			HardLinkId:      hardLinkId,
			HardLinkCounter: 2,
			Chunks:          []*filer_pb.FileChunk{{FileId: "3,01637037d6"}},
		}, nil, false, false, nil, true, 255))
	}

	require.NoError(t, f.DeleteEntryMetaAndData(ctx, "/d", true, false, true, false, nil, time.Time{}))

	_, err := store.KvGet(ctx, hardLinkId)
	require.NoError(t, err, "surviving link must keep the inode record")
	require.Equal(t, 0, f.pendingDeletionCount())
}

func TestDeleteEntryMetaAndData_IfNotModifiedAfterSubsecond(t *testing.T) {
	store := newStubFilerStore()
	f := newTestFiler(t, store, NewFilerRemoteStorage())
	ctx := context.Background()

	baseTime := time.Date(2026, 10, 26, 12, 0, 0, 500_000_000, time.UTC) // 12:00:00.500
	filePath := util.FullPath("/subsecond.txt")

	entry := &Entry{
		FullPath: filePath,
		Attr:     Attr{Mode: 0644, Mtime: baseTime},
	}
	require.NoError(t, f.CreateEntry(ctx, entry, nil, false, false, nil, true, 255))

	// Guard is 100ms before entry Mtime (within the same second)
	guardNs := baseTime.Add(-100 * time.Millisecond).UnixNano()
	require.NoError(t, f.DeleteEntryMetaAndData(ctx, filePath, false, false, false, false, nil, time.Unix(0, guardNs)))

	// Entry must NOT be deleted because entry's Mtime is newer than guardNs
	stored, err := f.FindEntry(ctx, filePath)
	require.NoError(t, err)
	require.NotNil(t, stored, "entry modified after guardNs must be preserved")

	// Guard is at or after entry Mtime
	olderGuardNs := baseTime.Add(100 * time.Millisecond).UnixNano()
	require.NoError(t, f.DeleteEntryMetaAndData(ctx, filePath, false, false, false, false, nil, time.Unix(0, olderGuardNs)))

	// Entry must be deleted now
	_, err = f.FindEntry(ctx, filePath)
	require.ErrorIs(t, err, filer_pb.ErrNotFound)

	// A guard equal to the entry's Mtime still deletes
	require.NoError(t, f.CreateEntry(ctx, entry, nil, false, false, nil, true, 255))
	require.NoError(t, f.DeleteEntryMetaAndData(ctx, filePath, false, false, false, false, nil, baseTime))
	_, err = f.FindEntry(ctx, filePath)
	require.ErrorIs(t, err, filer_pb.ErrNotFound)

	// An entry mtime beyond the UnixNano range compares as a time, not an int
	farFuture := &Entry{FullPath: filePath, Attr: Attr{Mode: 0644, Mtime: time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC)}}
	require.NoError(t, f.CreateEntry(ctx, farFuture, nil, false, false, nil, true, 255))
	require.NoError(t, f.DeleteEntryMetaAndData(ctx, filePath, false, false, false, false, nil, baseTime))
	stored, err = f.FindEntry(ctx, filePath)
	require.NoError(t, err)
	require.NotNil(t, stored, "entry past the UnixNano range must be preserved")

	// A whole-second cutoff beyond the UnixNano range still matches: the
	// unchanged year-3000 entry is at its guard's second, so it is deleted
	require.NoError(t, f.DeleteEntryMetaAndData(ctx, filePath, false, false, false, false, nil, time.Unix(farFuture.Attr.Mtime.Unix(), 999_999_999)))
	_, err = f.FindEntry(ctx, filePath)
	require.ErrorIs(t, err, filer_pb.ErrNotFound)
}
