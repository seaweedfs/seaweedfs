package filer

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
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
	require.NoError(t, f.DeleteEntryMetaAndData(ordinaryCtx, file.FullPath, false, false, false, false, nil, 0))
	require.False(t, IsMetadataOnlyDelete(ordinary.Last().EventNotification.OldEntry))

	require.NoError(t, f.CreateEntry(ctx, &Entry{FullPath: file.FullPath, Attr: Attr{Mode: 0644}}, nil, false, false, nil, true, 255))
	keepCtx, keep := WithMetadataEventSink(WithKeepRemoteObject(ctx))
	require.NoError(t, f.DeleteEntryMetaAndData(keepCtx, dir.FullPath, true, false, false, false, nil, 0))
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

	require.NoError(t, f.DeleteEntryMetaAndData(ctx, "/d", true, false, true, false, nil, 0))

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

	require.NoError(t, f.DeleteEntryMetaAndData(ctx, "/d", true, false, true, false, nil, 0))

	_, err := store.KvGet(ctx, hardLinkId)
	require.NoError(t, err, "surviving link must keep the inode record")
	require.Equal(t, 0, f.pendingDeletionCount())
}

func retentionEntry(mode string, until time.Time) map[string][]byte {
	return map[string][]byte{
		s3_constants.ExtObjectLockModeKey:     []byte(mode),
		s3_constants.ExtRetentionUntilDateKey: []byte(strconv.FormatInt(until.Unix(), 10)),
	}
}

func TestEntryLockedForDelete(t *testing.T) {
	now := time.Now()
	future := now.Add(time.Hour)
	past := now.Add(-time.Hour)

	for _, tt := range []struct {
		name   string
		ext    map[string][]byte
		bypass bool
		locked bool
	}{
		{"no attributes", nil, false, false},
		{"legal hold on", map[string][]byte{s3_constants.ExtLegalHoldKey: []byte("ON")}, false, true},
		{"legal hold bypass still locks", map[string][]byte{s3_constants.ExtLegalHoldKey: []byte("on")}, true, true},
		{"legal hold off", map[string][]byte{s3_constants.ExtLegalHoldKey: []byte("OFF")}, false, false},
		{"compliance active", retentionEntry(s3_constants.RetentionModeCompliance, future), false, true},
		{"compliance bypass still locks", retentionEntry(s3_constants.RetentionModeCompliance, future), true, true},
		{"compliance expired", retentionEntry(s3_constants.RetentionModeCompliance, past), false, false},
		{"governance active", retentionEntry(s3_constants.RetentionModeGovernance, future), false, true},
		{"governance bypass", retentionEntry(s3_constants.RetentionModeGovernance, future), true, false},
		{"governance expired", retentionEntry(s3_constants.RetentionModeGovernance, past), false, false},
		{"mode without date", map[string][]byte{s3_constants.ExtObjectLockModeKey: []byte(s3_constants.RetentionModeCompliance)}, false, false},
		{"unparseable date fails closed", map[string][]byte{
			s3_constants.ExtObjectLockModeKey:     []byte(s3_constants.RetentionModeCompliance),
			s3_constants.ExtRetentionUntilDateKey: []byte("not-a-time"),
		}, false, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			entry := &Entry{FullPath: "/buckets/b/o", Attr: Attr{Mode: 0644}, Extended: tt.ext}
			require.Equal(t, tt.locked, entryLockedForDelete(entry, tt.bypass, now))
		})
	}
}

// The same leading-marker contract as ErrNonEmptyFolder keeps the wire text
// classifiable and unforgeable.
func TestEntryLockedClassification(t *testing.T) {
	err := fmt.Errorf("%w: %s", ErrEntryLocked, "/buckets/b/locked.bin")
	if !IsEntryLockedError(err) {
		t.Errorf("expected the sentinel to be recognized: %v", err)
	}
	if !errors.Is(DeleteEntryError(err.Error()), ErrEntryLocked) {
		t.Errorf("expected the wire text to classify: %v", err)
	}
	spoofed := "delete file /buckets/b/" + MsgFailDelLockedEntry + ": filer store delete: disk full"
	if errors.Is(DeleteEntryError(spoofed), ErrEntryLocked) {
		t.Errorf("expected no forgery from the entry name: %v", spoofed)
	}
	if IsEntryLockedError(errors.New(spoofed)) {
		t.Errorf("expected no forgery from the entry name: %v", spoofed)
	}
}

func TestDeleteEntryRefusesLockedObject(t *testing.T) {
	store := newStubFilerStore()
	f := newTestFiler(t, store, NewFilerRemoteStorage())
	ctx := context.Background()

	locked := &Entry{
		FullPath: "/buckets/b/locked.bin",
		Attr:     Attr{Mode: 0644},
		Extended: retentionEntry(s3_constants.RetentionModeCompliance, time.Now().Add(time.Hour)),
	}
	require.NoError(t, f.CreateEntry(ctx, locked, nil, false, false, nil, true, 255))

	err := f.DeleteEntryMetaAndData(ctx, locked.FullPath, false, false, true, false, nil, 0)
	require.ErrorIs(t, err, ErrEntryLocked)
	require.True(t, IsEntryLockedError(err))
	require.True(t, IsEntryLockedError(DeleteEntryError(err.Error())))
	require.NotNil(t, store.getEntry("/buckets/b/locked.bin"), "locked entry must survive the refused delete")
}

// A recursive delete fails at the locked descendant without removing anything.
func TestDeleteEntryRecursiveRefusesLockedDescendant(t *testing.T) {
	store := newStubFilerStore()
	f := newTestFiler(t, store, NewFilerRemoteStorage())
	ctx := context.Background()

	require.NoError(t, f.CreateEntry(ctx, &Entry{FullPath: "/d", Attr: Attr{Mode: os.ModeDir | 0755}}, nil, false, false, nil, true, 255))
	require.NoError(t, f.CreateEntry(ctx, &Entry{FullPath: "/d/free.txt", Attr: Attr{Mode: 0644}}, nil, false, false, nil, true, 255))
	require.NoError(t, f.CreateEntry(ctx, &Entry{
		FullPath: "/d/locked.txt",
		Attr:     Attr{Mode: 0644},
		Extended: map[string][]byte{s3_constants.ExtLegalHoldKey: []byte("ON")},
	}, nil, false, false, nil, true, 255))

	err := f.DeleteEntryMetaAndData(ctx, "/d", true, true, true, false, nil, 0)
	require.ErrorIs(t, err, ErrEntryLocked)
	require.NotNil(t, store.getEntry("/d"), "the folder itself must survive")
	require.NotNil(t, store.getEntry("/d/free.txt"), "unlocked siblings must survive a refused delete")
	require.NotNil(t, store.getEntry("/d/locked.txt"))
}

// Governance bypass is the only waiver the delete honors.
func TestDeleteEntryGovernanceBypass(t *testing.T) {
	future := time.Now().Add(time.Hour)

	t.Run("governance locked deletes with bypass", func(t *testing.T) {
		store := newStubFilerStore()
		f := newTestFiler(t, store, NewFilerRemoteStorage())
		entry := &Entry{FullPath: "/o", Attr: Attr{Mode: 0644}, Extended: retentionEntry(s3_constants.RetentionModeGovernance, future)}
		require.NoError(t, f.CreateEntry(context.Background(), entry, nil, false, false, nil, true, 255))
		require.NoError(t, f.DeleteEntryMetaAndData(WithBypassGovernanceRetention(context.Background()), entry.FullPath, false, false, true, false, nil, 0))
		require.Nil(t, store.getEntry("/o"))
	})

	t.Run("compliance still refuses with bypass", func(t *testing.T) {
		store := newStubFilerStore()
		f := newTestFiler(t, store, NewFilerRemoteStorage())
		entry := &Entry{FullPath: "/o", Attr: Attr{Mode: 0644}, Extended: retentionEntry(s3_constants.RetentionModeCompliance, future)}
		require.NoError(t, f.CreateEntry(context.Background(), entry, nil, false, false, nil, true, 255))
		require.ErrorIs(t, f.DeleteEntryMetaAndData(WithBypassGovernanceRetention(context.Background()), entry.FullPath, false, false, true, false, nil, 0), ErrEntryLocked)
		require.NotNil(t, store.getEntry("/o"))
	})
}

// A delete replayed from a peer cluster applies as sent: the origin already
// enforced retention before shipping the event.
func TestDeleteEntryClusterReplaySkipsLockCheck(t *testing.T) {
	store := newStubFilerStore()
	f := newTestFiler(t, store, NewFilerRemoteStorage())
	ctx := context.Background()

	entry := &Entry{
		FullPath: "/locked",
		Attr:     Attr{Mode: 0644},
		Extended: map[string][]byte{s3_constants.ExtLegalHoldKey: []byte("ON")},
	}
	require.NoError(t, f.CreateEntry(ctx, entry, nil, false, false, nil, true, 255))
	require.NoError(t, f.DeleteEntryMetaAndData(ctx, entry.FullPath, false, false, true, true, nil, 0))
	require.Nil(t, store.getEntry("/locked"))
}

type bucketDropStore struct {
	*stubFilerStore
}

func (s *bucketDropStore) OnBucketCreation(string)  {}
func (s *bucketDropStore) OnBucketDeletion(string)  {}
func (s *bucketDropStore) CanDropWholeBucket() bool { return true }

// A lock-enabled bucket is walked rather than dropped whole, so its locked
// objects still refuse the delete on a store that could drop it blindly.
func TestDeleteBucketOnDropCapableStoreRefusesLockedObject(t *testing.T) {
	store := newStubFilerStore()
	f := newTestFiler(t, store, NewFilerRemoteStorage())
	f.DirBucketsPath = "/buckets"
	f.Store = NewFilerStoreWrapper(&bucketDropStore{store})
	ctx := context.Background()

	bucket := &Entry{
		FullPath: "/buckets/b",
		Attr:     Attr{Mode: os.ModeDir | 0755},
		Extended: map[string][]byte{s3_constants.ExtObjectLockEnabledKey: []byte(s3_constants.ObjectLockEnabled)},
	}
	require.NoError(t, f.CreateEntry(ctx, bucket, nil, false, false, nil, true, 255))
	require.NoError(t, f.CreateEntry(ctx, &Entry{
		FullPath: "/buckets/b/locked.bin",
		Attr:     Attr{Mode: 0644},
		Extended: retentionEntry(s3_constants.RetentionModeCompliance, time.Now().Add(time.Hour)),
	}, nil, false, false, nil, true, 255))

	err := f.DeleteEntryMetaAndData(ctx, "/buckets/b", true, false, true, false, nil, 0)
	require.ErrorIs(t, err, ErrEntryLocked)
	require.NotNil(t, store.getEntry("/buckets/b/locked.bin"), "a drop-whole-bucket store must not swallow the locked object")
}

// An expired retention no longer locks: the delete proceeds normally.
func TestDeleteEntryAllowsExpiredRetention(t *testing.T) {
	store := newStubFilerStore()
	f := newTestFiler(t, store, NewFilerRemoteStorage())
	ctx := context.Background()

	entry := &Entry{
		FullPath: "/old",
		Attr:     Attr{Mode: 0644},
		Extended: retentionEntry(s3_constants.RetentionModeCompliance, time.Now().Add(-time.Hour)),
	}
	require.NoError(t, f.CreateEntry(ctx, entry, nil, false, false, nil, true, 255))
	require.NoError(t, f.DeleteEntryMetaAndData(ctx, entry.FullPath, false, false, true, false, nil, 0))
	require.Nil(t, store.getEntry("/old"))
}
