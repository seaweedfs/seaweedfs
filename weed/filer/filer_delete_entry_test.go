package filer

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"

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
