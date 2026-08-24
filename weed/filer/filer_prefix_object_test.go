package filer

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPromoteToPrefixObject covers a key written before the keys nested under it:
// the file becomes the directory they live in, and has to stay an object of its own.
func TestPromoteToPrefixObject(t *testing.T) {
	f, store := newTestFilerWithStubStore()
	ctx := context.Background()

	object := &Entry{
		FullPath: util.FullPath("/buckets/bkt/a/foo"),
		Attr: Attr{
			Mode:   0o644,
			Mime:   "text/plain",
			TtlSec: 3600,
		},
		Chunks:   []*filer_pb.FileChunk{{FileId: "1,01", Size: 4}},
		Extended: map[string][]byte{s3_constants.SeaweedFSExpiresS3: []byte("true")},
	}
	require.NoError(t, f.CreateEntry(ctx, object, nil, false, false, nil, false, f.MaxFilenameLength))

	nested := &Entry{FullPath: util.FullPath("/buckets/bkt/a/foo/bar"), Attr: Attr{Mode: 0o644}}
	require.NoError(t, f.CreateEntry(ctx, nested, nil, false, false, nil, false, f.MaxFilenameLength))

	promoted, err := store.FindEntry(ctx, object.FullPath)
	require.NoError(t, err)
	require.NotNil(t, promoted)

	assert.True(t, promoted.IsDirectory(), "the nested key needs a directory here")
	assert.Equal(t, object.Chunks, promoted.Chunks, "the object's data stays on it")
	assert.Contains(t, promoted.Extended, s3_constants.SeaweedFSPrefixObject)
	// Expiring the entry deletes the directory row on its own and strands the keys
	// under it, so the promotion gives up the lazy TTL.
	assert.Zero(t, promoted.Attr.TtlSec)
}

// TestExpiredDirectoryIsNotDeletedOnRead pins the other half: a TTL an older build
// left on a promoted directory must not take the keys under it with it.
func TestExpiredDirectoryIsNotDeletedOnRead(t *testing.T) {
	f, store := newTestFilerWithStubStore()
	ctx := context.Background()

	dirPath := util.FullPath("/buckets/bkt/a/foo")
	expired := time.Now().Add(-2 * time.Hour)
	require.NoError(t, store.InsertEntry(ctx, &Entry{
		FullPath: dirPath,
		Attr: Attr{
			Mode:   os.ModeDir | 0o755,
			Crtime: expired,
			Mtime:  expired,
			TtlSec: 60,
		},
		Extended: map[string][]byte{s3_constants.SeaweedFSExpiresS3: []byte("true")},
	}))
	nested := &Entry{FullPath: dirPath + "/bar", Attr: Attr{Mode: 0o644}}
	require.NoError(t, store.InsertEntry(ctx, nested))

	found, err := f.FindEntry(ctx, dirPath)
	require.NoError(t, err)
	require.NotNil(t, found, "deleting it here would leave the nested key unreachable")

	stillThere, err := store.FindEntry(ctx, nested.FullPath)
	require.NoError(t, err)
	require.NotNil(t, stillThere)
}
