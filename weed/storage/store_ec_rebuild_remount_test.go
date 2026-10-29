package storage

import (
	"bytes"
	"os"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/stretchr/testify/require"
)

// A shard file the rebuild replaced under its mount keeps serving the old
// inode through the open descriptor. Unmount+mount must reopen the new file.
func TestRemountRegeneratedShardReadsNewFile(t *testing.T) {
	store := newEcTargetTestStore(t, 1)
	collection := "col"
	vid := needle.VolumeId(42)
	dataDir := store.Locations[0].Directory

	writeEcShard0(t, dataDir, collection, vid)
	writeEcIndex(t, dataDir, collection, vid)

	shardPath := erasure_coding.EcShardFileName(collection, dataDir, int(vid)) + erasure_coding.ToExt(0)
	content := make([]byte, calculateExpectedShardSize(1024*1024, 10))
	for i := range content {
		content[i] = 'A'
	}
	require.NoError(t, os.WriteFile(shardPath, content, 0o644))

	require.NoError(t, store.MountEcShards(collection, vid, 0, ""))

	readShard := func() []byte {
		shard, found := store.Locations[0].FindEcShard(vid, 0)
		require.True(t, found)
		buf := make([]byte, len(content))
		n, err := shard.ReadAt(buf, 0)
		require.NoError(t, err)
		require.Equal(t, len(content), n)
		return buf
	}
	require.True(t, bytes.Equal(content, readShard()))

	for i := range content {
		content[i] = 'B'
	}
	rebuilt := shardPath + ".rebuilding"
	require.NoError(t, os.WriteFile(rebuilt, content, 0o644))
	require.NoError(t, os.Rename(rebuilt, shardPath))

	stale := readShard()
	require.False(t, bytes.Equal(content, stale), "mounted shard should still read the replaced inode")

	require.NoError(t, store.UnmountEcShards(vid, 0, 0))
	require.NoError(t, store.MountEcShards(collection, vid, 0, ""))

	require.True(t, bytes.Equal(content, readShard()), "remounted shard should read the regenerated file")
}
