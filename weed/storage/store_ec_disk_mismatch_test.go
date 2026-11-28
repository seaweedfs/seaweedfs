package storage

import (
	"os"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/stretchr/testify/require"
)

func TestCheckMountedEcShardsFindsGhostAndStaleSize(t *testing.T) {
	store := newEcTargetTestStore(t, 1)
	collection := "col"
	vid := needle.VolumeId(42)
	dataDir := store.Locations[0].Directory

	writeEcShard0(t, dataDir, collection, vid)
	writeEcIndex(t, dataDir, collection, vid)
	shardPath := erasure_coding.EcShardFileName(collection, dataDir, int(vid)) + erasure_coding.ToExt(0)
	content := make([]byte, calculateExpectedShardSize(1024*1024, 10))
	require.NoError(t, os.WriteFile(shardPath, content, 0o644))
	require.NoError(t, store.MountEcShards(collection, vid, 0, ""))

	check := func() map[string]int { return store.CheckMountedEcShards() }
	require.Equal(t, map[string]int{erasure_coding.EcShardGhost: 0, erasure_coding.EcShardStaleSize: 0}, check())

	f, err := os.OpenFile(shardPath, os.O_APPEND|os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = f.Write([]byte("grown"))
	require.NoError(t, err)
	require.NoError(t, f.Close())
	require.Equal(t, 1, check()[erasure_coding.EcShardStaleSize])

	replacement := shardPath + ".new"
	require.NoError(t, os.WriteFile(replacement, content, 0o644))
	require.NoError(t, os.Rename(replacement, shardPath))
	require.Equal(t, map[string]int{erasure_coding.EcShardGhost: 1, erasure_coding.EcShardStaleSize: 0}, check())

	reopened, err := store.Locations[0].ReopenEcShard(vid, 0)
	require.NoError(t, err)
	require.True(t, reopened)
	require.Equal(t, 0, check()[erasure_coding.EcShardGhost])

	require.NoError(t, os.Remove(shardPath))
	require.Equal(t, map[string]int{erasure_coding.EcShardGhost: 1, erasure_coding.EcShardStaleSize: 0}, check())
}
