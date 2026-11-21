package filer

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpdateFilerConfKeepsAFlagClearedDuringTheUpdate(t *testing.T) {
	client := newFakeFilerConfClient()
	putFilerConf(t, client, &filer_pb.FilerConf_PathConf{LocationPrefix: "/buckets/b/", ReadOnly: true})

	changed, err := UpdateFilerConf(context.Background(), client, func(fc *FilerConf) (bool, error) {
		cleared, err := ClearBucketReadOnly(context.Background(), client, "/buckets", "b")
		require.NoError(t, err)
		require.True(t, cleared)
		_, flipped := fc.ApplyBucketQuotaReadOnly("/buckets/a/", 200, 100)
		return flipped, nil
	})
	assert.Error(t, err)
	assert.False(t, changed)

	fc := NewFilerConf()
	require.NoError(t, fc.LoadFromBytes([]byte(readFilerConfText(client))))
	assert.False(t, fc.MatchStorageRule("/buckets/b/").ReadOnly, "the cleared flag must not come back")
	assert.False(t, fc.MatchStorageRule("/buckets/a/").ReadOnly)
}

func TestUpdateFilerConfSavesOnlyAChange(t *testing.T) {
	client := newFakeFilerConfClient()
	putFilerConf(t, client, &filer_pb.FilerConf_PathConf{LocationPrefix: "/buckets/b/", Collection: "b"})
	before := readFilerConfText(client)

	changed, err := UpdateFilerConf(context.Background(), client, func(fc *FilerConf) (bool, error) { return false, nil })
	require.NoError(t, err)
	assert.False(t, changed)
	assert.Equal(t, before, readFilerConfText(client))

	changed, err = UpdateFilerConf(context.Background(), client, func(fc *FilerConf) (bool, error) {
		_, flipped := fc.ApplyBucketQuotaReadOnly("/buckets/a/", 200, 100)
		return flipped, nil
	})
	require.NoError(t, err)
	assert.True(t, changed)
	fc := NewFilerConf()
	require.NoError(t, fc.LoadFromBytes([]byte(readFilerConfText(client))))
	assert.True(t, fc.MatchStorageRule("/buckets/a/").ReadOnly)
}
