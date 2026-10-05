package filer

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Rewriting identical inline content must not issue UpdateEntry: each one is a
// metadata event the local meta log persists to /topics/.system/log, and a
// client that rewrites the same config on a timer (e.g. the operator's 5-minute
// resync) keeps the volumes growing on an otherwise idle cluster.
func TestSaveInsideFilerSkipsIdenticalContent(t *testing.T) {
	ctx := context.Background()
	client := newFakeFilerConfClient()

	require.NoError(t, SaveInsideFiler(ctx, client, DirectoryEtcSeaweedFS, "a.json", []byte("v1")))
	require.Equal(t, 0, client.updateCalls)

	require.NoError(t, SaveInsideFiler(ctx, client, DirectoryEtcSeaweedFS, "a.json", []byte("v1")))
	assert.Equal(t, 0, client.updateCalls)

	require.NoError(t, SaveInsideFiler(ctx, client, DirectoryEtcSeaweedFS, "a.json", []byte("v2")))
	assert.Equal(t, 1, client.updateCalls)

	content, err := ReadInsideFiler(ctx, client, DirectoryEtcSeaweedFS, "a.json")
	require.NoError(t, err)
	assert.Equal(t, []byte("v2"), content)
}

// A legacy entry holding identical bytes but no Md5 still gets one write to
// stamp the hash that IF_ETAG_MATCH conditional writes key off; later
// identical writes skip.
func TestSaveInsideFilerStampsMd5OnLegacyEntry(t *testing.T) {
	ctx := context.Background()
	client := newFakeFilerConfClient()
	client.entries[client.key(DirectoryEtcSeaweedFS, "b.json")] = &filer_pb.Entry{
		Name:       "b.json",
		Content:    []byte("v1"),
		Attributes: &filer_pb.FuseAttributes{},
	}

	require.NoError(t, SaveInsideFiler(ctx, client, DirectoryEtcSeaweedFS, "b.json", []byte("v1")))
	assert.Equal(t, 1, client.updateCalls)

	require.NoError(t, SaveInsideFiler(ctx, client, DirectoryEtcSeaweedFS, "b.json", []byte("v1")))
	assert.Equal(t, 1, client.updateCalls)
}
