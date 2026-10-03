package filer

import (
	"context"
	"testing"

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
