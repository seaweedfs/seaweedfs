package app

import (
	"bytes"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/admin/dash"
	"github.com/stretchr/testify/require"
)

func TestClusterVectorServers(t *testing.T) {
	var out bytes.Buffer
	require.NoError(t, ClusterVectorServers(dash.VectorOperationsData{Engines: []dash.VectorNode{{Address: "10.0.0.6:18888", Version: "0.1.0", Status: "Ready"}}}).Render(t.Context(), &out))
	require.Contains(t, out.String(), "10.0.0.6:18888")
	require.Contains(t, out.String(), "Ready")

	out.Reset()
	require.NoError(t, ClusterVectorServers(dash.VectorOperationsData{}).Render(t.Context(), &out))
	require.Contains(t, out.String(), "No weed-vector engine is registered")

	out.Reset()
	require.NoError(t, ClusterVectorServers(dash.VectorOperationsData{DiscoveryError: "no master"}).Render(t.Context(), &out))
	require.Contains(t, out.String(), "Engine discovery failed: no master")
}
