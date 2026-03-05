package framework

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/test/volume_server/matrix"
)

// DualVolumeCluster is deprecated. Use MultiVolumeCluster instead.
// For backward compatibility, it's a type alias for MultiVolumeCluster.
type DualVolumeCluster = MultiVolumeCluster

// StartDualVolumeCluster starts a cluster with 2 volume servers.
// Deprecated: Use StartMultiVolumeCluster(t, profile, 2) directly.
func StartDualVolumeCluster(t testing.TB, profile matrix.Profile) *DualVolumeCluster {
	return StartMultiVolumeCluster(t, profile, 2)
}
