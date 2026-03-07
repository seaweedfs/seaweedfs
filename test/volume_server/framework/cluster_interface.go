package framework

import (
	"os"
	"testing"

	"github.com/seaweedfs/seaweedfs/test/volume_server/matrix"
)

// TestCluster is the common interface for single-volume cluster harnesses.
// Both *Cluster (Go volume) and *RustCluster (Rust volume) satisfy it.
type TestCluster interface {
	MasterAddress() string
	VolumeAdminAddress() string
	VolumePublicAddress() string
	VolumeGRPCAddress() string
	VolumeServerAddress() string
	MasterURL() string
	VolumeAdminURL() string
	VolumePublicURL() string
	BaseDir() string
	Stop()
}

// StartVolumeCluster starts a single-volume cluster using either the Go or
// Rust volume server, depending on the VOLUME_SERVER_IMPL environment variable.
// Set VOLUME_SERVER_IMPL=rust to use the Rust volume server.
func StartVolumeCluster(t testing.TB, profile matrix.Profile) TestCluster {
	t.Helper()
	if os.Getenv("VOLUME_SERVER_IMPL") == "rust" {
		return StartRustVolumeCluster(t, profile)
	}
	return StartSingleVolumeCluster(t, profile)
}
