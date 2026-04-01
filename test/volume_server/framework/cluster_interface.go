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

func useRustVolumeServer() bool {
	return os.Getenv("VOLUME_SERVER_IMPL") == "rust"
}

// StartVolumeCluster starts a single-volume cluster using either the Go or
// Rust volume server, depending on the VOLUME_SERVER_IMPL environment variable.
// Set VOLUME_SERVER_IMPL=rust to use the Rust volume server.
func StartVolumeCluster(t testing.TB, profile matrix.Profile) TestCluster {
	t.Helper()
	if useRustVolumeServer() {
		return StartRustVolumeCluster(t, profile)
	}
	return StartSingleVolumeCluster(t, profile)
}

// MultiCluster is the common interface for multi-volume cluster harnesses.
// Both *MultiVolumeCluster (Go) and *RustMultiVolumeCluster (Rust) satisfy it.
type MultiCluster interface {
	MasterAddress() string
	MasterURL() string
	BaseDir() string
	VolumeAdminAddress(index int) string
	VolumeAdminURL(index int) string
	VolumePublicAddress(index int) string
	VolumePublicURL(index int) string
	VolumeGRPCAddress(index int) string
	Stop()
}

// StartMultiVolumeClusterAuto starts a multi-volume cluster using either Go or
// Rust volume servers, depending on the VOLUME_SERVER_IMPL environment variable.
// Set VOLUME_SERVER_IMPL=rust to use Rust volume servers.
func StartMultiVolumeClusterAuto(t testing.TB, profile matrix.Profile, count int) MultiCluster {
	t.Helper()
	if useRustVolumeServer() {
		return StartRustMultiVolumeCluster(t, profile, count)
	}
	return StartMultiVolumeCluster(t, profile, count)
}
