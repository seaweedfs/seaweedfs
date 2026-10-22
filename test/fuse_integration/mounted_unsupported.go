//go:build !unix && !windows

package fuse

// FUSE mounts are unix-only: reporting one device keeps waitForMount from
// mistaking the plain pre-mount directory for a live mount on platforms
// where a mount can never appear.
func deviceID(path string) (uint64, error) {
	return 0, nil
}

// mountedAt cannot be detected off unix; report mounted so an external
// TEST_MOUNT_POINT keeps its best-effort write verification.
func mountedAt(path string) bool {
	return true
}
