//go:build !unix

package fuse

// FUSE mounts are unix-only: reporting one device keeps waitForMount from
// mistaking the plain pre-mount directory for a live mount on platforms
// where a mount can never appear.
func deviceID(path string) (uint64, error) {
	return 0, nil
}
