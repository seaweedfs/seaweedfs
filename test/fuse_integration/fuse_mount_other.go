//go:build (unix && !linux && !darwin && !freebsd) || (!unix && !windows)

package fuse

// fuseFilesystem has no portable check on these platforms; the mount check
// and write verification in Setup stay the only gates.
func fuseFilesystem(path string) bool {
	return true
}
