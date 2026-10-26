//go:build !(linux || darwin || freebsd || netbsd || openbsd || solaris)

package util

// IsReadOnlyMount reports whether path is on a filesystem mounted read-only.
// Not detected on this platform.
func IsReadOnlyMount(path string) bool {
	return false
}
