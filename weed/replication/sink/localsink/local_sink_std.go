//go:build !windows

package localsink

// sanitizeFsKey is a no-op on non-Windows filesystems. The Windows build
// strips characters that are illegal in NTFS paths.
func sanitizeFsKey(key string) string {
	return key
}
