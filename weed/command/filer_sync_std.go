//go:build !windows
// +build !windows

package command

func escapeKey(key string) string {
	return key
}
