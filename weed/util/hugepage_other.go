//go:build !linux

package util

// LogTransparentHugePages is a no-op: the sysfs knobs it reports are Linux only.
func LogTransparentHugePages() {}
