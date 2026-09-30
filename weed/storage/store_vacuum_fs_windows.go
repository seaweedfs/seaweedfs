//go:build windows

package storage

import (
	"path/filepath"
	"strings"
)

// sameFilesystem reports whether two directories share one free-space pool.
// On Windows that is the volume the path is on; when in doubt it says yes,
// which makes the space check ask for the sum.
func sameFilesystem(a, b string) bool {
	va, vb := filepath.VolumeName(a), filepath.VolumeName(b)
	if va == "" || vb == "" {
		return true
	}
	return strings.EqualFold(va, vb)
}
