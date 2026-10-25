//go:build windows

package storage

import (
	"path/filepath"
	"strings"

	"golang.org/x/sys/windows"
)

// sameFilesystem reports whether two directories share one free-space pool.
// On Windows a volume can be reached through a drive letter and through a
// folder it is mounted on, so the path prefix says nothing; the volume GUID
// behind each path does. When in doubt it says yes, which makes the space
// check ask for the sum.
func sameFilesystem(a, b string) bool {
	if a == b {
		return true
	}
	va, errA := volumeGUID(a)
	vb, errB := volumeGUID(b)
	if errA != nil || errB != nil {
		return true
	}
	return strings.EqualFold(va, vb)
}

func volumeGUID(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	p, err := windows.UTF16PtrFromString(abs)
	if err != nil {
		return "", err
	}
	mountPoint := make([]uint16, windows.MAX_LONG_PATH)
	if err := windows.GetVolumePathName(p, &mountPoint[0], uint32(len(mountPoint))); err != nil {
		return "", err
	}
	// \\?\Volume{GUID}\ is 49 characters plus the terminator.
	guid := make([]uint16, 50)
	if err := windows.GetVolumeNameForVolumeMountPoint(&mountPoint[0], &guid[0], uint32(len(guid))); err != nil {
		return "", err
	}
	return windows.UTF16ToString(guid), nil
}
