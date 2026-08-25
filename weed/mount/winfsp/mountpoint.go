package winfsp

import (
	"fmt"
	"strings"
)

// VolumePrefix converts a \\server\share mount point into WinFsp's
// VolumePrefix option, which registers the mount as a network file system.
// That registration is what makes the mount reachable from every logon
// session by its UNC path — a drive letter lives inside the session that
// created it, which is why a service mount is invisible to the users logged
// on at the desktop. Handed through as a plain mount point instead, the UNC
// path would be taken for a directory on an actual remote server and the
// mount would fail.
//
// Mount points that are not UNC paths return "", including the \\?\ and \\.\
// device forms, which WinFsp interprets itself.
func VolumePrefix(mountPoint string) (string, error) {
	normalized := strings.ReplaceAll(mountPoint, "/", `\`)
	if !strings.HasPrefix(normalized, `\\`) {
		return "", nil
	}
	if strings.HasPrefix(normalized, `\\?\`) || strings.HasPrefix(normalized, `\\.\`) {
		return "", nil
	}
	parts := strings.Split(strings.TrimRight(normalized[2:], `\`), `\`)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", fmt.Errorf(`a network mount point must be \\server\share, got %s`, mountPoint)
	}
	return `\` + parts[0] + `\` + parts[1], nil
}
