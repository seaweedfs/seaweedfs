//go:build !linux

package mount

import "github.com/seaweedfs/go-fuse/v2/fuse"

// FUSE_PASSTHROUGH is a Linux-only kernel feature (>= 6.9). On other platforms
// these are no-ops so the mount package builds and behaves as before.

func (wfs *WFS) tryEnablePassthrough(fh *FileHandle, out *fuse.OpenOut) bool {
	return false
}

func (fh *FileHandle) teardownPassthrough() {}
