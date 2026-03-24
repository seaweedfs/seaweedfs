package mount

import "github.com/seaweedfs/go-fuse/v2/fuse"

// https://github.com/libfuse/libfuse/blob/48ae2e72b39b6a31cb2194f6f11786b7ca06aac6/include/fuse.h#L778

/**
 * Allocates space for an open file
 *
 * This function ensures that required space is allocated for specified
 * file.  If this function returns success then any subsequent write
 * request to specified range is guaranteed not to fail because of lack
 * of space on the file system media.
 */
func (wfs *WFS) Fallocate(cancel <-chan struct{}, in *fuse.FallocateIn) (code fuse.Status) {
	return fuse.ENOSYS
}
